import os
import re
import hashlib
import secrets
import asyncio
from io import BytesIO
from datetime import datetime, date, timedelta

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

import openpyxl

from aiohttp import web

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
)
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application


# ================== ENV ==================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
PORT = int(os.getenv("PORT", "10000"))

ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "")
SECRET_SALT = os.getenv("SECRET_SALT", "CHANGE_ME_SALT")

WEBHOOK_BASE_URL = (os.getenv("WEBHOOK_BASE_URL") or "").strip()  # https://xxx.onrender.com
WEBHOOK_PATH = (os.getenv("WEBHOOK_PATH") or "/webhook").strip()
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()      # optional
USE_WEBHOOK = bool(WEBHOOK_BASE_URL)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


# ================== UI ==================
LOGIN_KB = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Отмена"), KeyboardButton(text="Заново")]],
    resize_keyboard=True
)

MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Заполнить сверку")]],
    resize_keyboard=True
)

CANCEL_KB = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Отмена")]],
    resize_keyboard=True
)


# ================== Helpers ==================
def parse_admin_ids(raw: str) -> set[int]:
    out = set()
    for p in (raw or "").split(","):
        p = p.strip()
        if p.isdigit():
            out.add(int(p))
    return out


ADMIN_IDS = parse_admin_ids(ADMIN_IDS_RAW)


def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


def fio_display(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def fio_norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("ё", "е")
    s = re.sub(r"[\u00A0\u2000-\u200B\u202F\u205F\u3000]", " ", s)
    s = re.sub(r"[^а-яa-z\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def extract_last4_from_phone(phone: str) -> str:
    digits = re.sub(r"\D+", "", phone or "")
    return digits[-4:] if len(digits) >= 4 else ""


def hash_last4(last4: str) -> str:
    s = (last4.strip() + SECRET_SALT).encode("utf-8")
    return hashlib.sha256(s).hexdigest()


def normalize_point_code(v) -> str:
    s = str(v or "").strip()
    s = re.sub(r"\s+", "", s)
    return s


def month_start(y: int, m: int) -> date:
    return date(y, m, 1)


def month_end_exclusive(y: int, m: int) -> date:
    return date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)


def days_in_month(y: int, m: int) -> int:
    return (month_end_exclusive(y, m) - timedelta(days=1)).day


def weekday_of(y: int, m: int, d: int) -> int:
    return date(y, m, d).weekday()  # Mon=0


def month_title(y: int, m: int) -> str:
    names = ["Январь","Февраль","Март","Апрель","Май","Июнь","Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"]
    return f"{names[m-1]} {y}"


def compress_days(days: list[int]) -> str:
    days = sorted(set([d for d in days if isinstance(d, int) and d > 0]))
    if not days:
        return "—"
    ranges = []
    a = b = days[0]
    for d in days[1:]:
        if d == b + 1:
            b = d
        else:
            ranges.append((a, b))
            a = b = d
    ranges.append((a, b))
    parts = []
    for a, b in ranges:
        parts.append(str(a) if a == b else f"{a}–{b}")
    return ", ".join(parts)


def parse_bool_cell(v) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "да", "true", "yes", "y", "есть", "кофе"):
        return True
    return False


# ================== Defaults ==================
DEFAULT_RATE_SUPPLY = 800
DEFAULT_RATE_NO_SUPPLY = 400
DEFAULT_RATE_INVENTORY = 400
DEFAULT_RATE_COFFEE = 100  # фикс


SLOT_DAY = "DAY"
SLOT_FULL_INVENT = "FULL_INVENT"   # только ПТ и СБ


# ================== DB schema ==================
def ensure_tables():
    with engine.begin() as conn:
        # В проде на Render деплой может зависать из-за DDL-lock'ов при активных пользователях.
        # Ставим таймауты, чтобы старт не висел бесконечно.
        try:
            conn.execute(text("SET lock_timeout = '5s';"))
            conn.execute(text("SET statement_timeout = '30s';"))
        except Exception:
            pass
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS merchants (
            id SERIAL PRIMARY KEY,
            fio TEXT NOT NULL,
            fio_norm TEXT,
            pass_hash TEXT NOT NULL,
            telegram_id BIGINT UNIQUE,
            tu TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))
        conn.execute(text("ALTER TABLE merchants ADD COLUMN IF NOT EXISTS fio_norm TEXT;"))
        conn.execute(text("ALTER TABLE merchants ADD COLUMN IF NOT EXISTS tu TEXT;"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS merchants_fio_norm_uq ON merchants(fio_norm);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS merchants_tu_idx ON merchants(tu);"))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS supplies (
            id SERIAL PRIMARY KEY,
            point_code TEXT NOT NULL,
            supply_date DATE NOT NULL,
            boxes INTEGER NOT NULL,
            has_supply BOOLEAN NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(point_code, supply_date)
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS visits (
            id SERIAL PRIMARY KEY,
            merchant_id INTEGER NOT NULL REFERENCES merchants(id) ON DELETE CASCADE,
            point_code TEXT NOT NULL,
            visit_date DATE NOT NULL,
            slot TEXT NOT NULL, -- DAY / FULL_INVENT
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(merchant_id, point_code, visit_date, slot)
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS reimbursements (
            id SERIAL PRIMARY KEY,
            merchant_id INTEGER NOT NULL REFERENCES merchants(id) ON DELETE CASCADE,
            point_code TEXT NOT NULL,
            month_key DATE NOT NULL, -- 1-е число месяца
            amount INTEGER NOT NULL,
            note TEXT NOT NULL,
            kind TEXT NOT NULL DEFAULT 'NOTE', -- NOTE / REIMB
            receipt_file_id TEXT,
            receipt_uploaded_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))
        conn.execute(text("ALTER TABLE reimbursements ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'NOTE';"))
        conn.execute(text("ALTER TABLE reimbursements ADD COLUMN IF NOT EXISTS receipt_file_id TEXT;"))
        conn.execute(text("ALTER TABLE reimbursements ADD COLUMN IF NOT EXISTS receipt_uploaded_at TIMESTAMPTZ;"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS reimb_idx ON reimbursements(merchant_id, point_code, month_key);"))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS submissions (
            id SERIAL PRIMARY KEY,
            merchant_id INTEGER NOT NULL REFERENCES merchants(id) ON DELETE CASCADE,
            month_key DATE NOT NULL,
            submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_after_submit_at TIMESTAMPTZ,
            UNIQUE(merchant_id, month_key)
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS point_rates (
            id SERIAL PRIMARY KEY,
            point_code TEXT NOT NULL,
            month_key DATE NOT NULL,
            rate_supply INTEGER NOT NULL,
            rate_no_supply INTEGER NOT NULL,
            rate_inventory INTEGER NOT NULL,
            coffee_enabled BOOLEAN NOT NULL DEFAULT FALSE,
            pay_lt5 BOOLEAN NOT NULL DEFAULT FALSE,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(point_code, month_key)
        );
        """))
        conn.execute(text("ALTER TABLE point_rates ADD COLUMN IF NOT EXISTS coffee_enabled BOOLEAN NOT NULL DEFAULT FALSE;"))
        conn.execute(text("ALTER TABLE point_rates ADD COLUMN IF NOT EXISTS pay_lt5 BOOLEAN NOT NULL DEFAULT FALSE;"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS point_rates_month_idx ON point_rates(month_key);"))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS tu_admins (
            tu TEXT PRIMARY KEY,
            telegram_id BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))
        # Заполним fio_norm для старых записей (если вдруг осталось пустым). Не критично, поэтому в try.
        try:
            conn.execute(text("""
                UPDATE merchants
                SET fio_norm = lower(replace(replace(fio, 'Ё', 'Е'), 'ё', 'е'))
                WHERE fio_norm IS NULL OR fio_norm = '';
            """))
        except Exception:
            pass


# ================== DB ops ==================
def get_merch_by_tg_id(tg_id: int):
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT id, fio, tu FROM merchants WHERE telegram_id=:tg"),
            {"tg": tg_id}
        ).mappings().first()


def get_merch_by_fio(fio: str):
    fn = fio_norm(fio)
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT id, fio, pass_hash, telegram_id, tu FROM merchants WHERE fio_norm=:fn"),
            {"fn": fn}
        ).mappings().first()


def bind_merch_tg_id(merch_id: int, tg_id: int):
    with engine.begin() as conn:
        conn.execute(text("UPDATE merchants SET telegram_id=:tg WHERE id=:id"), {"tg": tg_id, "id": merch_id})


def unbind_merch_tg_id(tg_id: int) -> bool:
    with engine.begin() as conn:
        r = conn.execute(text("UPDATE merchants SET telegram_id=NULL WHERE telegram_id=:tg"), {"tg": tg_id})
        return (r.rowcount or 0) > 0


def upsert_merchant(conn, fio_raw: str, phone_raw: str, tu: str) -> tuple[bool, bool, bool]:
    fio_disp = fio_display(fio_raw or "")
    fio_n = fio_norm(fio_raw or "")
    last4 = extract_last4_from_phone(phone_raw or "")
    tu = (tu or "").strip().lower()

    if not fio_n or len(fio_n.split(" ")) < 2 or not re.fullmatch(r"\d{4}", last4):
        return False, False, True

    ph = hash_last4(last4)

    res = conn.execute(text("""
        INSERT INTO merchants (fio, fio_norm, pass_hash, tu)
        VALUES (:fio, :fn, :ph, :tu)
        ON CONFLICT (fio_norm) DO UPDATE
          SET fio=EXCLUDED.fio,
              pass_hash=EXCLUDED.pass_hash,
              tu=EXCLUDED.tu
        RETURNING xmax;
    """), {"fio": fio_disp, "fn": fio_n, "ph": ph, "tu": tu})

    xmax = res.scalar()
    if xmax == 0:
        return True, False, False
    return False, True, False


def point_has_any_supply_in_month(point_code: str, y: int, m: int) -> bool:
    start = month_start(y, m)
    end = month_end_exclusive(y, m)
    with engine.connect() as conn:
        v = conn.execute(text("""
            SELECT 1
            FROM supplies
            WHERE point_code=:p AND supply_date>=:s AND supply_date<:e
            LIMIT 1
        """), {"p": point_code, "s": start, "e": end}).scalar()
    return v is not None


def get_supply_boxes_map(point_code: str, y: int, m: int) -> dict[int, int]:
    start = month_start(y, m)
    end = month_end_exclusive(y, m)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT supply_date, boxes
            FROM supplies
            WHERE point_code=:p AND supply_date>=:s AND supply_date<:e
        """), {"p": point_code, "s": start, "e": end}).mappings().all()
    out: dict[int, int] = {}
    for r in rows:
        d: date = r["supply_date"]
        out[d.day] = int(r["boxes"])
    return out


def get_visits_for_month(merchant_id: int, point_code: str, y: int, m: int) -> dict[int, set[str]]:
    start = month_start(y, m)
    end = month_end_exclusive(y, m)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT visit_date, slot
            FROM visits
            WHERE merchant_id=:mid AND point_code=:p
              AND visit_date>=:s AND visit_date<:e
        """), {"mid": merchant_id, "p": point_code, "s": start, "e": end}).mappings().all()
    out: dict[int, set[str]] = {}
    for r in rows:
        d: date = r["visit_date"]
        out.setdefault(d.day, set()).add(str(r["slot"]))
    return out


def get_submission_status(merchant_id: int, y: int, m: int):
    mk = month_start(y, m)
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT submitted_at, updated_after_submit_at
            FROM submissions
            WHERE merchant_id=:mid AND month_key=:mk
        """), {"mid": merchant_id, "mk": mk}).mappings().first()


def mark_submitted(merchant_id: int, y: int, m: int) -> bool:
    mk = month_start(y, m)
    with engine.begin() as conn:
        existing = conn.execute(text("""
            SELECT id FROM submissions WHERE merchant_id=:mid AND month_key=:mk
        """), {"mid": merchant_id, "mk": mk}).scalar()
        if existing:
            return False
        conn.execute(text("""
            INSERT INTO submissions (merchant_id, month_key, submitted_at)
            VALUES (:mid, :mk, NOW())
        """), {"mid": merchant_id, "mk": mk})
        return True


def touch_updated_after_submit(merchant_id: int, y: int, m: int):
    mk = month_start(y, m)
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE submissions
            SET updated_after_submit_at=NOW()
            WHERE merchant_id=:mid AND month_key=:mk
        """), {"mid": merchant_id, "mk": mk})


def get_points_for_month(merchant_id: int, y: int, m: int) -> list[str]:
    start = month_start(y, m)
    end = month_end_exclusive(y, m)
    mk = start
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT point_code FROM (
                SELECT point_code FROM visits
                WHERE merchant_id=:mid AND visit_date>=:s AND visit_date<:e
                UNION
                SELECT point_code FROM reimbursements
                WHERE merchant_id=:mid AND month_key=:mk
            ) t
            ORDER BY point_code
        """), {"mid": merchant_id, "s": start, "e": end, "mk": mk}).all()
    return [r[0] for r in rows if r and r[0]]


def get_point_rates(point_code: str, y: int, m: int) -> tuple[int, int, int, bool, bool]:
    mk = month_start(y, m)
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT rate_supply, rate_no_supply, rate_inventory, coffee_enabled, pay_lt5
            FROM point_rates
            WHERE point_code=:p AND month_key=:mk
        """), {"p": point_code, "mk": mk}).mappings().first()
    if not row:
        return DEFAULT_RATE_SUPPLY, DEFAULT_RATE_NO_SUPPLY, DEFAULT_RATE_INVENTORY, False, False
    return (
        int(row["rate_supply"]),
        int(row["rate_no_supply"]),
        int(row["rate_inventory"]),
        bool(row["coffee_enabled"]),
        bool(row["pay_lt5"]),
    )


def get_reimb_aggregates(merchant_id: int, point_code: str, y: int, m: int) -> tuple[int, int, int, int]:
    mk = month_start(y, m)
    with engine.connect() as conn:
        notes_sum = conn.execute(text("""
            SELECT COALESCE(SUM(amount),0) FROM reimbursements
            WHERE merchant_id=:mid AND point_code=:p AND month_key=:mk AND kind='NOTE'
        """), {"mid": merchant_id, "p": point_code, "mk": mk}).scalar()

        reimb_sum = conn.execute(text("""
            SELECT COALESCE(SUM(amount),0) FROM reimbursements
            WHERE merchant_id=:mid AND point_code=:p AND month_key=:mk AND kind='REIMB'
        """), {"mid": merchant_id, "p": point_code, "mk": mk}).scalar()

        reimb_count = conn.execute(text("""
            SELECT COUNT(*) FROM reimbursements
            WHERE merchant_id=:mid AND point_code=:p AND month_key=:mk AND kind='REIMB'
        """), {"mid": merchant_id, "p": point_code, "mk": mk}).scalar()

        reimb_missing_receipt = conn.execute(text("""
            SELECT COUNT(*) FROM reimbursements
            WHERE merchant_id=:mid AND point_code=:p AND month_key=:mk
              AND kind='REIMB' AND receipt_file_id IS NULL
        """), {"mid": merchant_id, "p": point_code, "mk": mk}).scalar()

    return int(notes_sum or 0), int(reimb_sum or 0), int(reimb_count or 0), int(reimb_missing_receipt or 0)




def get_reimb_comments(merchant_id: int, point_code: str, y: int, m: int) -> tuple[str, str, str]:
    """
    Для отчёта.

    Возвращает:
    - комментарии примечаний (NOTE)
    - комментарии возмещений (REIMB) + отметка чек/без чека для каждой строки
    - флаг "Есть возмещения без чека" (Да/Нет)
    """
    mk = month_start(y, m)
    with engine.connect() as conn:
        notes = conn.execute(text("""
            SELECT amount, note
            FROM reimbursements
            WHERE merchant_id=:mid AND point_code=:p AND month_key=:mk AND kind='NOTE'
            ORDER BY created_at
        """), {"mid": merchant_id, "p": point_code, "mk": mk}).mappings().all()

        reimb = conn.execute(text("""
            SELECT amount, note, receipt_file_id
            FROM reimbursements
            WHERE merchant_id=:mid AND point_code=:p AND month_key=:mk AND kind='REIMB'
            ORDER BY created_at
        """), {"mid": merchant_id, "p": point_code, "mk": mk}).mappings().all()

    note_parts: list[str] = []
    for r in notes:
        amt = int(r["amount"] or 0)
        txt = (r["note"] or "").strip()
        note_parts.append(f"{amt} — {txt}" if txt else str(amt))

    reimb_parts: list[str] = []
    missing = False
    for r in reimb:
        amt = int(r["amount"] or 0)
        txt = (r["note"] or "").strip()
        has_receipt = bool(r["receipt_file_id"])
        if not has_receipt:
            missing = True
        label = "чек" if has_receipt else "без чека"
        reimb_parts.append(f"{amt} — {txt} ({label})" if txt else f"{amt} ({label})")

    return (
        " | ".join(note_parts),
        " | ".join(reimb_parts),
        ("Да" if missing else "Нет"),
    )

def effective_has_supply(boxes: int, pay_lt5: bool) -> bool:
    # pay_lt5=True => кофесушки: если коробок > 0, то это оплачиваемая поставка
    if boxes <= 0:
        return False
    return True if pay_lt5 else (boxes >= 5)


def compute_point_total(merchant_id: int, point_code: str, y: int, m: int) -> tuple[int, int, int, int, int, int, int, bool, int, bool]:
    boxes_map = get_supply_boxes_map(point_code, y, m)
    visits = get_visits_for_month(merchant_id, point_code, y, m)
    rate_supply, rate_no_supply, rate_inv, coffee_on, pay_lt5 = get_point_rates(point_code, y, m)
    notes_sum, reimb_sum, reimb_count, reimb_missing_receipt = get_reimb_aggregates(merchant_id, point_code, y, m)

    total = 0
    day_cnt = 0
    cnt_supply_day = 0
    cnt_no_supply_day = 0
    cnt_full_inv = 0

    for day, slots in visits.items():
        if SLOT_DAY in slots:
            day_cnt += 1
            boxes = boxes_map.get(day, 0)
            if effective_has_supply(boxes, pay_lt5):
                cnt_supply_day += 1
                total += rate_supply
            else:
                cnt_no_supply_day += 1
                total += rate_no_supply

        if SLOT_FULL_INVENT in slots:
            cnt_full_inv += 1
            total += rate_inv

    coffee_sum = 0
    if coffee_on and day_cnt > 0:
        coffee_sum = DEFAULT_RATE_COFFEE * day_cnt
        total += coffee_sum

    total += notes_sum + reimb_sum

    return (
        total,
        cnt_supply_day,
        cnt_no_supply_day,
        (cnt_supply_day + cnt_no_supply_day),
        cnt_full_inv,
        notes_sum,
        reimb_sum,
        coffee_on,
        coffee_sum,
        (reimb_missing_receipt > 0),
    )


def compute_overall_total(merchant_id: int, y: int, m: int) -> tuple[int, dict[str, int]]:
    points = get_points_for_month(merchant_id, y, m)
    per_point: dict[str, int] = {}
    total = 0
    for p in points:
        s, *_ = compute_point_total(merchant_id, p, y, m)
        per_point[p] = s
        total += s
    return total, per_point


# ================== Supplies parsing ==================
RU_MONTH = {
    "янв": 1, "январ": 1,
    "фев": 2, "феврал": 2,
    "мар": 3, "март": 3,
    "апр": 4, "апрел": 4,
    "май": 5,
    "июн": 6, "июнь": 6,
    "июл": 7, "июль": 7,
    "авг": 8, "август": 8,
    "сен": 9, "сент": 9,
    "окт": 10, "октябр": 10,
    "ноя": 11, "ноябр": 11,
    "дек": 12, "декабр": 12,
}


def parse_header_date(cell_value, default_year: int) -> date | None:
    if cell_value is None:
        return None
    if isinstance(cell_value, datetime):
        return cell_value.date()
    if isinstance(cell_value, date):
        return cell_value

    s = str(cell_value).strip().lower()
    s = s.replace(",", ".").replace("-", ".")
    s = re.sub(r"\s+", " ", s)

    m = re.match(r"^(\d{1,2})[.\s](\D+)$", s)  # 20.янв
    if m:
        day = int(m.group(1))
        mon_raw = re.sub(r"[^а-я]", "", m.group(2).strip())
        mon = None
        for k, v in RU_MONTH.items():
            if mon_raw.startswith(k):
                mon = v
                break
        if mon:
            return date(default_year, mon, day)

    m2 = re.match(r"^(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?$", s)  # 20.01 or 20.01.2026
    if m2:
        day = int(m2.group(1))
        mon = int(m2.group(2))
        yr = m2.group(3)
        year = default_year
        if yr:
            y = int(yr)
            if y < 100:
                y += 2000
            year = y
        return date(year, mon, day)

    return None


# ================== FSM ==================
class UploadMerchants(StatesGroup):
    waiting_file = State()


class UploadSupplies(StatesGroup):
    waiting_file = State()


class UploadRates(StatesGroup):
    waiting_file = State()


class LoginFlow(StatesGroup):
    waiting_fio = State()
    waiting_last4 = State()


class FillFlow(StatesGroup):
    waiting_point = State()
    calendar = State()


class PRFlow(StatesGroup):
    choosing_kind = State()
    waiting_amount = State()
    waiting_text = State()
    waiting_receipt = State()


class ResetFlow(StatesGroup):
    waiting_code = State()


# ================== Notifications ==================
async def notify_admins(text_msg: str):
    for aid in ADMIN_IDS:
        try:
            await bot.send_message(aid, text_msg)
        except Exception:
            pass


def get_tu_admin_id(tu: str) -> int | None:
    tu = (tu or "").strip().lower()
    if not tu:
        return None
    with engine.connect() as conn:
        v = conn.execute(text("SELECT telegram_id FROM tu_admins WHERE tu=:tu"), {"tu": tu}).scalar()
    return int(v) if v is not None else None


async def maybe_notify_post_submit_change(merchant_id: int, y: int, m: int, action: str):
    status = get_submission_status(merchant_id, y, m)
    if not status:
        return
    touch_updated_after_submit(merchant_id, y, m)

    with engine.connect() as conn:
        fio = conn.execute(text("SELECT fio FROM merchants WHERE id=:id"), {"id": merchant_id}).scalar()

    total, _ = compute_overall_total(merchant_id, y, m)
    await notify_admins(
        "⚠️ Изменения после отправки сверки!\n"
        f"Мерч: {fio}\n"
        f"Месяц: {y}-{m:02d}\n"
        f"Действие: {action}\n"
        f"Текущая общая сумма: {total} ₽"
    )


# ================== Cancel/Restart ==================
@dp.message(F.text.in_({"Отмена", "Заново"}))
async def cancel_or_restart(message: types.Message, state: FSMContext):
    # Особый случай: если мерч на этапе обязательного чека — удаляем черновик возмещения
    if await state.get_state() == PRFlow.waiting_receipt.state and (message.text or "").strip().lower() == "отмена":
        data = await state.get_data()
        rid = data.get("pr_reimb_id")
        if rid:
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM reimbursements WHERE id=:id AND kind='REIMB' AND receipt_file_id IS NULL"), {"id": int(rid)})
        await state.clear()
        await message.answer("Ок, отменил. Возмещение не сохранено (чек обязателен).", reply_markup=ReplyKeyboardRemove())
        return

    await state.clear()
    if (message.text or "").strip() == "Отмена":
        await message.answer("Ок, отменил. Напиши /start чтобы начать заново.", reply_markup=ReplyKeyboardRemove())
    else:
        await message.answer("Начнём заново. Напиши /start", reply_markup=ReplyKeyboardRemove())


# ================== Basic ==================
@dp.message(Command("start"))
async def start_handler(message: types.Message, state: FSMContext):
    merch = get_merch_by_tg_id(message.from_user.id)
    if merch:
        await state.clear()
        await message.answer(f"✅ Вы уже авторизованы как: {merch['fio']}", reply_markup=MAIN_KB)
        return

    await state.set_state(LoginFlow.waiting_fio)
    await message.answer(
        "Привет! 👋\n"
        "Для входа введи ФИО полностью.\n\n"
        "Пример:\n"
        "Иванов Иван Иванович\n\n"
        "Если передумал — нажми «Отмена».",
        reply_markup=LOGIN_KB
    )


@dp.message(Command("myid"))
async def my_id(message: types.Message):
    await message.answer(f"Ваш Telegram ID: {message.from_user.id}")


@dp.message(Command("pingdb"))
async def ping_db(message: types.Message):
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
        await message.answer("✅ База данных доступна.")
    except Exception as e:
        await message.answer(f"❌ Ошибка БД: {type(e).__name__}: {e}")


# ================== Login ==================
async def verify_login_last4(user_tg_id: int, fio_in: str, last4: str) -> tuple[bool, str]:
    merch = get_merch_by_fio(fio_in)
    if not merch:
        return False, "❌ Ошибка: запись не найдена. Начни заново: /start"

    if hash_last4(last4) != merch["pass_hash"]:
        return False, "❌ Эти 4 цифры не совпадают с номером в системе.\nПопробуй ещё раз."

    if merch["telegram_id"] is not None and int(merch["telegram_id"]) != user_tg_id:
        return False, "⛔ Этот аккаунт уже привязан к другому Telegram. Обратитесь к администратору."

    bind_merch_tg_id(merch["id"], user_tg_id)
    return True, f"✅ Успешный вход. Вы: {merch['fio']}"


@dp.message(LoginFlow.waiting_fio)
async def login_get_fio(message: types.Message, state: FSMContext):
    fio_in = fio_display(message.text or "")
    merch = get_merch_by_fio(fio_in)
    if not merch:
        await message.answer(
            "❌ Не получилось найти ФИО.\nПроверь написание или обратись к ТУ.",
            reply_markup=LOGIN_KB
        )
        return

    await state.update_data(fio=fio_in)
    await state.set_state(LoginFlow.waiting_last4)
    await message.answer("Теперь введи последние 4 цифры номера телефона (только 4 цифры).", reply_markup=LOGIN_KB)


@dp.message(LoginFlow.waiting_last4)
async def login_get_last4(message: types.Message, state: FSMContext):
    last4 = (message.text or "").strip()
    if not re.fullmatch(r"\d{4}", last4):
        await message.answer("Нужно ровно 4 цифры. Пример: 6384", reply_markup=LOGIN_KB)
        return

    data = await state.get_data()
    fio_in = data.get("fio", "")
    ok, msg = await verify_login_last4(message.from_user.id, fio_in, last4)
    if ok:
        await state.clear()
        await message.answer(msg, reply_markup=MAIN_KB)
    else:
        await message.answer(msg, reply_markup=LOGIN_KB)


# ================== Admin: TU mapping ==================
@dp.message(Command("set_tu_admin"))
async def set_tu_admin_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer("Использование: /set_tu_admin <ту> <telegram_id>\nПример: /set_tu_admin хрупов 345235374")
        return
    tu = parts[1].strip().lower()
    tg = parts[2].strip()
    if not tg.isdigit():
        await message.answer("telegram_id должен быть числом.")
        return
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO tu_admins (tu, telegram_id, updated_at)
            VALUES (:tu, :tg, NOW())
            ON CONFLICT (tu) DO UPDATE
              SET telegram_id=EXCLUDED.telegram_id,
                  updated_at=NOW()
        """), {"tu": tu, "tg": int(tg)})
    await message.answer(f"✅ Привязал ТУ '{tu}' -> Telegram ID {tg}")


@dp.message(Command("tu_admins"))
async def tu_admins_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT tu, telegram_id FROM tu_admins ORDER BY tu")).mappings().all()
    if not rows:
        await message.answer("Пока нет привязок ТУ -> Telegram ID.\nДобавь: /set_tu_admin <ту> <tg_id>")
        return
    lines = ["ТУ -> Telegram ID:"]
    for r in rows:
        lines.append(f"- {r['tu']}: {r['telegram_id']}")
    await message.answer("\n".join(lines))


# ================== Admin: upload_merchants (xlsx) ==================
@dp.message(Command("upload_merchants"))
async def upload_merchants_cmd(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Использование: /upload_merchants <ту>\nПример: /upload_merchants хрупов")
        return
    tu = parts[1].strip().lower()
    await state.set_state(UploadMerchants.waiting_file)
    await state.update_data(upload_tu=tu)
    await message.answer(
        f"Пришли Excel .xlsx с 2 столбцами:\n"
        f"A: ФИО\nB: Телефон\n\n"
        f"ТУ будет записан как: {tu}\n"
        f"Телефон может быть в любом формате (бот возьмёт последние 4 цифры).",
        reply_markup=CANCEL_KB
    )


@dp.message(UploadMerchants.waiting_file, F.document)
async def handle_merchants_file(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return
    doc = message.document
    try:
        data = await state.get_data()
        tu = (data.get("upload_tu") or "").strip().lower()
        if not tu:
            await state.clear()
            await message.answer("❌ Не указан ТУ. Запусти: /upload_merchants <ту>", reply_markup=ReplyKeyboardRemove())
            return

        if not (doc.file_name or "").lower().endswith(".xlsx"):
            await state.clear()
            await message.answer("❌ Нужен .xlsx файл.", reply_markup=ReplyKeyboardRemove())
            return

        f = await bot.get_file(doc.file_id)
        buf = BytesIO()
        await bot.download_file(f.file_path, destination=buf)

        wb = openpyxl.load_workbook(BytesIO(buf.getvalue()), read_only=True, data_only=True)
        ws = wb.worksheets[0]

        added = updated = skipped = 0
        with engine.begin() as conn:
            for row in ws.iter_rows(min_row=1, values_only=True):
                if not row or len(row) < 2:
                    continue
                a = "" if row[0] is None else str(row[0])
                b = "" if row[1] is None else str(row[1])
                ins, upd, sk = upsert_merchant(conn, a, b, tu)
                added += 1 if ins else 0
                updated += 1 if upd else 0
                skipped += 1 if sk else 0

        await state.clear()
        await message.answer(
            f"✅ Готово ({tu}).\nДобавлено: {added}\nОбновлено: {updated}\nПропущено: {skipped}",
            reply_markup=ReplyKeyboardRemove()
        )

    except Exception as e:
        await state.clear()
        await message.answer(f"❌ Ошибка файла мерчей: {type(e).__name__}: {e}", reply_markup=ReplyKeyboardRemove())


# ================== Admin: upload_supplies (xlsx) ==================
@dp.message(Command("upload_supplies"))
async def upload_supplies_cmd(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return
    parts = (message.text or "").split()
    y = datetime.utcnow().year
    if len(parts) >= 2 and parts[1].isdigit():
        y = int(parts[1])
    await state.set_state(UploadSupplies.waiting_file)
    await state.update_data(supplies_year=y)
    await message.answer(
        "Пришли Excel .xlsx с поставками:\n"
        "- строки: точки\n"
        "- в шапке: даты\n"
        "- в ячейках: коробки\n\n"
        f"Год по умолчанию (если в шапке нет года): {y}",
        reply_markup=CANCEL_KB
    )


@dp.message(UploadSupplies.waiting_file, F.document)
async def handle_supplies_file(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return
    doc = message.document
    try:
        if not (doc.file_name or "").lower().endswith(".xlsx"):
            await state.clear()
            await message.answer("❌ Нужен .xlsx файл.", reply_markup=ReplyKeyboardRemove())
            return

        data = await state.get_data()
        default_year = int(data.get("supplies_year", datetime.utcnow().year))

        f = await bot.get_file(doc.file_id)
        buf = BytesIO()
        await bot.download_file(f.file_path, destination=buf)

        wb = openpyxl.load_workbook(BytesIO(buf.getvalue()), read_only=True, data_only=True)
        ws = wb.worksheets[0]

        header = list(next(ws.iter_rows(min_row=1, max_row=1, values_only=True)))
        if not header or len(header) < 3:
            raise ValueError("Не смог прочитать шапку: ожидаю TT + (игнор) + даты")

        date_cols: dict[int, date] = {}
        for idx in range(2, len(header)):
            d = parse_header_date(header[idx], default_year)
            if d:
                date_cols[idx] = d
        if not date_cols:
            raise ValueError("Не нашёл даты в шапке (после TT должны быть даты)")

        inserted = updated = skipped = 0
        with engine.begin() as conn:
            for r in ws.iter_rows(min_row=2, values_only=True):
                if not r:
                    continue
                point = normalize_point_code(r[0])
                if not point:
                    continue
                for col_idx, d in date_cols.items():
                    if col_idx >= len(r):
                        continue
                    val = r[col_idx]
                    if val is None or str(val).strip() == "":
                        continue
                    try:
                        boxes = int(float(val))
                    except Exception:
                        skipped += 1
                        continue
                    has_supply = boxes >= 5  # базовый флаг, реальное правило может быть pay_lt5 по rates
                    res = conn.execute(text("""
                        INSERT INTO supplies (point_code, supply_date, boxes, has_supply)
                        VALUES (:p, :d, :b, :hs)
                        ON CONFLICT (point_code, supply_date) DO UPDATE
                          SET boxes=EXCLUDED.boxes,
                              has_supply=EXCLUDED.has_supply
                        RETURNING xmax;
                    """), {"p": point, "d": d, "b": boxes, "hs": has_supply})
                    xmax = res.scalar()
                    inserted += 1 if xmax == 0 else 0
                    updated += 1 if xmax != 0 else 0

        await state.clear()
        await message.answer(
            f"✅ Поставки загружены.\nДобавлено: {inserted}\nОбновлено: {updated}\nПропущено: {skipped}",
            reply_markup=ReplyKeyboardRemove()
        )

    except Exception as e:
        await state.clear()
        await message.answer(f"❌ Ошибка файла поставок: {type(e).__name__}: {e}", reply_markup=ReplyKeyboardRemove())


# ================== Admin: upload_rates (xlsx) ==================
def parse_month_arg(s: str) -> tuple[int, int] | None:
    m = re.fullmatch(r"(\d{4})-(\d{2})", (s or "").strip())
    if not m:
        return None
    y = int(m.group(1))
    mm = int(m.group(2))
    if mm < 1 or mm > 12:
        return None
    return y, mm


@dp.message(Command("upload_rates"))
async def upload_rates_cmd(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Использование: /upload_rates YYYY-MM\nПример: /upload_rates 2026-01")
        return
    ym = parse_month_arg(parts[1])
    if not ym:
        await message.answer("Неверный формат. Нужно YYYY-MM, например 2026-01")
        return
    y, m = ym
    await state.set_state(UploadRates.waiting_file)
    await state.update_data(rates_y=y, rates_m=m)
    await message.answer(
        f"Пришли Excel .xlsx со ставками на {y}-{m:02d}.\n"
        "Столбцы:\n"
        "A: номер точки\n"
        "B: ставка выход с поставкой\n"
        "C: ставка выход без поставки\n"
        "D: ставка полный инвент\n"
        "E: кофемашина (да/нет)\n"
        "F: оплачивать поставку <5 коробок (да/нет) [пусто = нет]\n",
        reply_markup=CANCEL_KB
    )


@dp.message(UploadRates.waiting_file, F.document)
async def handle_rates_file(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return
    doc = message.document
    try:
        if not (doc.file_name or "").lower().endswith(".xlsx"):
            await state.clear()
            await message.answer("❌ Нужен .xlsx файл.", reply_markup=ReplyKeyboardRemove())
            return

        data = await state.get_data()
        y = int(data["rates_y"])
        m = int(data["rates_m"])
        mk = month_start(y, m)

        f = await bot.get_file(doc.file_id)
        buf = BytesIO()
        await bot.download_file(f.file_path, destination=buf)

        wb = openpyxl.load_workbook(BytesIO(buf.getvalue()), read_only=True, data_only=True)
        ws = wb.worksheets[0]

        inserted = updated = skipped = 0
        with engine.begin() as conn:
            for r in ws.iter_rows(min_row=1, values_only=True):
                if not r or len(r) < 4:
                    continue
                point = normalize_point_code(r[0])
                if not point:
                    continue
                try:
                    rs = int(float(r[1]))
                    rns = int(float(r[2]))
                    rinv = int(float(r[3]))
                except Exception:
                    skipped += 1
                    continue

                coffee = parse_bool_cell(r[4]) if len(r) >= 5 else False
                pay_lt5 = parse_bool_cell(r[5]) if len(r) >= 6 else False  # пусто = False

                if rs <= 0 or rns <= 0 or rinv <= 0:
                    skipped += 1
                    continue

                res = conn.execute(text("""
                    INSERT INTO point_rates (point_code, month_key, rate_supply, rate_no_supply, rate_inventory, coffee_enabled, pay_lt5, updated_at)
                    VALUES (:p, :mk, :rs, :rns, :rinv, :coffee, :pay_lt5, NOW())
                    ON CONFLICT (point_code, month_key) DO UPDATE
                      SET rate_supply=EXCLUDED.rate_supply,
                          rate_no_supply=EXCLUDED.rate_no_supply,
                          rate_inventory=EXCLUDED.rate_inventory,
                          coffee_enabled=EXCLUDED.coffee_enabled,
                          pay_lt5=EXCLUDED.pay_lt5,
                          updated_at=NOW()
                    RETURNING xmax;
                """), {"p": point, "mk": mk, "rs": rs, "rns": rns, "rinv": rinv, "coffee": coffee, "pay_lt5": pay_lt5})

                xmax = res.scalar()
                inserted += 1 if xmax == 0 else 0
                updated += 1 if xmax != 0 else 0

        await state.clear()
        await message.answer(
            f"✅ Ставки загружены на {y}-{m:02d}.\nДобавлено: {inserted}\nОбновлено: {updated}\nПропущено: {skipped}",
            reply_markup=ReplyKeyboardRemove()
        )

    except Exception as e:
        await state.clear()
        await message.answer(f"❌ Ошибка файла ставок: {type(e).__name__}: {e}", reply_markup=ReplyKeyboardRemove())


# ================== Reset / unlink ==================
@dp.message(Command("unlink_me"))
async def unlink_me_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return
    ok = unbind_merch_tg_id(message.from_user.id)
    await message.answer("✅ Отвязал ваш Telegram от мерчендайзера." if ok else "ℹ️ Ваш Telegram сейчас ни к кому не привязан.")


def make_reset_code() -> str:
    return "RESET-" + secrets.token_hex(2).upper()


@dp.message(Command("reset_data"))
async def reset_data_cmd(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return
    code = make_reset_code()
    await state.set_state(ResetFlow.waiting_code)
    await state.update_data(reset_kind="data", reset_code=code)
    await message.answer(
        "⚠️ Сброс данных сверок (выходы/примечания/возмещения/отправки/ставки).\n"
        "Мерчендайзеры и поставки останутся.\n\n"
        f"Чтобы подтвердить — отправь код:\n{code}",
        reply_markup=CANCEL_KB
    )


@dp.message(Command("reset_all"))
async def reset_all_cmd(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return
    code = make_reset_code()
    await state.set_state(ResetFlow.waiting_code)
    await state.update_data(reset_kind="all", reset_code=code)
    await message.answer(
        "⚠️ ПОЛНЫЙ СБРОС ВСЕГО (мерчи/поставки/ставки/сверки/примечания/возмещения и т.д.).\n"
        "Это необратимо.\n\n"
        f"Чтобы подтвердить — отправь код:\n{code}",
        reply_markup=CANCEL_KB
    )


@dp.message(ResetFlow.waiting_code)
async def reset_confirm(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        await message.answer("⛔ Эта команда только для администратора.")
        return

    txt = (message.text or "").strip()
    data = await state.get_data()
    code = data.get("reset_code")
    kind = data.get("reset_kind")

    if txt.lower() == "отмена":
        await state.clear()
        await message.answer("Ок, отменил.", reply_markup=ReplyKeyboardRemove())
        return

    if txt != code:
        await message.answer("❌ Неверный код. Отмена.", reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return

    with engine.begin() as conn:
        if kind == "data":
            conn.execute(text("DELETE FROM visits;"))
            conn.execute(text("DELETE FROM reimbursements;"))
            conn.execute(text("DELETE FROM submissions;"))
            conn.execute(text("DELETE FROM point_rates;"))
        else:
            conn.execute(text("DELETE FROM visits;"))
            conn.execute(text("DELETE FROM reimbursements;"))
            conn.execute(text("DELETE FROM submissions;"))
            conn.execute(text("DELETE FROM point_rates;"))
            conn.execute(text("DELETE FROM supplies;"))
            conn.execute(text("UPDATE merchants SET telegram_id=NULL;"))
            conn.execute(text("DELETE FROM merchants;"))

    await state.clear()
    await message.answer("✅ Готово. Сброс выполнен.", reply_markup=ReplyKeyboardRemove())


# ================== Calendar UI ==================
def build_calendar_kb(y: int, m: int, boxes_map: dict[int, int], pay_lt5: bool, visits: dict[int, set[str]], submitted: bool) -> InlineKeyboardMarkup:
    dim = days_in_month(y, m)
    first_wd = date(y, m, 1).weekday()
    rows: list[list[InlineKeyboardButton]] = []

    rows.append([InlineKeyboardButton(text=x, callback_data="noop") for x in ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]])

    day = 1
    row: list[InlineKeyboardButton] = []
    for _ in range(first_wd):
        row.append(InlineKeyboardButton(text=" ", callback_data="noop"))

    while day <= dim:
        boxes = boxes_map.get(day, 0)
        has_eff = effective_has_supply(boxes, pay_lt5)
        v = visits.get(day, set())

        marker_supply = "🟩" if has_eff else "⬜"
        marker_visit = ""
        if SLOT_DAY in v:
            marker_visit += "✅"
        if SLOT_FULL_INVENT in v:
            marker_visit += "📦"

        text_btn = f"{day:02d}{marker_supply}{marker_visit}"
        row.append(InlineKeyboardButton(text=text_btn, callback_data=f"cal:{day}"))

        if len(row) == 7:
            rows.append(row)
            row = []
        day += 1

    if row:
        while len(row) < 7:
            row.append(InlineKeyboardButton(text=" ", callback_data="noop"))
        rows.append(row)

    rows.append([
        InlineKeyboardButton(text=("📤 Сверка: отправлена" if submitted else "📤 Отправить сверку"), callback_data=("submit:noop" if submitted else "submit:send")),
        InlineKeyboardButton(text="✅ Готово", callback_data="done"),
    ])
    rows.append([
        InlineKeyboardButton(text="◀️ Месяц", callback_data="nav:prev"),
        InlineKeyboardButton(text="Месяц ▶️", callback_data="nav:next"),
    ])
    rows.append([InlineKeyboardButton(text="📍 Сменить точку", callback_data="back_point")])
    rows.append([InlineKeyboardButton(text="➕ Примечания / возмещения", callback_data="pr:start")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_day_action_kb(day: int, has_supply_effective: bool, can_full_inv: bool) -> InlineKeyboardMarkup:
    # Меню действий ТОЛЬКО для ПТ/СБ. Без слова «переключить».
    exit_text = "Отметить выход с поставкой" if has_supply_effective else "Отметить выход без поставки"
    rows = [
        [InlineKeyboardButton(text=exit_text, callback_data=f"toggle:{SLOT_DAY}:{day}")],
    ]
    if can_full_inv:
        rows.append([InlineKeyboardButton(text="Отметить полный инвент", callback_data=f"toggle:{SLOT_FULL_INVENT}:{day}")])
    rows.append([InlineKeyboardButton(text="↩️ Назад к календарю", callback_data="slot_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_pr_kind_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Примечание", callback_data="pr:kind:NOTE")],
        [InlineKeyboardButton(text="🚕 Возмещение расходов (чек обязателен)", callback_data="pr:kind:REIMB")],
        [InlineKeyboardButton(text="↩️ Назад", callback_data="pr:cancel")],
    ])


async def render_calendar(message_or_cb, state: FSMContext):
    data = await state.get_data()
    y = int(data["cal_y"])
    m = int(data["cal_m"])
    point = data["point_code"]

    merch = get_merch_by_tg_id(message_or_cb.from_user.id)
    if not merch:
        await state.clear()
        if isinstance(message_or_cb, types.CallbackQuery):
            await message_or_cb.message.answer("Сначала нужно войти: /start", reply_markup=ReplyKeyboardRemove())
            await message_or_cb.answer()
        else:
            await message_or_cb.answer("Сначала нужно войти: /start", reply_markup=ReplyKeyboardRemove())
        return

    boxes_map = get_supply_boxes_map(point, y, m)
    visits = get_visits_for_month(merch["id"], point, y, m)

    rate_supply, rate_no_supply, rate_inv, coffee_on, pay_lt5 = get_point_rates(point, y, m)
    notes_sum, reimb_sum, reimb_count, reimb_missing_receipt = get_reimb_aggregates(merch["id"], point, y, m)

    point_total, cnt_supply, cnt_nos, cnt_day_total, cnt_full_inv, notes_sum2, reimb_sum2, coffee_on2, coffee_sum, missing_receipts = compute_point_total(merch["id"], point, y, m)
    overall_total, per_point = compute_overall_total(merch["id"], y, m)

    days_supply = []
    days_no_supply = []
    inv_days = []
    for d, slots in visits.items():
        if SLOT_DAY in slots:
            boxes = boxes_map.get(d, 0)
            (days_supply if effective_has_supply(boxes, pay_lt5) else days_no_supply).append(d)
        if SLOT_FULL_INVENT in slots:
            inv_days.append(d)

    selected_block = (
        "📋 Выбранные дни:\n"
        f"🟩 Выходы с поставкой: {compress_days(days_supply)}\n"
        f"⬜ Выходы без поставок: {compress_days(days_no_supply)}\n"
        f"📦 Полный инвент: {compress_days(inv_days)}\n"
        f"📌 Выходы всего (день): {cnt_day_total}"
    )

    submitted = bool(get_submission_status(merch["id"], y, m))

    per_point_lines = []
    for p, s in per_point.items():
        mark = "👉" if p == point else "•"
        per_point_lines.append(f"{mark} {p}: {s} ₽")
    per_point_text = "\n".join(per_point_lines) if per_point_lines else "• (нет данных)"

    text_msg = (
        f"📍 Точка: {point}\n"
        f"🗓 {month_title(y, m)}\n\n"
        f"Ставки на {y}-{m:02d}:\n"
        f"• выход с поставкой: {rate_supply} ₽\n"
        f"• выход без поставки: {rate_no_supply} ₽\n"
        f"• полный инвент: {rate_inv} ₽\n"
        f"• кофемашина: {'ДА' if coffee_on else 'НЕТ'} (+{DEFAULT_RATE_COFFEE} ₽ за дневной выход)\n"
        f"• правило поставок: {'оплачивать <5 коробок' if pay_lt5 else 'оплачивать от 5 коробок'}\n\n"
        f"Легенда:\n"
        f"🟩 выход с поставкой | ⬜ выход без поставки\n"
        f"📦 полный инвент\n\n"
        f"{selected_block}\n\n"
        f"☕ Начислено за кофемашину: {coffee_sum} ₽\n"
        f"📝 Примечания (сумма): {notes_sum} ₽\n"
        f"🚕 Возмещения (сумма): {reimb_sum} ₽ (шт: {reimb_count})\n\n"
        f"💰 Сумма по этой точке: {point_total} ₽\n"
        f"📊 Общая сумма за месяц (все точки): {overall_total} ₽\n\n"
        f"Суммы по точкам:\n{per_point_text}"
    )

    kb = build_calendar_kb(y, m, boxes_map, pay_lt5, visits, submitted)

    try:
        if isinstance(message_or_cb, types.CallbackQuery):
            await message_or_cb.message.edit_text(text_msg, reply_markup=kb)
            await message_or_cb.answer()
        else:
            await message_or_cb.answer(text_msg, reply_markup=kb)
    except Exception:
        # если edit не прошёл — просто отправим новым сообщением
        if isinstance(message_or_cb, types.CallbackQuery):
            await message_or_cb.message.answer(text_msg, reply_markup=kb)
            await message_or_cb.answer()
        else:
            await message_or_cb.answer(text_msg, reply_markup=kb)


# ================== Collisions ==================
def add_or_remove_visit(merchant_id: int, point: str, y: int, m: int, day: int, slot: str) -> tuple[bool, bool]:
    d = date(y, m, day)
    with engine.begin() as conn:
        existing = conn.execute(text("""
            SELECT id FROM visits
            WHERE merchant_id=:mid AND point_code=:p AND visit_date=:d AND slot=:s
        """), {"mid": merchant_id, "p": point, "d": d, "s": slot}).scalar()

        if existing:
            conn.execute(text("DELETE FROM visits WHERE id=:id"), {"id": existing})
            return True, False

        conn.execute(text("""
            INSERT INTO visits (merchant_id, point_code, visit_date, slot)
            VALUES (:mid, :p, :d, :s)
            ON CONFLICT DO NOTHING
        """), {"mid": merchant_id, "p": point, "d": d, "s": slot})
        return False, True


def find_collisions(point: str, y: int, m: int, day: int, merchant_id: int) -> list[dict]:
    d = date(y, m, day)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT v.merchant_id, m.fio, m.telegram_id
            FROM visits v
            JOIN merchants m ON m.id=v.merchant_id
            WHERE v.point_code=:p AND v.visit_date=:d AND v.merchant_id<>:mid
        """), {"p": point, "d": d, "mid": merchant_id}).mappings().all()
    return [dict(r) for r in rows]


async def notify_collision(point: str, y: int, m: int, day: int, current_fio: str, others: list[dict]):
    d = date(y, m, day)
    other_names = ", ".join([o["fio"] for o in others]) if others else "?"
    await notify_admins(
        "⚠️ Пересечение!\n"
        f"Точка: {point}\n"
        f"Дата: {d.isoformat()}\n"
        f"Новый: {current_fio}\n"
        f"Уже отмечены: {other_names}"
    )
    for o in others:
        tg = o.get("telegram_id")
        if tg:
            try:
                await bot.send_message(int(tg), f"⚠️ Пересечение по точке {point} на {d.isoformat()}. Проверьте с руководителем.")
            except Exception:
                pass


# ================== Merch flow ==================
@dp.message(F.text == "Заполнить сверку")
async def fill_reconcile_start(message: types.Message, state: FSMContext):
    merch = get_merch_by_tg_id(message.from_user.id)
    if not merch:
        await message.answer("Сначала нужно войти: /start", reply_markup=ReplyKeyboardRemove())
        return
    await state.set_state(FillFlow.waiting_point)
    await message.answer(
        "Введите номер точки.\nПример: 2674\n\nЕсли хотите отменить — нажмите «Отмена».",
        reply_markup=CANCEL_KB
    )


@dp.message(FillFlow.waiting_point)
async def fill_reconcile_point(message: types.Message, state: FSMContext):
    txt = (message.text or "").strip()
    if txt.lower() == "отмена":
        await state.clear()
        await message.answer("Ок, отменил.", reply_markup=MAIN_KB)
        return

    point = normalize_point_code(txt)
    if len(point) < 3:
        await message.answer("Код точки слишком короткий. Попробуйте ещё раз.", reply_markup=CANCEL_KB)
        return

    now = datetime.utcnow().date()
    y, m = now.year, now.month

    # Проверка: если нет поставок по точке в текущем месяце — не пускаем в календарь
    if not point_has_any_supply_in_month(point, y, m):
        await message.answer(
            f"⚠️ Поставки на этой точке отсутствовали в заполняемом месяце ({y}-{m:02d}).\n"
            f"Обратитесь к территориальному управляющему.",
            reply_markup=CANCEL_KB
        )
        return

    await state.set_state(FillFlow.calendar)
    await state.update_data(point_code=point, cal_y=y, cal_m=m)
    await render_calendar(message, state)


@dp.callback_query(F.data == "noop")
async def noop(cb: types.CallbackQuery):
    await cb.answer()


@dp.callback_query(F.data == "done")
async def cal_done(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await cb.message.edit_text("✅ Готово. Возвращаю в меню.", reply_markup=None)
    except Exception:
        pass
    await cb.message.answer("Главное меню:", reply_markup=MAIN_KB)
    await cb.answer()


@dp.callback_query(F.data == "back_point")
async def cal_back_point(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(FillFlow.waiting_point)
    try:
        await cb.message.edit_text("Введите номер точки. Пример: 2674", reply_markup=None)
    except Exception:
        pass
    await cb.message.answer("Введите номер точки. Пример: 2674", reply_markup=CANCEL_KB)
    await cb.answer()


@dp.callback_query(F.data.startswith("nav:"))
async def cal_nav(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if "cal_y" not in data:
        await cb.answer()
        return

    y = int(data["cal_y"])
    m = int(data["cal_m"])
    direction = cb.data.split(":")[1]

    if direction == "prev":
        y2, m2 = ((y - 1, 12) if m == 1 else (y, m - 1))
    else:
        y2, m2 = ((y + 1, 1) if m == 12 else (y, m + 1))

    point = data["point_code"]

    # Проверка точки по выбранному месяцу
    if not point_has_any_supply_in_month(point, y2, m2):
        await cb.answer("Нет поставок по точке в этом месяце.")
        await cb.message.answer(
            f"⚠️ Поставки на этой точке отсутствовали в заполняемом месяце ({y2}-{m2:02d}).\n"
            f"Обратитесь к территориальному управляющему."
        )
        return

    await state.update_data(cal_y=y2, cal_m=m2)
    await render_calendar(cb, state)


# ======= НОВОЕ: общий хелпер для переключения слота (чтобы переиспользовать и для обычных дней) =======
async def _toggle_slot_and_refresh(cb: types.CallbackQuery, state: FSMContext, slot: str, day: int):
    data = await state.get_data()
    if "point_code" not in data:
        await cb.answer()
        return

    y = int(data["cal_y"])
    m = int(data["cal_m"])
    point = data["point_code"]

    if slot == SLOT_FULL_INVENT:
        wd = weekday_of(y, m, day)
        if wd not in (4, 5):
            await cb.answer("Полный инвент доступен только в пятницу и субботу.")
            return

    merch = get_merch_by_tg_id(cb.from_user.id)
    if not merch:
        await cb.answer("Сначала /start")
        return

    existed, added = add_or_remove_visit(merch["id"], point, y, m, day, slot)
    await maybe_notify_post_submit_change(
        merch["id"], y, m,
        f"{'удалил' if existed else 'добавил'} {slot} {point} {y}-{m:02d}-{day:02d}"
    )

    if added:
        others = find_collisions(point, y, m, day, merch["id"])
        if others:
            await cb.message.answer("⚠️ Внимание: есть пересечение с другим мерчендайзером. Нужна проверка.")
            await notify_collision(point, y, m, day, merch["fio"], others)

    await render_calendar(cb, state)


@dp.callback_query(F.data.startswith("cal:"))
async def cal_day_click(cb: types.CallbackQuery, state: FSMContext):
    """
    UX:
    - Пн–Чт и Вс: 1 тап по дню = переключить дневной выход (без меню)
    - ПТ и СБ: показываем меню (Выход с поставкой/без поставки + Полный инвент + Назад)
    """
    data = await state.get_data()
    if "point_code" not in data:
        await cb.answer()
        return

    y = int(data["cal_y"])
    m = int(data["cal_m"])
    point = data["point_code"]
    day = int(cb.data.split(":")[1])

    if day < 1 or day > days_in_month(y, m):
        await cb.answer()
        return

    wd = weekday_of(y, m, day)
    is_fri_sat = (wd == 4 or wd == 5)

    if not is_fri_sat:
        # Обычный день: сразу переключаем дневной выход
        await cb.answer("Ок")
        await _toggle_slot_and_refresh(cb, state, SLOT_DAY, day)
        return

    # ПТ/СБ: меню выбора
    _, _, _, _, pay_lt5 = get_point_rates(point, y, m)
    boxes_map = get_supply_boxes_map(point, y, m)
    boxes = boxes_map.get(day, 0)
    has_eff = effective_has_supply(boxes, pay_lt5)
    can_full_inv = True

    await cb.message.edit_text(
        f"{day:02d}.{m:02d} — выберите действие:",
        reply_markup=build_day_action_kb(day, has_eff, can_full_inv)
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("toggle:"))
async def cal_toggle_slot(cb: types.CallbackQuery, state: FSMContext):
    # Оставляем как было, но используем общий хелпер, чтобы логика была в одном месте
    _, slot, day_s = cb.data.split(":")
    day = int(day_s)
    await cb.answer("Ок")
    await _toggle_slot_and_refresh(cb, state, slot, day)


@dp.callback_query(F.data == "slot_cancel")
async def slot_cancel(cb: types.CallbackQuery, state: FSMContext):
    await render_calendar(cb, state)


# ================== Submit ==================
@dp.callback_query(F.data == "submit:noop")
async def submit_noop(cb: types.CallbackQuery):
    await cb.answer("Сверка уже отправлена.")


@dp.callback_query(F.data == "submit:send")
async def submit_send(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if "cal_y" not in data:
        await cb.answer()
        return

    merch = get_merch_by_tg_id(cb.from_user.id)
    if not merch:
        await cb.answer("Сначала /start")
        return

    y = int(data["cal_y"])
    m = int(data["cal_m"])

    created = mark_submitted(merch["id"], y, m)
    total, _ = compute_overall_total(merch["id"], y, m)

    if created:
        await cb.answer("Сверка отправлена ✅")
        await notify_admins(
            "📤 Сверка отправлена\n"
            f"Мерч: {merch['fio']}\n"
            f"ТУ: {merch.get('tu') or '-'}\n"
            f"Месяц: {y}-{m:02d}\n"
            f"Общая сумма: {total} ₽"
        )
    else:
        await cb.answer("Уже отправлено ранее.")

    await render_calendar(cb, state)


# ================== Notes / reimbursements ==================
@dp.callback_query(F.data == "pr:start")
async def pr_start(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(PRFlow.choosing_kind)
    await cb.message.answer(
        "➕ Примечания / возмещения",
        reply_markup=build_pr_kind_kb()
    )
    await cb.answer()


@dp.callback_query(F.data == "pr:cancel")
async def pr_cancel(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(FillFlow.calendar)
    await cb.answer("Ок")
    await render_calendar(cb, state)


@dp.callback_query(F.data.startswith("pr:kind:"))
async def pr_kind(cb: types.CallbackQuery, state: FSMContext):
    kind = cb.data.split(":")[-1]
    if kind not in ("NOTE", "REIMB"):
        await cb.answer()
        return
    await state.update_data(pr_kind=kind)
    await state.set_state(PRFlow.waiting_amount)

    if kind == "NOTE":
        await cb.message.answer(
            "Введите сумму (целое число).\nПример: 1500\n\nДалее напишите комментарий.",
            reply_markup=CANCEL_KB
        )
    else:
        await cb.message.answer(
            "Введите сумму возмещения (целое число).\nПример: 350\n\nДалее напишите комментарий.\n\n⚠️ Чек обязателен.",
            reply_markup=CANCEL_KB
        )
    await cb.answer()


@dp.message(PRFlow.waiting_amount)
async def pr_amount(message: types.Message, state: FSMContext):
    txt = (message.text or "").strip()
    if txt.lower() == "отмена":
        await state.set_state(FillFlow.calendar)
        await message.answer("Ок, отменил.", reply_markup=ReplyKeyboardRemove())
        await render_calendar(message, state)
        return

    if not re.fullmatch(r"-?\d{1,7}", txt):
        await message.answer("Нужно целое число. Пример: 350 или -200", reply_markup=CANCEL_KB)
        return

    await state.update_data(pr_amount=int(txt))
    await state.set_state(PRFlow.waiting_text)
    await message.answer("Теперь напиши комментарий (обязательно).", reply_markup=CANCEL_KB)


@dp.message(PRFlow.waiting_text)
async def pr_text(message: types.Message, state: FSMContext):
    txt = (message.text or "").strip()
    if txt.lower() == "отмена":
        await state.set_state(FillFlow.calendar)
        await message.answer("Ок, отменил.", reply_markup=ReplyKeyboardRemove())
        await render_calendar(message, state)
        return

    if len(txt) < 3:
        await message.answer("Комментарий слишком короткий. Напиши подробнее.", reply_markup=CANCEL_KB)
        return

    data = await state.get_data()
    kind = data.get("pr_kind", "NOTE")
    amount = int(data.get("pr_amount", 0))

    merch = get_merch_by_tg_id(message.from_user.id)
    if not merch:
        await state.clear()
        await message.answer("Сначала /start", reply_markup=ReplyKeyboardRemove())
        return

    point = data.get("point_code")
    y = int(data.get("cal_y"))
    m = int(data.get("cal_m"))
    mk = month_start(y, m)

    with engine.begin() as conn:
        rid = conn.execute(text("""
            INSERT INTO reimbursements (merchant_id, point_code, month_key, amount, note, kind)
            VALUES (:mid, :p, :mk, :a, :n, :k)
            RETURNING id
        """), {"mid": merch["id"], "p": point, "mk": mk, "a": amount, "n": txt, "k": kind}).scalar()

    await maybe_notify_post_submit_change(merch["id"], y, m, f"добавил {('возмещение' if kind=='REIMB' else 'примечание')} {amount} ₽ на {point} {y}-{m:02d}")

    if kind == "NOTE":
        await state.set_state(FillFlow.calendar)
        await message.answer("✅ Примечание добавлено.", reply_markup=ReplyKeyboardRemove())
        await render_calendar(message, state)
        return

    # Возмещение: чек обязателен
    await state.set_state(PRFlow.waiting_receipt)
    await state.update_data(pr_reimb_id=int(rid))
    await message.answer(
        "✅ Возмещение сохранено.\n\n"
        "⚠️ Теперь обязательно загрузите фото/файл чека.\n"
        "Без чека возмещение не принимается.",
        reply_markup=CANCEL_KB
    )


async def _save_receipt_and_notify_tu(message: types.Message, state: FSMContext, file_id: str):
    data = await state.get_data()
    rid = data.get("pr_reimb_id")
    if not rid:
        await message.answer("Не нашёл заявку. Попробуйте снова через кнопку.")
        return

    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE reimbursements
            SET receipt_file_id=:fid, receipt_uploaded_at=NOW()
            WHERE id=:id AND kind='REIMB'
        """), {"fid": file_id, "id": int(rid)})

    merch = get_merch_by_tg_id(message.from_user.id)
    point = data.get("point_code")
    y = int(data.get("cal_y"))
    m = int(data.get("cal_m"))

    # Отправляем чек ТУ в личку
    tu_admin = get_tu_admin_id(merch.get("tu") if merch else "")
    if tu_admin:
        try:
            await bot.send_photo(
                tu_admin,
                photo=file_id,
                caption=(
                    "📎 Чек загружен (возмещение)\n"
                    f"Мерч: {merch['fio'] if merch else '-'}\n"
                    f"ТУ: {merch.get('tu') or '-'}\n"
                    f"Точка: {point}\n"
                    f"Месяц: {y}-{m:02d}"
                )
            )
        except Exception:
            # если это не фото (документ), отправим как документ
            try:
                await bot.send_document(
                    tu_admin,
                    document=file_id,
                    caption=(
                        "📎 Чек загружен (возмещение)\n"
                        f"Мерч: {merch['fio'] if merch else '-'}\n"
                        f"ТУ: {merch.get('tu') or '-'}\n"
                        f"Точка: {point}\n"
                        f"Месяц: {y}-{m:02d}"
                    )
                )
            except Exception:
                pass


@dp.message(PRFlow.waiting_receipt, F.photo)
async def pr_receipt_photo(message: types.Message, state: FSMContext):
    file_id = message.photo[-1].file_id
    await _save_receipt_and_notify_tu(message, state, file_id)
    await state.set_state(FillFlow.calendar)
    await message.answer("✅ Чек загружен. Возмещение принято.", reply_markup=ReplyKeyboardRemove())
    await render_calendar(message, state)


@dp.message(PRFlow.waiting_receipt, F.document)
async def pr_receipt_document(message: types.Message, state: FSMContext):
    file_id = message.document.file_id
    await _save_receipt_and_notify_tu(message, state, file_id)
    await state.set_state(FillFlow.calendar)
    await message.answer("✅ Чек загружен. Возмещение принято.", reply_markup=ReplyKeyboardRemove())
    await render_calendar(message, state)


# ================== REPORT (xlsx) ==================
def build_report_xlsx(y: int, m: int, tu: str | None) -> bytes:
    """Собирает отчёт .xlsx за месяц.

    В отчёте строки формируются по мерчендайзеру и точке (если за месяц по точке есть:
    выходы / примечания / возмещения).
    """
    tu = (tu or "").strip().lower()
    params: dict = {}
    tu_filter_sql = ""
    if tu:
        tu_filter_sql = "WHERE m.tu = :tu"
        params["tu"] = tu

    with engine.connect() as conn:
        merchants = conn.execute(text(f"""
            SELECT m.id, m.fio, m.tu
            FROM merchants m
            {tu_filter_sql}
            ORDER BY m.fio
        """), params).mappings().all()

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Report"

    headers = [
        "ФИО мерчендайзера",
        "ТУ",
        "Номер точки",
        "Выходы с поставкой",
        "Выходы без поставок",
        "Выходы всего (день)",
        "Полный инвент",
        "Кофемашина (Да/Нет)",
        "Кофемашина начислено, ₽",
        "Примечания сумма, ₽",
        "Примечания комментарии",
        "Возмещения сумма, ₽",
        "Возмещения комментарии",
        "Есть возмещения без чека (Да/Нет)",
        "Сумма по точке, ₽",
    ]
    ws.append(headers)

    for mer in merchants:
        mid = int(mer["id"])
        fio = mer.get("fio") or ""
        tu_name = mer.get("tu") or ""

        points = get_points_for_month(mid, y, m)
        for p in points:
            (
                point_total,
                cnt_supply,
                cnt_nos,
                cnt_day_total,
                cnt_full_inv,
                notes_sum,
                reimb_sum,
                coffee_on,
                coffee_sum,
                _missing_receipts_bool,
            ) = compute_point_total(mid, p, y, m)

            note_comments, reimb_comments, missing_receipt_flag = get_reimb_comments(mid, p, y, m)

            ws.append([
                fio,
                tu_name,
                p,
                cnt_supply,
                cnt_nos,
                cnt_day_total,
                cnt_full_inv,
                "Да" if coffee_on else "Нет",
                coffee_sum,
                notes_sum,
                note_comments,
                reimb_sum,
                reimb_comments,
                missing_receipt_flag,
                point_total,
            ])

    # Авто-ширина колонок (с ограничением)
    for col in ws.columns:
        try:
            col_letter = col[0].column_letter
        except Exception:
            continue
        max_len = 0
        for cell in col:
            v = "" if cell.value is None else str(cell.value)
            if len(v) > max_len:
                max_len = len(v)
        ws.column_dimensions[col_letter].width = min(55, max(12, max_len + 2))

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()

@dp.message(Command("report"))
async def report_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Использование:\n/report YYYY-MM\n/report <ту> YYYY-MM\nПример: /report хрупов 2026-01")
        return

    tu = None
    ym_str = None
    if len(parts) == 2:
        ym_str = parts[1]
    else:
        tu = parts[1].strip().lower()
        ym_str = parts[2]

    ym = parse_month_arg(ym_str)
    if not ym:
        await message.answer("Неверный формат месяца. Нужно YYYY-MM, например 2026-01")
        return
    y, m = ym

    xlsx_bytes = build_report_xlsx(y, m, tu)
    fname = f"report_{tu + '_' if tu else ''}{y}-{m:02d}.xlsx"
    await message.answer_document(
        BufferedInputFile(xlsx_bytes, filename=fname),
        caption=f"✅ Отчёт за {y}-{m:02d}" + (f" (ТУ: {tu})" if tu else "")
    )


# ================== Startup / Webhook / Polling ==================
async def on_startup(bot: Bot):
    ensure_tables()
    if USE_WEBHOOK:
        url = WEBHOOK_BASE_URL.rstrip("/") + WEBHOOK_PATH
        await bot.set_webhook(url, secret_token=WEBHOOK_SECRET or None)


async def on_shutdown(bot: Bot):
    if USE_WEBHOOK:
        await bot.delete_webhook(drop_pending_updates=False)


def build_app() -> web.Application:
    app = web.Application()
    # Healthcheck endpoint (Render иногда ждёт 200 на /)
    app.router.add_get('/', lambda request: web.Response(text='OK'))
    SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=(WEBHOOK_SECRET or None)).register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    return app


async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    if USE_WEBHOOK:
        app = build_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
        await site.start()
        while True:
            await asyncio.sleep(3600)
    else:
        await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
