import os
import asyncio
import hashlib
import re
from io import BytesIO
from datetime import datetime, date, timedelta, timezone

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


# ----------------- ENV -----------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
PORT = int(os.getenv("PORT", "10000"))

ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "")
SECRET_SALT = os.getenv("SECRET_SALT", "CHANGE_ME_SALT")

WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()  # https://xxx.onrender.com
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook").strip()  # /webhook
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()      # any random
USE_WEBHOOK = bool(WEBHOOK_BASE_URL)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


# ----------------- UI -----------------
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


# ----------------- Helpers -----------------
def parse_admin_ids(raw: str) -> set[int]:
    ids = set()
    for part in (raw or "").split(","):
        part = part.strip()
        if part.isdigit():
            ids.add(int(part))
    return ids


ADMIN_IDS = parse_admin_ids(ADMIN_IDS_RAW)


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


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
    if len(digits) < 4:
        return ""
    return digits[-4:]


def hash_last4(last4: str) -> str:
    s = (last4.strip() + SECRET_SALT).encode("utf-8")
    return hashlib.sha256(s).hexdigest()


def normalize_point_code(v) -> str:
    s = str(v or "").strip()
    s = re.sub(r"\s+", "", s)
    return s


def month_start(year: int, month: int) -> date:
    return date(year, month, 1)


def month_end_exclusive(year: int, month: int) -> date:
    if month == 12:
        return date(year + 1, 1, 1)
    return date(year, month + 1, 1)


def days_in_month(y: int, m: int) -> int:
    return (month_end_exclusive(y, m) - timedelta(days=1)).day


def weekday_of(y: int, m: int, d: int) -> int:
    return date(y, m, d).weekday()  # Mon=0


def month_title(y: int, m: int) -> str:
    names = ["Январь","Февраль","Март","Апрель","Май","Июнь","Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"]
    return f"{names[m-1]} {y}"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ----------------- DB schema -----------------
def ensure_tables():
    with engine.begin() as conn:
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
            slot TEXT NOT NULL, -- DAY / FRI_EVENING / SAT_MORNING
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
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS reimb_idx ON reimbursements(merchant_id, point_code, month_key);"))

        # ☕ кофемашина (на точку и месяц)
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS coffee_bonus (
            id SERIAL PRIMARY KEY,
            merchant_id INTEGER NOT NULL REFERENCES merchants(id) ON DELETE CASCADE,
            point_code TEXT NOT NULL,
            month_key DATE NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT FALSE,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(merchant_id, point_code, month_key)
        );
        """))

        # отправка сверки (на месяц)
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

        # best-effort заполнение fio_norm
        conn.execute(text("""
        UPDATE merchants
        SET fio_norm = lower(replace(replace(fio, 'Ё', 'Е'), 'ё', 'е'))
        WHERE fio_norm IS NULL OR fio_norm = '';
        """))


# ----------------- DB queries -----------------
def get_merch_by_tg_id(tg_id: int):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, fio, tu FROM merchants WHERE telegram_id = :tg_id"),
            {"tg_id": tg_id},
        ).mappings().first()
    return row


def get_merch_by_fio(fio: str):
    fn = fio_norm(fio)
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, fio, pass_hash, telegram_id, tu FROM merchants WHERE fio_norm = :fio_norm"),
            {"fio_norm": fn},
        ).mappings().first()
    return row


def bind_merch_tg_id(merch_id: int, tg_id: int):
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE merchants SET telegram_id = :tg_id WHERE id = :id"),
            {"tg_id": tg_id, "id": merch_id},
        )


def upsert_merchant(conn, fio_raw: str, phone_raw: str, tu: str) -> tuple[bool, bool]:
    fio_disp = fio_display(fio_raw or "")
    fio_n = fio_norm(fio_raw or "")
    last4 = extract_last4_from_phone(phone_raw or "")
    tu = (tu or "").strip().lower()

    if not fio_n or len(fio_n.split(" ")) < 2 or not re.fullmatch(r"\d{4}", last4):
        return (False, False)

    ph = hash_last4(last4)
    res = conn.execute(text("""
        INSERT INTO merchants (fio, fio_norm, pass_hash, tu)
        VALUES (:fio, :fio_norm, :pass_hash, :tu)
        ON CONFLICT (fio_norm) DO UPDATE
            SET fio = EXCLUDED.fio,
                pass_hash = EXCLUDED.pass_hash,
                tu = EXCLUDED.tu
        RETURNING xmax;
    """), {"fio": fio_disp, "fio_norm": fio_n, "pass_hash": ph, "tu": tu})

    xmax = res.scalar()
    if xmax == 0:
        return (True, False)
    return (False, True)


def get_supply_map(point_code: str, y: int, m: int) -> dict[int, bool]:
    start = month_start(y, m)
    end = month_end_exclusive(y, m)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT supply_date, has_supply FROM supplies
            WHERE point_code = :p AND supply_date >= :start AND supply_date < :end
        """), {"p": point_code, "start": start, "end": end}).mappings().all()
    out: dict[int, bool] = {}
    for r in rows:
        d: date = r["supply_date"]
        out[d.day] = bool(r["has_supply"])
    return out


def get_visits_for_month(merchant_id: int, point_code: str, y: int, m: int) -> dict[int, set[str]]:
    start = month_start(y, m)
    end = month_end_exclusive(y, m)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT visit_date, slot FROM visits
            WHERE merchant_id = :mid AND point_code = :p
              AND visit_date >= :start AND visit_date < :end
        """), {"mid": merchant_id, "p": point_code, "start": start, "end": end}).mappings().all()
    out: dict[int, set[str]] = {}
    for r in rows:
        d: date = r["visit_date"]
        out.setdefault(d.day, set()).add(str(r["slot"]))
    return out


def get_reimb_sum(merchant_id: int, point_code: str, y: int, m: int) -> int:
    mk = month_start(y, m)
    with engine.connect() as conn:
        s = conn.execute(text("""
            SELECT COALESCE(SUM(amount),0) FROM reimbursements
            WHERE merchant_id=:mid AND point_code=:p AND month_key=:mk
        """), {"mid": merchant_id, "p": point_code, "mk": mk}).scalar()
    return int(s or 0)


def coffee_enabled(merchant_id: int, point_code: str, y: int, m: int) -> bool:
    mk = month_start(y, m)
    with engine.connect() as conn:
        v = conn.execute(text("""
            SELECT enabled FROM coffee_bonus
            WHERE merchant_id=:mid AND point_code=:p AND month_key=:mk
        """), {"mid": merchant_id, "p": point_code, "mk": mk}).scalar()
    return bool(v) if v is not None else False


def set_coffee_enabled(merchant_id: int, point_code: str, y: int, m: int, enabled: bool):
    mk = month_start(y, m)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO coffee_bonus (merchant_id, point_code, month_key, enabled, updated_at)
            VALUES (:mid, :p, :mk, :e, NOW())
            ON CONFLICT (merchant_id, point_code, month_key) DO UPDATE
              SET enabled = EXCLUDED.enabled,
                  updated_at = NOW()
        """), {"mid": merchant_id, "p": point_code, "mk": mk, "e": enabled})


def get_submission_status(merchant_id: int, y: int, m: int):
    mk = month_start(y, m)
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT submitted_at, updated_after_submit_at
            FROM submissions
            WHERE merchant_id=:mid AND month_key=:mk
        """), {"mid": merchant_id, "mk": mk}).mappings().first()
    return row  # None or dict


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
            SET updated_after_submit_at = NOW()
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
                WHERE merchant_id=:mid AND visit_date >= :start AND visit_date < :end
                UNION
                SELECT point_code FROM reimbursements
                WHERE merchant_id=:mid AND month_key=:mk
                UNION
                SELECT point_code FROM coffee_bonus
                WHERE merchant_id=:mid AND month_key=:mk
            ) t
            ORDER BY point_code
        """), {"mid": merchant_id, "start": start, "end": end, "mk": mk}).all()
    return [r[0] for r in rows if r and r[0]]


def compute_point_total(merchant_id: int, point_code: str, y: int, m: int) -> int:
    supply = get_supply_map(point_code, y, m)
    visits = get_visits_for_month(merchant_id, point_code, y, m)
    total = 0
    day_count = 0  # только DAY для кофемашины

    for day, slots in visits.items():
        for slot in slots:
            if slot == "FRI_EVENING":
                total += 400
            elif slot == "SAT_MORNING":
                total += 400
            else:
                day_count += 1
                total += 800 if supply.get(day, False) else 400

    if coffee_enabled(merchant_id, point_code, y, m):
        total += 100 * day_count

    total += get_reimb_sum(merchant_id, point_code, y, m)
    return total


def compute_overall_total(merchant_id: int, y: int, m: int) -> tuple[int, dict[str, int]]:
    points = get_points_for_month(merchant_id, y, m)
    per_point: dict[str, int] = {}
    total = 0
    for p in points:
        s = compute_point_total(merchant_id, p, y, m)
        per_point[p] = s
        total += s
    return total, per_point


# ----------------- Supplies parsing (header dates) -----------------
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

    # 20.янв / 20 янв
    m = re.match(r"^(\d{1,2})[.\s](\D+)$", s)
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

    # 20.01 or 20.01.2026
    m2 = re.match(r"^(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?$", s)
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


# ----------------- States -----------------
class UploadMerchants(StatesGroup):
    waiting_file = State()


class UploadSupplies(StatesGroup):
    waiting_file = State()


class LoginFlow(StatesGroup):
    waiting_fio = State()
    waiting_last4 = State()


class FillFlow(StatesGroup):
    waiting_point = State()
    calendar = State()


class NoteFlow(StatesGroup):
    waiting_amount = State()
    waiting_text = State()


# ----------------- Notifications -----------------
async def notify_admins(text_msg: str):
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text_msg)
        except Exception:
            pass


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


# ----------------- Cancel / Restart -----------------
@dp.message(F.text.in_({"Отмена", "Заново"}))
async def cancel_or_restart(message: types.Message, state: FSMContext):
    await state.clear()
    if message.text == "Отмена":
        await message.answer("Ок, отменил. Напиши /start чтобы начать заново.", reply_markup=ReplyKeyboardRemove())
    else:
        await message.answer("Начнём заново. Напиши /start", reply_markup=ReplyKeyboardRemove())


# ----------------- Basic commands -----------------
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
        await message.answer(f"❌ Ошибка БД: {type(e).__name__}")


@dp.message(Command("merchants_count"))
async def merchants_count(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return
    with engine.connect() as conn:
        cnt = conn.execute(text("SELECT COUNT(*) FROM merchants;")).scalar()
    await message.answer(f"Сейчас мерчендайзеров в базе: {cnt}")


# ----------------- Login flow -----------------
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
    txt = (message.text or "").strip()
    prefilled_last4 = None
    if "," in txt:
        p1, p2 = [p.strip() for p in txt.split(",", 1)]
        if re.fullmatch(r"\d{4}", p2):
            txt = p1
            prefilled_last4 = p2

    fio_in = fio_display(txt)
    merch = get_merch_by_fio(fio_in)
    if not merch:
        await message.answer(
            "❌ Не получилось найти ФИО.\n"
            "Проверь написание или обратись к территориальному управляющему.",
            reply_markup=LOGIN_KB
        )
        return

    await state.update_data(fio=fio_in)
    await state.set_state(LoginFlow.waiting_last4)

    if prefilled_last4:
        ok, msg = await verify_login_last4(message.from_user.id, fio_in, prefilled_last4)
        if ok:
            await state.clear()
            await message.answer(msg, reply_markup=MAIN_KB)
        else:
            await message.answer(msg, reply_markup=LOGIN_KB)
        return

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


# ----------------- Admin: upload merchants (.xlsx) -----------------
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
        f"Ок. Пришли Excel .xlsx с 2 столбцами:\n"
        f"A: ФИО\n"
        f"B: Телефон\n\n"
        f"ТУ будет записан как: {tu}\n"
        f"Телефон может быть в любом формате — бот сам возьмёт последние 4 цифры.",
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
            await message.answer("❌ Не указан ТУ. Запусти команду заново: /upload_merchants <ту>", reply_markup=ReplyKeyboardRemove())
            return

        filename = (doc.file_name or "").lower()
        if not filename.endswith(".xlsx"):
            await message.answer("❌ Нужен файл .xlsx", reply_markup=ReplyKeyboardRemove())
            await state.clear()
            return

        f = await bot.get_file(doc.file_id)
        buf = BytesIO()
        await bot.download_file(f.file_path, destination=buf)
        raw = buf.getvalue()

        wb = openpyxl.load_workbook(BytesIO(raw), read_only=True, data_only=True)
        ws = wb.worksheets[0]

        added = updated = bad_rows = 0
        with engine.begin() as conn:
            for r in ws.iter_rows(min_row=1, values_only=True):
                if not r or len(r) < 2:
                    continue
                a = "" if r[0] is None else str(r[0])
                b = "" if r[1] is None else str(r[1])
                ins, upd = upsert_merchant(conn, a, b, tu)
                if ins:
                    added += 1
                elif upd:
                    updated += 1
                else:
                    bad_rows += 1

        await state.clear()
        await message.answer(
            f"✅ Готово ({tu}).\nДобавлено: {added}\nОбновлено: {updated}\nПропущено (ошибочные строки): {bad_rows}",
            reply_markup=ReplyKeyboardRemove()
        )

    except Exception as e:
        await state.clear()
        await message.answer(f"❌ Ошибка обработки файла: {type(e).__name__}: {e}", reply_markup=ReplyKeyboardRemove())


# ----------------- Admin: upload supplies (.xlsx) -----------------
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
        "Ок. Пришли Excel .xlsx с поставками (как в твоём формате):\n"
        "- строки: точки\n"
        "- в шапке: даты\n"
        "- в ячейках: коробки\n\n"
        f"Год для дат без года: {y}\n"
        "Если нужен другой год: /upload_supplies 2027",
        reply_markup=CANCEL_KB
    )


@dp.message(UploadSupplies.waiting_file, F.document)
async def handle_supplies_file(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return

    doc = message.document
    try:
        filename = (doc.file_name or "").lower()
        if not filename.endswith(".xlsx"):
            await message.answer("❌ Нужен файл .xlsx", reply_markup=ReplyKeyboardRemove())
            await state.clear()
            return

        data = await state.get_data()
        default_year = int(data.get("supplies_year", datetime.utcnow().year))

        f = await bot.get_file(doc.file_id)
        buf = BytesIO()
        await bot.download_file(f.file_path, destination=buf)
        raw = buf.getvalue()

        wb = openpyxl.load_workbook(BytesIO(raw), read_only=True, data_only=True)
        ws = wb.worksheets[0]

        header = None
        for r in ws.iter_rows(min_row=1, max_row=1, values_only=True):
            header = list(r)
        if not header or len(header) < 3:
            raise ValueError("Не смог прочитать шапку: ожидаю TT + (игнор) + даты")

        date_cols: dict[int, date] = {}
        for idx in range(2, len(header)):
            d = parse_header_date(header[idx], default_year)
            if d:
                date_cols[idx] = d

        if not date_cols:
            raise ValueError("Не нашёл даты в шапке. Проверь: после TT должны быть даты (например 20.янв).")

        inserted = updated = skipped = 0

        with engine.begin() as conn:
            for r in ws.iter_rows(min_row=2, values_only=True):
                if not r or len(r) < 1:
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

                    has_supply = boxes >= 5

                    res = conn.execute(text("""
                        INSERT INTO supplies (point_code, supply_date, boxes, has_supply)
                        VALUES (:p, :d, :b, :hs)
                        ON CONFLICT (point_code, supply_date) DO UPDATE
                            SET boxes = EXCLUDED.boxes,
                                has_supply = EXCLUDED.has_supply
                        RETURNING xmax;
                    """), {"p": point, "d": d, "b": boxes, "hs": has_supply})

                    xmax = res.scalar()
                    if xmax == 0:
                        inserted += 1
                    else:
                        updated += 1

        await state.clear()
        await message.answer(
            f"✅ Поставки загружены.\nДобавлено: {inserted}\nОбновлено: {updated}\nПропущено (плохие ячейки): {skipped}",
            reply_markup=ReplyKeyboardRemove()
        )

    except Exception as e:
        await state.clear()
        await message.answer(f"❌ Ошибка загрузки поставок: {type(e).__name__}: {e}", reply_markup=ReplyKeyboardRemove())


# ----------------- Calendar UI -----------------
def build_calendar_kb(
    y: int, m: int,
    supply: dict[int, bool],
    visits: dict[int, set[str]],
    coffee_on: bool,
    submitted: bool
) -> InlineKeyboardMarkup:
    dim = days_in_month(y, m)
    first_wd = date(y, m, 1).weekday()
    rows: list[list[InlineKeyboardButton]] = []

    wd = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    rows.append([InlineKeyboardButton(text=x, callback_data="noop") for x in wd])

    day = 1
    row: list[InlineKeyboardButton] = []
    for _ in range(first_wd):
        row.append(InlineKeyboardButton(text=" ", callback_data="noop"))

    while day <= dim:
        has = supply.get(day, False)
        v = visits.get(day, set())

        marker_supply = "🟩" if has else "⬜"
        marker_visit = ""
        if "DAY" in v:
            marker_visit += "✅"
        if "FRI_EVENING" in v:
            marker_visit += "🌙"
        if "SAT_MORNING" in v:
            marker_visit += "🌅"

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
        InlineKeyboardButton(text=("☕ Кофемашина: ВКЛ" if coffee_on else "☕ Кофемашина: ВЫКЛ"), callback_data="coffee:toggle"),
        InlineKeyboardButton(text="➕ Примечание", callback_data="note:add"),
    ])
    rows.append([
        InlineKeyboardButton(text=("📤 Сверка: отправлена" if submitted else "📤 Отправить сверку"), callback_data=("submit:noop" if submitted else "submit:send")),
        InlineKeyboardButton(text="✅ Готово", callback_data="done"),
    ])
    rows.append([
        InlineKeyboardButton(text="◀️ Месяц", callback_data="nav:prev"),
        InlineKeyboardButton(text="Месяц ▶️", callback_data="nav:next"),
    ])
    rows.append([InlineKeyboardButton(text="📍 Сменить точку", callback_data="back_point")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_friday_slot_kb(day: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Пт: Дневной", callback_data=f"slot:DAY:{day}")],
        [InlineKeyboardButton(text="Пт: Вечерний (400)", callback_data=f"slot:FRI_EVENING:{day}")],
        [InlineKeyboardButton(text="↩️ Назад к календарю", callback_data="slot_cancel")],
    ])


def build_saturday_slot_kb(day: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Сб: Утренний (400)", callback_data=f"slot:SAT_MORNING:{day}")],
        [InlineKeyboardButton(text="Сб: Дневной (400/800)", callback_data=f"slot:DAY:{day}")],
        [InlineKeyboardButton(text="↩️ Назад к календарю", callback_data="slot_cancel")],
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

    supply = get_supply_map(point, y, m)
    visits = get_visits_for_month(merch["id"], point, y, m)
    reimb_sum = get_reimb_sum(merch["id"], point, y, m)
    coffee_on = coffee_enabled(merch["id"], point, y, m)

    point_total = compute_point_total(merch["id"], point, y, m)
    overall_total, per_point = compute_overall_total(merch["id"], y, m)

    sub = get_submission_status(merch["id"], y, m)
    submitted = bool(sub)
    submitted_line = ""
    if sub:
        sa = sub.get("submitted_at")
        ua = sub.get("updated_after_submit_at")
        if sa:
            submitted_line = f"📤 Сверка отправлена: {str(sa)[:16].replace('T',' ')}"
            if ua:
                submitted_line += f"\n⚠️ Были изменения после отправки: {str(ua)[:16].replace('T',' ')}"

    # мини-сводка по точкам (чтобы не завалить текст)
    per_point_lines = []
    for p, s in per_point.items():
        mark = "👉" if p == point else "•"
        per_point_lines.append(f"{mark} {p}: {s} ₽")
    per_point_text = "\n".join(per_point_lines) if per_point_lines else "• (нет данных)"

    text_msg = (
        f"📍 Точка: {point}\n"
        f"🗓 {month_title(y, m)}\n\n"
        f"Легенда:\n"
        f"🟩 есть поставка (≥5) | ⬜ нет поставки\n"
        f"✅ дневной выход | 🌙 пятница вечер | 🌅 суббота утро\n\n"
        f"☕ Кофемашина: {'ВКЛ (+100 ₽ за каждый дневной выход)' if coffee_on else 'ВЫКЛ'}\n"
        f"🧾 Примечания/возмещения по этой точке: {reimb_sum} ₽\n\n"
        f"💰 Сумма по этой точке: {point_total} ₽\n"
        f"📊 Общая сумма за месяц (все точки): {overall_total} ₽\n\n"
        f"Суммы по точкам:\n{per_point_text}"
    )
    if submitted_line:
        text_msg += f"\n\n{submitted_line}"

    kb = build_calendar_kb(y, m, supply, visits, coffee_on, submitted)

    if isinstance(message_or_cb, types.CallbackQuery):
        await message_or_cb.message.edit_text(text_msg, reply_markup=kb)
        await message_or_cb.answer()
    else:
        await message_or_cb.answer(text_msg, reply_markup=kb)


# ----------------- Visits / collisions -----------------
def add_or_remove_visit(merchant_id: int, point: str, y: int, m: int, day: int, slot: str) -> tuple[bool, bool]:
    d = date(y, m, day)
    with engine.begin() as conn:
        existing = conn.execute(text("""
            SELECT id FROM visits
            WHERE merchant_id=:mid AND point_code=:p AND visit_date=:d AND slot=:s
        """), {"mid": merchant_id, "p": point, "d": d, "s": slot}).scalar()

        if existing:
            conn.execute(text("DELETE FROM visits WHERE id=:id"), {"id": existing})
            return (True, False)

        conn.execute(text("""
            INSERT INTO visits (merchant_id, point_code, visit_date, slot)
            VALUES (:mid, :p, :d, :s)
            ON CONFLICT DO NOTHING
        """), {"mid": merchant_id, "p": point, "d": d, "s": slot})
        return (False, True)


def find_collisions(point: str, y: int, m: int, day: int, merchant_id: int) -> list[dict]:
    d = date(y, m, day)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT v.merchant_id, m.fio, m.telegram_id
            FROM visits v
            JOIN merchants m ON m.id = v.merchant_id
            WHERE v.point_code = :p AND v.visit_date = :d AND v.merchant_id <> :mid
        """), {"p": point, "d": d, "mid": merchant_id}).mappings().all()
    return [dict(r) for r in rows]


async def notify_collision(point: str, y: int, m: int, day: int, current_fio: str, others: list[dict]):
    d = date(y, m, day)
    other_names = ", ".join([o["fio"] for o in others]) if others else "?"
    msg_admin = (
        f"⚠️ Пересечение!\n"
        f"Точка: {point}\n"
        f"Дата: {d.isoformat()}\n"
        f"Новый: {current_fio}\n"
        f"Уже отмечены: {other_names}"
    )
    await notify_admins(msg_admin)

    for o in others:
        tg = o.get("telegram_id")
        if tg:
            try:
                await bot.send_message(int(tg), f"⚠️ Пересечение по точке {point} на {d.isoformat()}. Проверьте с руководителем.")
            except Exception:
                pass


# ----------------- Merch flow -----------------
@dp.message(F.text == "Заполнить сверку")
async def fill_reconcile_start(message: types.Message, state: FSMContext):
    merch = get_merch_by_tg_id(message.from_user.id)
    if not merch:
        await message.answer("Сначала нужно войти: /start", reply_markup=ReplyKeyboardRemove())
        return

    await state.set_state(FillFlow.waiting_point)
    await message.answer(
        "Введите номер точки (4–5 цифр).\nПример: 2674\n\nЕсли хотите отменить — нажмите «Отмена».",
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

    await state.set_state(FillFlow.calendar)
    await state.update_data(point_code=point, cal_y=y, cal_m=m)
    await render_calendar(message, state)


@dp.callback_query(F.data == "noop")
async def noop(cb: types.CallbackQuery):
    await cb.answer()


@dp.callback_query(F.data == "done")
async def cal_done(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text("✅ Готово. Возвращаю в меню.", reply_markup=None)
    await cb.message.answer("Главное меню:", reply_markup=MAIN_KB)
    await cb.answer()


@dp.callback_query(F.data == "back_point")
async def cal_back_point(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(FillFlow.waiting_point)
    await cb.message.edit_text("Введите номер точки (4–5 цифр). Пример: 2674", reply_markup=None)
    await cb.message.answer("Введите номер точки (4–5 цифр). Пример: 2674", reply_markup=CANCEL_KB)
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
        if m == 1:
            y -= 1
            m = 12
        else:
            m -= 1
    else:
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1

    await state.update_data(cal_y=y, cal_m=m)
    await render_calendar(cb, state)


@dp.callback_query(F.data.startswith("cal:"))
async def cal_day_click(cb: types.CallbackQuery, state: FSMContext):
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
    if wd == 4:  # Friday
        await cb.message.edit_text(
            f"Вы выбрали пятницу {day:02d}.{m:02d}. Выберите тип выхода:",
            reply_markup=build_friday_slot_kb(day)
        )
        await cb.answer()
        return

    if wd == 5:  # Saturday
        await cb.message.edit_text(
            f"Вы выбрали субботу {day:02d}.{m:02d}. Выберите тип выхода:",
            reply_markup=build_saturday_slot_kb(day)
        )
        await cb.answer()
        return

    merch = get_merch_by_tg_id(cb.from_user.id)
    if not merch:
        await cb.answer("Сначала /start")
        return

    existed, added = add_or_remove_visit(merch["id"], point, y, m, day, "DAY")
    action = f"{'удалил' if existed else 'добавил'} выход DAY {point} {y}-{m:02d}-{day:02d}"
    await maybe_notify_post_submit_change(merch["id"], y, m, action)

    if added:
        others = find_collisions(point, y, m, day, merch["id"])
        if others:
            await cb.message.answer("⚠️ Внимание: есть пересечение с другим мерчендайзером. Нужна проверка.")
            await notify_collision(point, y, m, day, merch["fio"], others)

    await render_calendar(cb, state)


@dp.callback_query(F.data.startswith("slot:"))
async def cal_slot_pick(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if "point_code" not in data:
        await cb.answer()
        return

    y = int(data["cal_y"])
    m = int(data["cal_m"])
    point = data["point_code"]

    _, slot, day_s = cb.data.split(":")
    day = int(day_s)

    merch = get_merch_by_tg_id(cb.from_user.id)
    if not merch:
        await cb.answer("Сначала /start")
        return

    existed, added = add_or_remove_visit(merch["id"], point, y, m, day, slot)
    action = f"{'удалил' if existed else 'добавил'} выход {slot} {point} {y}-{m:02d}-{day:02d}"
    await maybe_notify_post_submit_change(merch["id"], y, m, action)

    if added:
        others = find_collisions(point, y, m, day, merch["id"])
        if others:
            await cb.message.answer("⚠️ Внимание: есть пересечение с другим мерчендайзером. Нужна проверка.")
            await notify_collision(point, y, m, day, merch["fio"], others)

    await render_calendar(cb, state)


@dp.callback_query(F.data == "slot_cancel")
async def slot_cancel(cb: types.CallbackQuery, state: FSMContext):
    await render_calendar(cb, state)


# ----------------- Coffee toggle -----------------
@dp.callback_query(F.data == "coffee:toggle")
async def coffee_toggle(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if "point_code" not in data:
        await cb.answer()
        return

    merch = get_merch_by_tg_id(cb.from_user.id)
    if not merch:
        await cb.answer("Сначала /start")
        return

    y = int(data["cal_y"])
    m = int(data["cal_m"])
    point = data["point_code"]

    current = coffee_enabled(merch["id"], point, y, m)
    new_val = not current
    set_coffee_enabled(merch["id"], point, y, m, new_val)

    action = f"переключил кофемашину ({'ВКЛ' if new_val else 'ВЫКЛ'}) на {point} {y}-{m:02d}"
    await maybe_notify_post_submit_change(merch["id"], y, m, action)

    await cb.answer("☕ Ок")
    await render_calendar(cb, state)


# ----------------- Submit reconciliation -----------------
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


# ----------------- Notes / reimbursements -----------------
@dp.callback_query(F.data == "note:add")
async def note_add(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(NoteFlow.waiting_amount)
    await cb.message.answer(
        "Введите сумму примечания/возмещения (целое число).\n"
        "Пример: 350\n"
        "Если удержание — можно отрицательное: -200",
        reply_markup=CANCEL_KB
    )
    await cb.answer()


@dp.message(NoteFlow.waiting_amount)
async def note_amount(message: types.Message, state: FSMContext):
    txt = (message.text or "").strip()
    if txt.lower() == "отмена":
        await state.set_state(FillFlow.calendar)
        await message.answer("Ок, отменил добавление примечания.", reply_markup=ReplyKeyboardRemove())
        await render_calendar(message, state)
        return

    if not re.fullmatch(r"-?\d{1,7}", txt):
        await message.answer("Нужно целое число. Пример: 350 или -200", reply_markup=CANCEL_KB)
        return

    await state.update_data(note_amount=int(txt))
    await state.set_state(NoteFlow.waiting_text)
    await message.answer("Теперь напишите комментарий (например: 'такси, чек у ТУ').", reply_markup=CANCEL_KB)


@dp.message(NoteFlow.waiting_text)
async def note_text(message: types.Message, state: FSMContext):
    txt = (message.text or "").strip()
    if txt.lower() == "отмена":
        await state.set_state(FillFlow.calendar)
        await message.answer("Ок, отменил добавление примечания.", reply_markup=ReplyKeyboardRemove())
        await render_calendar(message, state)
        return

    data = await state.get_data()
    merch = get_merch_by_tg_id(message.from_user.id)
    if not merch:
        await state.clear()
        await message.answer("Сначала /start", reply_markup=ReplyKeyboardRemove())
        return

    point = data["point_code"]
    y = int(data["cal_y"])
    m = int(data["cal_m"])
    mk = month_start(y, m)
    amount = int(data["note_amount"])

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO reimbursements (merchant_id, point_code, month_key, amount, note)
            VALUES (:mid, :p, :mk, :a, :n)
        """), {"mid": merch["id"], "p": point, "mk": mk, "a": amount, "n": txt})

    action = f"добавил примечание {amount} ₽ на {point} {y}-{m:02d}"
    await maybe_notify_post_submit_change(merch["id"], y, m, action)

    await state.set_state(FillFlow.calendar)
    await message.answer("✅ Примечание добавлено.", reply_markup=ReplyKeyboardRemove())
    await render_calendar(message, state)


# ----------------- REPORT (admin) -----------------
def parse_month_arg(s: str) -> tuple[int, int] | None:
    s = (s or "").strip()
    m = re.fullmatch(r"(\d{4})-(\d{2})", s)
    if not m:
        return None
    y = int(m.group(1))
    mm = int(m.group(2))
    if mm < 1 or mm > 12:
        return None
    return y, mm


@dp.message(Command("report"))
async def report_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return

    parts = (message.text or "").split()
    # варианты:
    # /report 2026-01
    # /report хрупов 2026-01
    if len(parts) < 2:
        await message.answer("Использование:\n/report YYYY-MM\n/report <ту> YYYY-MM\nПример: /report хрупов 2026-01")
        return

    tu_filter = None
    ym_part = None

    if len(parts) == 2:
        ym_part = parts[1]
    else:
        tu_filter = parts[1].strip().lower()
        ym_part = parts[2]

    ym = parse_month_arg(ym_part)
    if not ym:
        await message.answer("Неверный формат месяца. Нужно YYYY-MM, например 2026-01")
        return

    y, m = ym
    start = month_start(y, m)
    end = month_end_exclusive(y, m)
    mk = start

    # Аггрегация:
    # supply_visits = DAY в день, где has_supply True
    # no_supply_visits = DAY в день, где has_supply False
    # inv = FRI_EVENING + SAT_MORNING
    # reimb_sum = reimbursements sum
    # coffee_bonus_sum = (enabled ? 100 * day_count : 0)
    tu_sql = ""
    params = {"start": start, "end": end, "mk": mk}
    if tu_filter:
        tu_sql = "AND lower(COALESCE(m.tu,'')) = :tu"
        params["tu"] = tu_filter

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            WITH v AS (
              SELECT
                v.merchant_id,
                m.fio,
                m.tu,
                v.point_code,
                v.visit_date,
                v.slot,
                COALESCE(s.has_supply, FALSE) AS has_supply
              FROM visits v
              JOIN merchants m ON m.id = v.merchant_id
              LEFT JOIN supplies s
                ON s.point_code = v.point_code
               AND s.supply_date = v.visit_date
              WHERE v.visit_date >= :start AND v.visit_date < :end
              {tu_sql}
            ),
            agg AS (
              SELECT
                merchant_id,
                fio,
                tu,
                point_code,
                SUM(CASE WHEN slot='DAY' AND has_supply THEN 1 ELSE 0 END) AS supply_visits,
                SUM(CASE WHEN slot='DAY' AND NOT has_supply THEN 1 ELSE 0 END) AS no_supply_visits,
                SUM(CASE WHEN slot IN ('FRI_EVENING','SAT_MORNING') THEN 1 ELSE 0 END) AS inventory_visits,
                SUM(CASE WHEN slot='DAY' THEN 1 ELSE 0 END) AS day_count
              FROM v
              GROUP BY merchant_id, fio, tu, point_code
            ),
            r AS (
              SELECT merchant_id, point_code, COALESCE(SUM(amount),0) AS reimb_sum
              FROM reimbursements
              WHERE month_key = :mk
              GROUP BY merchant_id, point_code
            ),
            c AS (
              SELECT merchant_id, point_code, enabled
              FROM coffee_bonus
              WHERE month_key = :mk
            )
            SELECT
              a.fio,
              a.point_code,
              a.supply_visits,
              a.no_supply_visits,
              a.inventory_visits,
              COALESCE(r.reimb_sum,0) AS reimb_sum,
              a.day_count,
              COALESCE(c.enabled, FALSE) AS coffee_enabled
            FROM agg a
            LEFT JOIN r ON r.merchant_id=a.merchant_id AND r.point_code=a.point_code
            LEFT JOIN c ON c.merchant_id=a.merchant_id AND c.point_code=a.point_code
            ORDER BY a.fio, a.point_code;
        """), params).mappings().all()

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = f"{y}-{m:02d}"

    ws.append([
        "ФИО мерчендайзера",
        "Номер точки",
        "Количество поставок (выходы с поставкой)",
        "Количество выходов без поставок",
        "Количество инвентов (пт вечер + сб утро)",
        "Примечания сумма",
        "Кофемашина сумма",
        "Сумма по точке",
    ])

    for r in rows:
        fio = r["fio"]
        point = r["point_code"]
        supply_vis = int(r["supply_visits"] or 0)
        no_supply_vis = int(r["no_supply_visits"] or 0)
        inv = int(r["inventory_visits"] or 0)
        reimb = int(r["reimb_sum"] or 0)

        day_count = int(r["day_count"] or 0)
        coffee_sum = (100 * day_count) if bool(r["coffee_enabled"]) else 0

        total = supply_vis * 800 + no_supply_vis * 400 + inv * 400 + reimb + coffee_sum
        ws.append([fio, point, supply_vis, no_supply_vis, inv, reimb, coffee_sum, total])

    out = BytesIO()
    wb.save(out)
    out.seek(0)

    fname = f"report_{(tu_filter + '_') if tu_filter else ''}{y}-{m:02d}.xlsx"
    await message.answer_document(BufferedInputFile(out.read(), filename=fname))


# ----------------- HTTP server & webhook -----------------
async def healthcheck(request):
    return web.Response(text="OK")


async def on_startup(app: web.Application):
    if USE_WEBHOOK:
        webhook_url = WEBHOOK_BASE_URL.rstrip("/") + WEBHOOK_PATH
        await bot.set_webhook(
            webhook_url,
            secret_token=WEBHOOK_SECRET or None,
            drop_pending_updates=True
        )
    else:
        await bot.delete_webhook(drop_pending_updates=True)


def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", healthcheck)

    if USE_WEBHOOK:
        SimpleRequestHandler(
            dispatcher=dp,
            bot=bot,
            secret_token=WEBHOOK_SECRET or None
        ).register(app, path=WEBHOOK_PATH)

        setup_application(app, dp, bot=bot)

    app.on_startup.append(on_startup)
    return app


# ----------------- main -----------------
async def main():
    ensure_tables()

    if USE_WEBHOOK:
        app = build_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()

        # keep alive
        while True:
            await asyncio.sleep(3600)

    else:
        await bot.delete_webhook(drop_pending_updates=True)

        async def start_http_server():
            app = build_app()
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "0.0.0.0", PORT)
            await site.start()

        await asyncio.gather(
            dp.start_polling(bot),
            start_http_server(),
        )


if __name__ == "__main__":
    asyncio.run(main())
