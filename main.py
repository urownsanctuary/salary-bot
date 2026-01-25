import os
import asyncio
import hashlib
import re
from io import BytesIO
from datetime import datetime, date, timedelta

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
)

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from aiohttp import web

import openpyxl


load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
PORT = int(os.getenv("PORT", "10000"))
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "")
SECRET_SALT = os.getenv("SECRET_SALT", "CHANGE_ME_SALT")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")


def parse_admin_ids(raw: str) -> set[int]:
    ids = set()
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            ids.add(int(part))
    return ids


ADMIN_IDS = parse_admin_ids(ADMIN_IDS_RAW)

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


def ensure_tables():
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS merchants (
            id SERIAL PRIMARY KEY,
            fio TEXT NOT NULL,
            fio_norm TEXT,
            pass_hash TEXT NOT NULL,
            telegram_id BIGINT UNIQUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))
        conn.execute(text("ALTER TABLE merchants ADD COLUMN IF NOT EXISTS fio_norm TEXT;"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS merchants_fio_norm_uq ON merchants(fio_norm);"))

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

        # Примечания/возмещения по точке за месяц
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS reimbursements (
            id SERIAL PRIMARY KEY,
            merchant_id INTEGER NOT NULL REFERENCES merchants(id) ON DELETE CASCADE,
            point_code TEXT NOT NULL,
            month_key DATE NOT NULL, -- 1-е число месяца
            amount INTEGER NOT NULL, -- можно отрицательное
            note TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS reimb_idx ON reimbursements(merchant_id, point_code, month_key);"))

        # best-effort заполнение fio_norm для старых строк
        conn.execute(text("""
        UPDATE merchants
        SET fio_norm = lower(replace(replace(fio, 'Ё', 'Е'), 'ё', 'е'))
        WHERE fio_norm IS NULL OR fio_norm = '';
        """))


def get_merch_by_tg_id(tg_id: int):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, fio FROM merchants WHERE telegram_id = :tg_id"),
            {"tg_id": tg_id},
        ).mappings().first()
    return row


def get_merch_by_fio(fio: str):
    fn = fio_norm(fio)
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, fio, pass_hash, telegram_id FROM merchants WHERE fio_norm = :fio_norm"),
            {"fio_norm": fn},
        ).mappings().first()
    return row


def bind_merch_tg_id(merch_id: int, tg_id: int):
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE merchants SET telegram_id = :tg_id WHERE id = :id"),
            {"tg_id": tg_id, "id": merch_id},
        )


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


def compute_month_total(merchant_id: int, point_code: str, y: int, m: int) -> int:
    supply = get_supply_map(point_code, y, m)
    visits = get_visits_for_month(merchant_id, point_code, y, m)
    total = 0
    for day, slots in visits.items():
        for slot in slots:
            if slot == "FRI_EVENING":
                total += 400
            elif slot == "SAT_MORNING":
                total += 400
            else:
                total += 800 if supply.get(day, False) else 400
    total += get_reimb_sum(merchant_id, point_code, y, m)
    return total


# ----------------- States -----------------
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


# ----------------- Calendar UI -----------------
def build_calendar_kb(y: int, m: int, supply: dict[int, bool], visits: dict[int, set[str]]) -> InlineKeyboardMarkup:
    dim = days_in_month(y, m)
    first_wd = date(y, m, 1).weekday()  # Mon=0
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
        InlineKeyboardButton(text="➕ Примечание", callback_data="note:add"),
        InlineKeyboardButton(text="✅ Готово", callback_data="done"),
    ])
    rows.append([
        InlineKeyboardButton(text="◀️ Месяц", callback_data="nav:prev"),
        InlineKeyboardButton(text="Месяц ▶️", callback_data="nav:next"),
    ])
    rows.append([
        InlineKeyboardButton(text="🔙 Сменить точку", callback_data="back_point"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_friday_slot_kb(day: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Пт: Дневной", callback_data=f"slot:DAY:{day}")],
        [InlineKeyboardButton(text="Пт: Вечерний (400)", callback_data=f"slot:FRI_EVENING:{day}")],
        [InlineKeyboardButton(text="↩️ Назад к календарю", callback_data="slot_cancel")],
    ])


def build_saturday_slot_kb(day: int) -> InlineKeyboardMarkup:
    # ВАЖНО: суббота может быть и утро (инвент), и день (поставка/без поставки)
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
    total = compute_month_total(merch["id"], point, y, m)

    text_msg = (
        f"📍 Точка: {point}\n"
        f"🗓 {month_title(y, m)}\n\n"
        f"Легенда:\n"
        f"🟩 есть поставка (≥5) | ⬜ нет поставки\n"
        f"✅ дневной выход | 🌙 пятница вечер | 🌅 суббота утро\n\n"
        f"🧾 Примечания/возмещения за месяц: {reimb_sum} ₽\n"
        f"💰 Сумма по точке за месяц: {total} ₽"
    )

    kb = build_calendar_kb(y, m, supply, visits)

    if isinstance(message_or_cb, types.CallbackQuery):
        await message_or_cb.message.edit_text(text_msg, reply_markup=kb)
        await message_or_cb.answer()
    else:
        await message_or_cb.answer(text_msg, reply_markup=kb)


# ----------------- Visits / collisions -----------------
def add_or_remove_visit(merchant_id: int, point: str, y: int, m: int, day: int, slot: str) -> tuple[bool, bool]:
    """Returns (exists_before, now_added). If existed -> removed."""
    d = date(y, m, day)
    with engine.begin() as conn:
        existing = conn.execute(text("""
            SELECT id FROM visits WHERE merchant_id=:mid AND point_code=:p AND visit_date=:d AND slot=:s
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
    # пересечение считаем по ДНЮ, независимо от slot (как ты и хотел ранее)
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
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, msg_admin)
        except Exception:
            pass

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
        "Введите номер/код точки.\nНапример: 2674MT_3\n\nЕсли хотите отменить — нажмите «Отмена».",
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
    await cb.message.edit_text("Введите номер/код точки:", reply_markup=None)
    await cb.message.answer("Введите номер/код точки:", reply_markup=CANCEL_KB)
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
    if added:
        others = find_collisions(point, y, m, day, merch["id"])
        if others:
            await cb.message.answer("⚠️ Внимание: есть пересечение с другим мерчендайзером. Нужна проверка.")
            await notify_collision(point, y, m, day, merch["fio"], others)

    await render_calendar(cb, state)


@dp.callback_query(F.data == "slot_cancel")
async def slot_cancel(cb: types.CallbackQuery, state: FSMContext):
    await render_calendar(cb, state)


# ----------------- Notes / reimbursements -----------------
@dp.callback_query(F.data == "note:add")
async def note_add(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if "point_code" not in data:
        await cb.answer()
        return

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
        # вернуться в календарь
        await state.set_state(FillFlow.calendar)
        await message.answer("Ок, отменил добавление примечания.", reply_markup=ReplyKeyboardRemove())
        await render_calendar(message, state)
        return

    if not re.fullmatch(r"-?\d{1,6}", txt):
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

    if len(txt) < 2:
        await message.answer("Комментарий слишком короткий. Напиши пару слов.", reply_markup=CANCEL_KB)
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
    if len(parts) < 2:
        await message.answer("Использование: /report YYYY-MM\nПример: /report 2026-01")
        return

    ym = parse_month_arg(parts[1])
    if not ym:
        await message.answer("Неверный формат месяца. Нужно YYYY-MM, например 2026-01")
        return

    y, m = ym
    start = month_start(y, m)
    end = month_end_exclusive(y, m)
    mk = start

    # Собираем агрегаты одним запросом:
    # - DAY с поставкой/без поставки через LEFT JOIN supplies
    # - инвенты
    # - reimbursements суммой
    with engine.connect() as conn:
        rows = conn.execute(text("""
            WITH v AS (
              SELECT
                v.merchant_id,
                m.fio,
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
            ),
            agg AS (
              SELECT
                merchant_id,
                fio,
                point_code,
                SUM(CASE WHEN slot='DAY' AND has_supply THEN 1 ELSE 0 END) AS supply_visits,
                SUM(CASE WHEN slot='DAY' AND NOT has_supply THEN 1 ELSE 0 END) AS no_supply_visits,
                SUM(CASE WHEN slot IN ('FRI_EVENING','SAT_MORNING') THEN 1 ELSE 0 END) AS inventory_visits
              FROM v
              GROUP BY merchant_id, fio, point_code
            ),
            r AS (
              SELECT merchant_id, point_code, COALESCE(SUM(amount),0) AS reimb_sum
              FROM reimbursements
              WHERE month_key = :mk
              GROUP BY merchant_id, point_code
            )
            SELECT
              a.fio,
              a.point_code,
              a.supply_visits,
              a.no_supply_visits,
              a.inventory_visits,
              COALESCE(r.reimb_sum,0) AS reimb_sum
            FROM agg a
            LEFT JOIN r ON r.merchant_id=a.merchant_id AND r.point_code=a.point_code
            ORDER BY a.fio, a.point_code;
        """), {"start": start, "end": end, "mk": mk}).mappings().all()

    # Генерим Excel
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = f"{y}-{m:02d}"

    headers = [
        "ФИО мерчендайзера",
        "Номер точки",
        "Количество поставок (выходы с поставкой)",
        "Количество выходов без поставок",
        "Количество инвентов (пт вечер + сб утро)",
        "Примечания сумма",
        "Сумма по точке",
    ]
    ws.append(headers)

    for r in rows:
        fio = r["fio"]
        point = r["point_code"]
        supply_vis = int(r["supply_visits"] or 0)
        no_supply_vis = int(r["no_supply_visits"] or 0)
        inv = int(r["inventory_visits"] or 0)
        reimb = int(r["reimb_sum"] or 0)
        total = supply_vis * 800 + no_supply_vis * 400 + inv * 400 + reimb

        ws.append([fio, point, supply_vis, no_supply_vis, inv, reimb, total])

    # чуть ширины колонок
    widths = [32, 18, 20, 22, 22, 16, 16]
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[openpyxl.utils.get_column_letter(i)].width = w

    out = BytesIO()
    wb.save(out)
    out.seek(0)

    filename = f"report_{y}-{m:02d}.xlsx"
    await message.answer_document(BufferedInputFile(out.read(), filename=filename))


# ----------------- HTTP server (Render) -----------------
async def healthcheck(request):
    return web.Response(text="OK")


async def start_http_server():
    app = web.Application()
    app.router.add_get("/", healthcheck)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()


# ----------------- main -----------------
async def main():
    ensure_tables()
    await asyncio.gather(
        dp.start_polling(bot),
        start_http_server(),
    )


if __name__ == "__main__":
    asyncio.run(main())
