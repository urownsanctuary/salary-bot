import os
import asyncio
import hashlib
import csv
import re
from io import StringIO, BytesIO

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from aiohttp import web

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


# ----------------- UI (кнопки) -----------------
LOGIN_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Отмена"), KeyboardButton(text="Заново")],
    ],
    resize_keyboard=True
)

MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Заполнить сверку")],
    ],
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
    s = re.sub(r"\s+", " ", s)
    return s


def extract_last4_from_phone(phone: str) -> str:
    """
    Берём любые форматы телефона:
    8-920-888-88-88, +7 (920) 888-88-88, 89208888888 и т.п.
    Вытаскиваем только цифры и берём последние 4.
    """
    digits = re.sub(r"\D+", "", phone or "")
    if len(digits) < 4:
        return ""
    return digits[-4:]


def hash_last4(last4: str) -> str:
    s = (last4.strip() + SECRET_SALT).encode("utf-8")
    return hashlib.sha256(s).hexdigest()


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

        # миграции на случай, если таблица уже была создана старой версией
        conn.execute(text("ALTER TABLE merchants ADD COLUMN IF NOT EXISTS fio_norm TEXT;"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS merchants_fio_norm_uq ON merchants(fio_norm);"))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS admins (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT NOT NULL UNIQUE
        );
        """))

        # попытка заполнить fio_norm для старых строк (минимально)
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


# ----------------- States -----------------
class UploadMerchants(StatesGroup):
    waiting_file = State()


class LoginFlow(StatesGroup):
    waiting_fio = State()
    waiting_last4 = State()


# ----------------- Common “cancel/restart” -----------------
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
        "Иванов Иван Иванович",
        reply_markup=LOGIN_KB
    )


@dp.message(Command("pingdb"))
async def ping_db(message: types.Message):
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
        await message.answer("✅ База данных доступна.")
    except Exception as e:
        await message.answer(f"❌ Ошибка БД: {type(e).__name__}")


@dp.message(Command("myid"))
async def my_id(message: types.Message):
    await message.answer(f"Ваш Telegram ID: {message.from_user.id}")


# ----------------- Login flow -----------------
@dp.message(LoginFlow.waiting_fio)
async def login_get_fio(message: types.Message, state: FSMContext):
    fio_in = fio_display(message.text or "")
    if len(fio_in) < 5:
        await message.answer("ФИО слишком короткое. Введи полностью (пример: Иванов Иван Иванович).", reply_markup=LOGIN_KB)
        return

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
    await message.answer("Теперь введи последние 4 цифры номера телефона (только 4 цифры).", reply_markup=LOGIN_KB)


@dp.message(LoginFlow.waiting_last4)
async def login_get_last4(message: types.Message, state: FSMContext):
    last4 = (message.text or "").strip()
    if not re.fullmatch(r"\d{4}", last4):
        await message.answer("Нужно ровно 4 цифры. Пример: 6384", reply_markup=LOGIN_KB)
        return

    data = await state.get_data()
    fio_in = data.get("fio", "")
    merch = get_merch_by_fio(fio_in)

    if not merch:
        await state.clear()
        await message.answer("❌ Ошибка: запись не найдена. Начни заново: /start", reply_markup=ReplyKeyboardRemove())
        return

    if hash_last4(last4) != merch["pass_hash"]:
        await message.answer("❌ Неверные 4 цифры. Попробуй ещё раз.", reply_markup=LOGIN_KB)
        return

    if merch["telegram_id"] is not None and int(merch["telegram_id"]) != message.from_user.id:
        await state.clear()
        await message.answer("⛔ Этот аккаунт уже привязан к другому Telegram. Обратитесь к администратору.", reply_markup=ReplyKeyboardRemove())
        return

    bind_merch_tg_id(merch["id"], message.from_user.id)
    await state.clear()
    await message.answer(f"✅ Успешный вход. Вы: {merch['fio']}", reply_markup=MAIN_KB)


# ----------------- Merch menu (пока заглушка) -----------------
@dp.message(F.text == "Заполнить сверку")
async def fill_reconcile_stub(message: types.Message):
    merch = get_merch_by_tg_id(message.from_user.id)
    if not merch:
        await message.answer("Сначала нужно войти: /start", reply_markup=ReplyKeyboardRemove())
        return
    await message.answer("Ок! Дальше здесь будет ввод точки и календарь. (Следующий этап)", reply_markup=MAIN_KB)


# ----------------- Admin: upload merchants -----------------
@dp.message(Command("upload_merchants"))
async def upload_merchants_cmd(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return

    await state.set_state(UploadMerchants.waiting_file)
    await message.answer(
        "Ок. Пришли CSV-файл мерчендайзеров документом.\n\n"
        "Поддерживаются форматы:\n"
        "1) fio,phone\n"
        "   РОМАШИНА ЕКАТЕРИНА ЮРЬЕВНА,8-920-888-88-88\n\n"
        "2) fio,last4\n"
        "   Ромашина Екатерина Юрьевна,6384\n\n"
        "Разделитель: запятая или точка с запятой.\n"
        "Телефон может быть с дефисами/скобками/пробелами — мы возьмём последние 4 цифры."
    )


@dp.message(UploadMerchants.waiting_file, F.document)
async def handle_merchants_file(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return

    doc = message.document
    try:
        file = await bot.get_file(doc.file_id)
        buf = BytesIO()
        await bot.download_file(file.file_path, destination=buf)
        buf.seek(0)

        text_data = buf.read().decode("utf-8-sig", errors="replace")

        sample = text_data[:2048]
        dialect = csv.Sniffer().sniff(sample, delimiters=";,")
        reader = csv.DictReader(StringIO(text_data), dialect=dialect)

        if not reader.fieldnames:
            raise ValueError("CSV без заголовков")

        fields = {f.strip().lower() for f in reader.fieldnames}

        # варианты заголовков
        fio_field = None
        for cand in ["fio", "фио", "full_name", "name"]:
            if cand in fields:
                fio_field = cand
                break

        phone_field = None
        for cand in ["phone", "телефон", "phone_number", "mobile"]:
            if cand in fields:
                phone_field = cand
                break

        last4_field = None
        for cand in ["last4", "pass", "password", "last_4"]:
            if cand in fields:
                last4_field = cand
                break

        if fio_field is None:
            raise ValueError("Не нашёл колонку fio/ФИО")

        if phone_field is None and last4_field is None:
            raise ValueError("Нужна колонка phone/телефон или last4")

        added = 0
        updated = 0
        bad_rows = 0

        with engine.begin() as conn:
            for row in reader:
                # достаём fio
                fio_raw = ""
                for k, v in row.items():
                    if (k or "").strip().lower() == fio_field:
                        fio_raw = str(v or "")
                        break

                fio_disp = fio_display(fio_raw)
                fio_n = fio_norm(fio_raw)

                # достаём last4
                last4 = ""
                if last4_field is not None:
                    for k, v in row.items():
                        if (k or "").strip().lower() == last4_field:
                            last4 = str(v or "").strip()
                            break

                if not re.fullmatch(r"\d{4}", last4):
                    # пробуем извлечь из телефона
                    if phone_field is not None:
                        phone_raw = ""
                        for k, v in row.items():
                            if (k or "").strip().lower() == phone_field:
                                phone_raw = str(v or "")
                                break
                        last4 = extract_last4_from_phone(phone_raw)

                if not fio_n or not re.fullmatch(r"\d{4}", last4):
                    bad_rows += 1
                    continue

                ph = hash_last4(last4)

                res = conn.execute(text("""
                    INSERT INTO merchants (fio, fio_norm, pass_hash)
                    VALUES (:fio, :fio_norm, :pass_hash)
                    ON CONFLICT (fio_norm) DO UPDATE
                        SET fio = EXCLUDED.fio,
                            pass_hash = EXCLUDED.pass_hash
                    RETURNING xmax;
                """), {"fio": fio_disp, "fio_norm": fio_n, "pass_hash": ph})

                xmax = res.scalar()
                if xmax == 0:
                    added += 1
                else:
                    updated += 1

        await state.clear()
        await message.answer(
            f"✅ Готово.\n"
            f"Добавлено: {added}\n"
            f"Обновлено: {updated}\n"
            f"Пропущено (ошибочные строки): {bad_rows}"
        )

    except Exception as e:
        await state.clear()
        await message.answer(f"❌ Не смог обработать файл: {type(e).__name__}: {e}")


@dp.message(UploadMerchants.waiting_file)
async def waiting_file_hint(message: types.Message):
    await message.answer("Пришли CSV-файл как документ (скрепка → Файл).")


@dp.message(Command("merchants_count"))
async def merchants_count(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return
    with engine.connect() as conn:
        cnt = conn.execute(text("SELECT COUNT(*) FROM merchants;")).scalar()
    await message.answer(f"Сейчас мерчендайзеров в базе: {cnt}")


# ----------------- HTTP server (для Render Web Service) -----------------
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
