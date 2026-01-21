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


def ensure_tables():
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS merchants (
            id SERIAL PRIMARY KEY,
            fio TEXT NOT NULL UNIQUE,
            pass_hash TEXT NOT NULL,
            telegram_id BIGINT UNIQUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS admins (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT NOT NULL UNIQUE
        );
        """))


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def normalize_fio(s: str) -> str:
    s = s.strip().lower()
    s = s.replace("ё", "е")
    s = re.sub(r"\s+", " ", s)
    return s



def hash_last4(last4: str) -> str:
    s = (last4.strip() + SECRET_SALT).encode("utf-8")
    return hashlib.sha256(s).hexdigest()

def get_merch_by_tg_id(tg_id: int):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, fio FROM merchants WHERE telegram_id = :tg_id"),
            {"tg_id": tg_id},
        ).mappings().first()
    return row

def get_merch_by_fio(fio: str):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, fio, pass_hash, telegram_id FROM merchants WHERE fio = :fio"),
            {"fio": fio},
        ).mappings().first()
    return row

def bind_merch_tg_id(merch_id: int, tg_id: int):
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE merchants SET telegram_id = :tg_id WHERE id = :id"),
            {"tg_id": tg_id, "id": merch_id},
        )


class UploadMerchants(StatesGroup):
    waiting_file = State()

class LoginFlow(StatesGroup):
    waiting_fio = State()
    waiting_last4 = State()



@dp.message(Command("start"))
async def start_handler(message: types.Message, state: FSMContext):
    merch = get_merch_by_tg_id(message.from_user.id)
    if merch:
        await message.answer(f"✅ Вы уже авторизованы как: {merch['fio']}")
        await message.answer("Главное меню появится позже. Пока всё ок 🙂")
        return

    await state.set_state(LoginFlow.waiting_fio)
    await message.answer("Привет! 👋\n"
"Для входа введи ФИО полностью.\n\n"
"Пример:\n"
"Иванов Иван Иванович"
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

@dp.message(Command("unbind"))
async def unbind_user(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return

    await message.answer("Отправь ФИО мерча, которому нужно сбросить привязку telegram_id.")



@dp.message(Command("upload_merchants"))
async def upload_merchants_cmd(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для администратора.")
        return

    await state.set_state(UploadMerchants.waiting_file)
    await message.answer(
        "Ок. Пришли CSV-файл мерчендайзеров документом.\n\n"
        "Формат CSV:\n"
        "fio,last4\n"
        "Иванов Иван Иванович,1234\n"
        "Петров Пётр Сергеевич,5678\n\n"
        "Можно разделитель запятая или точка с запятой."
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

        # Определяем разделитель
        sample = text_data[:2048]
        dialect = csv.Sniffer().sniff(sample, delimiters=";,")
        reader = csv.DictReader(StringIO(text_data), dialect=dialect)

        required = {"fio", "last4"}
        if not reader.fieldnames:
            raise ValueError("CSV без заголовков")
        fields = {f.strip().lower() for f in reader.fieldnames}
        if not required.issubset(fields):
            raise ValueError("Нужны колонки: fio,last4")

        added = 0
        updated = 0
        bad_rows = 0

        with engine.begin() as conn:
            for row in reader:
                fio = normalize_fio(str(row.get("fio", "") or row.get("FIO", "") or row.get("ФИО", "")).strip())
                last4 = str(row.get("last4", "") or row.get("LAST4", "")).strip()

                if not fio or not re.fullmatch(r"\d{4}", last4):
                    bad_rows += 1
                    continue

                ph = hash_last4(last4)

                # upsert
                res = conn.execute(text("""
                    INSERT INTO merchants (fio, pass_hash)
                    VALUES (:fio, :pass_hash)
                    ON CONFLICT (fio) DO UPDATE SET pass_hash = EXCLUDED.pass_hash
                    RETURNING xmax;
                """), {"fio": fio, "pass_hash": ph})

                # xmax == 0 примерно означает insert, иначе update (хак Postgres)
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

@dp.message(LoginFlow.waiting_fio)
async def login_get_fio(message: types.Message, state: FSMContext):
    fio = normalize_fio(message.text or "")
    if len(fio) < 2:
        await message.answer("ФИО слишком короткое. Введи полностью (пример: Иванов Иван Иванович).")
        return

    merch = get_merch_by_fio(fio)
    if not merch:
        await message.answer("❌ Не получилось найти ФИО.\n"
"Проверь написание или обратись к территориальному управляющему."
)
        return

    await state.update_data(fio=fio)
    await state.set_state(LoginFlow.waiting_last4)
    await message.answer("Теперь введи последние 4 цифры номера телефона (только 4 цифры).")


@dp.message(LoginFlow.waiting_last4)
async def login_get_last4(message: types.Message, state: FSMContext):
    last4 = (message.text or "").strip()
    if not re.fullmatch(r"\d{4}", last4):
        await message.answer("Нужно ровно 4 цифры. Пример: 1234")
        return

    data = await state.get_data()
    fio = data.get("fio")
    merch = get_merch_by_fio(fio)

    if not merch:
        await state.clear()
        await message.answer("❌ Ошибка: запись не найдена. Начни заново: /start")
        return

    expected = merch["pass_hash"]
    if hash_last4(last4) != expected:
        await message.answer("❌ Неверные 4 цифры. Попробуй ещё раз.")
        return

    # защита: если telegram_id уже привязан к другому — не даём привязать
    if merch["telegram_id"] is not None and int(merch["telegram_id"]) != message.from_user.id:
        await state.clear()
        await message.answer("⛔ Этот аккаунт уже привязан к другому Telegram. Обратитесь к администратору.")
        return

    bind_merch_tg_id(merch["id"], message.from_user.id)
    await state.clear()
    await message.answer(f"✅ Успешный вход. Вы: {merch['fio']}")
    await message.answer("Главное меню появится дальше 🙂")



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


# ---------- HTTP SERVER (для Render) ----------
async def healthcheck(request):
    return web.Response(text="OK")


async def start_http_server():
    app = web.Application()
    app.router.add_get("/", healthcheck)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()


# ---------- MAIN ----------
async def main():
    ensure_tables()
    await asyncio.gather(
        dp.start_polling(bot),
        start_http_server(),
    )


if __name__ == "__main__":
    asyncio.run(main())
