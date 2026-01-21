import os
import asyncio
import hashlib

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from dotenv import load_dotenv

from sqlalchemy import (
    create_engine,
    text,
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Add it in Render Environment Variables.")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set. Add it in Render Environment Variables.")

# SQLAlchemy engine (sync) — достаточно для MVP
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


def ensure_tables():
    """
    Создаёт таблицы, если их ещё нет.
    Делается один раз при старте бота.
    """
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


def hash_last4(last4: str) -> str:
    """
    Хешируем 4 цифры + соль.
    Соль хранится в переменной окружения SECRET_SALT.
    """
    salt = os.getenv("SECRET_SALT", "CHANGE_ME_SALT")
    s = (last4.strip() + salt).encode("utf-8")
    return hashlib.sha256(s).hexdigest()


@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("Привет! Я бот для расчёта зарплаты. ✅")


@dp.message(Command("pingdb"))
async def ping_db(message: types.Message):
    """
    Тестовая команда: проверяем, что база доступна.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
        await message.answer("✅ База данных доступна.")
    except Exception as e:
        await message.answer(f"❌ База недоступна: {type(e).__name__}")


async def main():
    # создаём таблицы перед запуском polling
    ensure_tables()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
