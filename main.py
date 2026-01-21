import os
import asyncio
import hashlib

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from dotenv import load_dotenv

from sqlalchemy import create_engine, text

from aiohttp import web


load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
PORT = int(os.getenv("PORT", "10000"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


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


@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("Привет! Я бот для расчёта зарплаты. ✅")


@dp.message(Command("pingdb"))
async def ping_db(message: types.Message):
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
        await message.answer("✅ База данных доступна.")
    except Exception as e:
        await message.answer(f"❌ Ошибка БД: {type(e).__name__}")


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
