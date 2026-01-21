import os
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Add it in Render Environment Variables.")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("Привет! Я бот для расчёта зарплаты. ✅")


@dp.message()
async def echo_handler(message: types.Message):
    await message.answer("Я живой 🙂 Напиши /start")


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
