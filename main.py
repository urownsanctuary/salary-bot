import asyncio
import os
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import CommandStart

BOT_TOKEN = os.getenv("BOT_TOKEN")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

sent_users = set()

@dp.message()
async def stop_bot(message: Message):
    if message.from_user.id in sent_users:
        return

    sent_users.add(message.from_user.id)

    await message.answer(
        "Коллеги, добрый день!\n\n"
        "Сверки перенесены на сайт.\n\n"
        "Пожалуйста, используйте новую систему:\n"
        "https://merch-web.onrender.com/login-page\n\n"
        "Бот больше не используется."
    )

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
