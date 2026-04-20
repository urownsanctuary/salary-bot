from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
import os

BOT_TOKEN = os.getenv("BOT_TOKEN")  # токен из переменных Render

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

sent_users = set()

@dp.message_handler()
async def stop_bot(message: types.Message):
    if message.from_user.id in sent_users:
        return

    sent_users.add(message.from_user.id)

    await message.answer(
        "Коллеги, добрый вечер!\n\n"
        "Сверки перенесены на сайт.\n\n"
        "Пожалуйста, используйте новую систему:\n"
        "https://merch-web.onrender.com/login-page\n\n"
        "Бот больше не используется."
    )

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
