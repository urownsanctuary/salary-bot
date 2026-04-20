sent_users = set()

@dp.message()
async def stop_bot(message: types.Message):
    if message.from_user.id in sent_users:
        return

    sent_users.add(message.from_user.id)

    await message.answer(
        "Сверки перенесены на сайт:\n"
        "https://merch-web.onrender.com/login-page\n\n"
        "Бот больше не используется."
    )
