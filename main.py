import asyncio
import logging
import os
import sys

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

load_dotenv()

from features import router, init_db, close_db, queue_worker, ADMIN_IDS  # noqa: E402

BOT_TOKEN = os.getenv("BOT_TOKEN")


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        stream=sys.stdout,
    )

    if not BOT_TOKEN:
        logging.error("BOT_TOKEN is not set in .env file! Exiting...")
        return
    if not ADMIN_IDS:
        logging.error("ADMIN_ID is not set in .env file! Exiting...")
        return

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    await init_db()
    dp.include_router(router)

    worker = asyncio.create_task(queue_worker(bot))
    logging.info("Bot starting polling...")
    try:
        await bot.delete_webhook(drop_pending_updates=False)
        # chat_join_request updates are auto-included by aiogram's router
        await dp.start_polling(bot)
    finally:
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass
        await bot.session.close()
        await close_db()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped.")
