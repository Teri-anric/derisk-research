import os
import asyncio

from aiogram.types import BotCommand, BotCommandScopeDefault

from . import bot, dp
from .notifications import notifications_broadcasting
from .crud import get_async_sessionmaker
from .middleware import DatabaseMiddleware


async def bot_start_polling():
    """
    Start the bot polling loop
    (added database middleware and bot commands if is outside Flask Api server)
    """
    await bot.set_my_commands(
        [BotCommand(command="menu", description="Show bot menu")],
        scope=BotCommandScopeDefault(),
    )

    async_sessionmaker = get_async_sessionmaker()
    dp.update.middleware(DatabaseMiddleware(async_sessionmaker))

    await asyncio.gather(
        notifications_broadcasting(is_infinity=True),
        dp.start_polling(bot)
    )


if __name__ == "__main__":
    if bot is not None:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(bot_start_polling())
