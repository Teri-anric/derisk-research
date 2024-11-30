import asyncio
from asyncio.queues import Queue, QueueFull
from redis.asyncio import Redis as AsyncRedis
from contextlib import suppress
from uuid import UUID

from aiogram import exceptions
from aiogram.utils.deep_linking import create_deep_link
from database.crud import DBConnector
from database.models import NotificationData, TelegramLog

from .bot import bot
from .config import REDIS_HOST, REDIS_PORT, TELEGRAM_NOTIFICATION_QUEUE

redis_client = AsyncRedis(host=REDIS_HOST, port=REDIS_PORT)
db_connector = DBConnector()

DEFAULT_MESSAGE_TEMPLATE = (
    "Warning. Your health ratio is too low for wallet_id {wallet_id}"
)


async def get_subscription_link(ident: UUID) -> str:
    """
    Generate a subscription link for a given identifier.

    :param ident: The unique identifier for the subscription (from NotificationData.id).
    :return: The generated subscription link, e.g., "https://t.me/TestBot?start=DARAAD".
    """
    me = await bot.me()
    return create_deep_link(
        username=me.username, link_type="start", payload=str(ident), encode=True
    )


async def send_notification(notification_id: UUID) -> None:
    """
    Add a Telegram ID to the queue for sending a notification.

    :param notification_id: Unique identifier of the NotificationData.id, which will be added to the queue for sending via Telegram.
    """
    await redis_client.lpush(TELEGRAM_NOTIFICATION_QUEUE, notification_id)


async def _log_send_message(notification_id: UUID, text: str, is_succesfully: bool):
    """
    Logs the sending status of a message.

    :param notification_id: The UUID identifying the notification data.
    :param text: The message text that was sent.
    :param is_succesfully: A boolean indicating whether the message was sent successfully or not.
    """
    db_connector.write_to_db(
        TelegramLog(
            notification_data_id=notification_id,
            is_succesfully=is_succesfully,
            message=text,
        )
    )


async def notifications_broadcasting(
    is_infinity: bool = False, sleep_time: float = 0.05
) -> None:
    """
    Process the queue and send notifications.

    This method processes the queue of Notifiication ID and sends notifications to each user.

    :param is_infinity: A boolean indicating whether the processing loop should continue indefinitely.
                If set to True, the method will continuously process notifications.
                Defaults to False.
    :param sleep_time: The time interval (in seconds) to wait between processing notifications.
                This parameter is effective only when is_infinity is set to True.
                Defaults to 0.05 seconds.
    """
    while notification_id := await redis_client.rpop(TELEGRAM_NOTIFICATION_QUEUE):
        # Retrieve notification data from the database based on its ID
        notification = db_connector.get_object(NotificationData, notification_id)
        if notification is None:
            continue  # skip is not valid notification_id
        is_succesfully = False
        # create text message
        text = DEFAULT_MESSAGE_TEMPLATE.format(wallet_id=notification.wallet_id)

        try:
            # Check if the notification has a Telegram ID and send the message
            if notification.telegram_id:
                await bot.send_message(
                    chat_id=notification.telegram_id,
                    text=text,
                )
                is_succesfully = True
        except exceptions.TelegramRetryAfter as e:
            # If Telegram returns a RetryAfter exception, wait for the specified time and then retry
            await asyncio.sleep(e.retry_after)
            # return notification to queue
            await redis_client.lpush(TELEGRAM_NOTIFICATION_QUEUE, notification_id)
        except exceptions.TelegramAPIError:
            pass  # skip errors

        finally:
            # Log the sending status of the message
            await _log_send_message(notification_id, text, is_succesfully)

        # If the loop is not set to infinity, break out after processing one notification
        if not is_infinity:
            break
        await asyncio.sleep(sleep_time)
