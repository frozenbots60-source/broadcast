import os
import asyncio
import logging
from telethon import TelegramClient, events, errors
from telethon.errors import (
    FloodWaitError,
    ChatWriteForbiddenError,
    UserIsBlockedError,
    ChatAdminRequiredError,
    PeerIdInvalidError,
)
from pymongo import MongoClient

# ==========================
# CONFIG
# ==========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = 29568441
API_HASH = "b32ec0fb66d22da6f77d355fbace4f2a"
OWNER_ID = 5268762773

# MongoDB setup
mongo_uri = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://frozenbotss:noobop0011@cluster0.s0tak.mongodb.net/?retryWrites=true&w=majority"
)
mongo_client = MongoClient(mongo_uri)
db = mongo_client["music_bot"]
broadcast_collection = db["broadcast"]

# ==========================
# LOGGING
# ==========================
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# ==========================
# BOT (Telethon)
# ==========================
# session name kept same as original pyrogram client name
client = TelegramClient(
    "broad112cast-bot",
    API_ID,
    API_HASH
)

# Start the client with the bot token when running (synchronous start)
# Handlers are registered below before the client is started.


@client.on(events.NewMessage(pattern=r"^/br(?:@[\w_]+)?(?:\s|$)", from_users=OWNER_ID))
async def broadcast_handler(event: events.NewMessage.Event):
    message = event.message

    # Ensure it's a reply
    if not message.is_reply:
        await message.reply("❌ Please reply to the message you want to broadcast.")
        return

    # Get the replied-to message object
    try:
        broadcast_message = await event.get_reply_message()
    except Exception as e:
        logging.error(f"❌ Couldn't fetch replied message: {e}")
        await message.reply("❌ Failed to fetch the replied message.")
        return

    all_chats = list(broadcast_collection.find({}))

    total = len(all_chats)
    success = 0
    failed = 0
    processed = 0

    logging.info(f"🚀 Starting broadcast to {total} chats...")

    for chat in all_chats:
        processed += 1
        try:
            target_chat_id = int(chat.get("chat_id"))
        except Exception as e:
            logging.warning(f"❌ Invalid chat_id: {chat.get('chat_id')} - {e}")
            failed += 1
            continue

        try:
            # Forward the message (works for any type). Telethon accepts the message object directly.
            sent = await client.forward_messages(
                entity=target_chat_id,
                messages=broadcast_message
            )

            # forward_messages may return a Message or a list of Messages
            # Normalize to a single message object (take first if list)
            if isinstance(sent, (list, tuple)):
                sent_msg = sent[0] if sent else None
            else:
                sent_msg = sent

            if sent_msg is None:
                raise Exception("No message returned after forwarding")

            success += 1
            logging.info(f"✅ Forwarded to {target_chat_id}")

            # Try pinning with notifications ON (notify=True means notifications; we try disable by notify=False)
            try:
                # Telethon's pin_message expects entity and message (object or id). Uses notify parameter to control notifications.
                # We attempt notify=True first to match "disable_notification=False" style from the original.
                await client.pin_message(entity=target_chat_id, message=sent_msg.id, notify=True)
                logging.info(f"📌 Pinned in {target_chat_id}")
            except Exception as e:
                logging.debug(f"⚠️ Could not pin in {target_chat_id}: {e}")

        except FloodWaitError as e:
            # Flood wait - sleep for required seconds and then continue (do not retry the same chat)
            wait_seconds = getattr(e, "seconds", None) or getattr(e, "wait", None) or 0
            logging.warning(f"⏳ FloodWait detected: Sleeping for {wait_seconds}s...")
            await asyncio.sleep(wait_seconds)
            logging.info("🔄 Resuming broadcast after FloodWait")
            # Do not retry here; just continue to next iteration
            continue

        except (ChatWriteForbiddenError, UserIsBlockedError,
                ChatAdminRequiredError, PeerIdInvalidError, ValueError) as e:
            logging.warning(f"❌ Removing {target_chat_id} (reason: {e})")
            failed += 1
            try:
                broadcast_collection.delete_one({"chat_id": chat.get("chat_id")})
            except Exception as ex:
                logging.debug(f"⚠️ Failed to remove chat_id from DB: {ex}")

        except errors.RPCError as e:
            logging.error(f"❌ Failed to forward to {target_chat_id}: {e}")
            failed += 1

        except Exception as e:
            # Catch-all for unexpected errors per chat to avoid stopping the whole broadcast
            logging.error(f"❌ Unexpected error for {target_chat_id}: {e}")
            failed += 1

        # Log progress every 50 messages
        if processed % 50 == 0:
            logging.info(f"📊 Progress: {processed}/{total} | ✅ {success} | ❌ {failed}")

        # Small delay to avoid instant flooding
        await asyncio.sleep(1)

    logging.info(f"✅ Broadcast finished! Total: {total} | Success: {success} | Failed: {failed}")
    try:
        await message.reply(f"📢 Broadcast complete!\n✅ Success: {success}\n❌ Failed & removed: {failed}")
    except Exception as e:
        logging.warning(f"⚠️ Could not send summary reply to owner: {e}")


# ==========================
# START BOT
# ==========================
if __name__ == "__main__":
    print("✅ Broadcast bot is running...")
    # start() is safe to call synchronously here; it will initialize the connection and the event loop used by run_until_disconnected
    client.start(bot_token=BOT_TOKEN)
    client.run_until_disconnected()
