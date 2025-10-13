import os
import asyncio
import logging
import re
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

# Suppress Telethon internal verbose logs
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("telethon.network.mtproto").setLevel(logging.WARNING)

# ==========================
# BOT (Telethon)
# ==========================
client = TelegramClient("broad112cast-bot", API_ID, API_HASH)


# --------------------------
# Broadcast Handler
# --------------------------
@client.on(events.NewMessage(pattern=r"^/br(?:@[\w_]+)?(?:\s|$)", from_users=OWNER_ID))
async def broadcast_handler(event: events.NewMessage.Event):
    message = event.message

    if not message.is_reply:
        await message.reply("❌ Please reply to the message you want to broadcast.")
        return

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
            sent = await client.forward_messages(entity=target_chat_id, messages=broadcast_message)
            sent_msg = sent[0] if isinstance(sent, (list, tuple)) else sent

            if not sent_msg:
                raise Exception("No message returned after forwarding")

            success += 1
            logging.info(f"✅ Forwarded to {target_chat_id}")

            try:
                await client.pin_message(entity=target_chat_id, message=sent_msg.id, notify=True)
                logging.info(f"📌 Pinned in {target_chat_id}")
            except Exception as e:
                logging.debug(f"⚠️ Could not pin in {target_chat_id}: {e}")

        except FloodWaitError as e:
            wait_seconds = getattr(e, "seconds", None) or getattr(e, "wait", None) or 0
            logging.warning(f"⏳ FloodWait detected: Sleeping for {wait_seconds}s...")
            await asyncio.sleep(wait_seconds)
            logging.info("🔄 Resuming broadcast after FloodWait")
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
            logging.error(f"❌ Unexpected error for {target_chat_id}: {e}")
            failed += 1

        if processed % 50 == 0:
            logging.info(f"📊 Progress: {processed}/{total} | ✅ {success} | ❌ {failed}")

        await asyncio.sleep(1)

    logging.info(f"✅ Broadcast finished! Total: {total} | Success: {success} | Failed: {failed}")
    try:
        await message.reply(f"📢 Broadcast complete!\n✅ Success: {success}\n❌ Failed & removed: {failed}")
    except Exception as e:
        logging.warning(f"⚠️ Could not send summary reply to owner: {e}")


# --------------------------
# Auto-register Stream Ended Chats
# --------------------------
@client.on(events.NewMessage)
async def register_stream_chat(event: events.NewMessage.Event):
    message_text = event.raw_text
    # Match pattern: "Stream ended in chat id -1003003164123"
    match = re.search(r"Stream ended in chat id (-?\d+)", message_text)
    if match:
        chat_id = int(match.group(1))
        # Check if already exists
        if not broadcast_collection.find_one({"chat_id": str(chat_id)}):
            try:
                broadcast_collection.insert_one({"chat_id": str(chat_id)})
                logging.info(f"🆕 Registered new chat for broadcast: {chat_id}")
            except Exception as e:
                logging.error(f"❌ Failed to register chat {chat_id}: {e}")


# ==========================
# START BOT
# ==========================
if __name__ == "__main__":
    print("✅ Broadcast bot is running...")
    client.start(bot_token=BOT_TOKEN)
    client.run_until_disconnected()
