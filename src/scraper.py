from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
import os
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

client = TelegramClient("session", API_ID, API_HASH)

CHANNELS = [
    "lobelia4cosmetics",
    "tikvahpharma"
]

BASE_PATH = "data/raw/telegram_messages"
IMAGE_PATH = "data/raw/images"


async def scrape():

    await client.start()

    for channel in CHANNELS:

        entity = await client.get_entity(channel)

        offset_id = 0
        limit = 100

        all_messages = []

        while True:
            history = await client(GetHistoryRequest(
                peer=entity,
                offset_id=offset_id,
                offset_date=None,
                add_offset=0,
                limit=limit,
                max_id=0,
                min_id=0,
                hash=0
            ))

            if not history.messages:
                break

            for msg in history.messages:

                message_data = {
                    "message_id": msg.id,
                    "channel_name": channel,
                    "date": msg.date.isoformat() if msg.date else None,
                    "text": msg.message,
                    "views": msg.views,
                    "forwards": msg.forwards,
                    "has_media": True if msg.photo else False
                }

                # Download image
                if msg.photo:
                    os.makedirs(f"{IMAGE_PATH}/{channel}", exist_ok=True)
                    image_file = f"{IMAGE_PATH}/{channel}/{msg.id}.jpg"
                    await msg.download_media(file=image_file)
                    message_data["image_path"] = image_file

                all_messages.append(message_data)

            offset_id = history.messages[-1].id

        # Save JSON
        today = datetime.now().strftime("%Y-%m-%d")
        folder = f"{BASE_PATH}/{today}"
        os.makedirs(folder, exist_ok=True)

        with open(f"{folder}/{channel}.json", "w") as f:
            json.dump(all_messages, f, indent=4)

        print(f"Saved {channel}")

with client:
    client.loop.run_until_complete(scrape())
