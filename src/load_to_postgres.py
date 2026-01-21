import json
import psycopg2
import glob
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS raw.telegram_messages (
    message_id BIGINT,
    channel_name TEXT,
    date TIMESTAMP,
    text TEXT,
    views INT,
    forwards INT,
    has_media BOOLEAN,
    image_path TEXT
)
""")

files = glob.glob("data/raw/telegram_messages/*/*.json")

for file in files:
    with open(file) as f:
        data = json.load(f)

    for row in data:
        cur.execute("""
        INSERT INTO raw.telegram_messages VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            row["message_id"],
            row["channel_name"],
            row["date"],
            row["text"],
            row["views"],
            row["forwards"],
            row["has_media"],
            row.get("image_path")
        ))

conn.commit()
cur.close()
conn.close()
