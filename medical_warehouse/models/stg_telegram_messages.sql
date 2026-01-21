SELECT
    message_id,
    channel_name,
    date::date as message_date,
    text as message_text,
    LENGTH(text) as message_length,
    views,
    forwards,
    has_media
FROM raw.telegram_messages
WHERE text IS NOT NULL
