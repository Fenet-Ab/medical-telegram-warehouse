SELECT
    s.message_id,
    d.channel_key,
    s.message_date,
    s.message_text,
    s.message_length,
    s.views,
    s.forwards,
    s.has_media
FROM {{ ref('stg_telegram_messages') }} s
JOIN {{ ref('dim_channels') }} d
ON s.channel_name = d.channel_name
