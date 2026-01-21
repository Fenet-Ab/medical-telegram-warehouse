SELECT
    ROW_NUMBER() OVER() as channel_key,
    channel_name,
    COUNT(*) as total_posts,
    AVG(views) as avg_views
FROM {{ ref('stg_telegram_messages') }}
GROUP BY channel_name
