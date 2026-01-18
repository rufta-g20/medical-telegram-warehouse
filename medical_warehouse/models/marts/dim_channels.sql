SELECT
    -- Create a unique key for the channel 
    MD5(channel_name) AS channel_key,
    channel_name,
    MIN(message_timestamp) AS first_post_date,
    MAX(message_timestamp) AS last_post_date,
    COUNT(message_id) AS total_posts,
    AVG(view_count) AS avg_views
FROM {{ ref('stg_telegram_messages') }}
GROUP BY channel_name