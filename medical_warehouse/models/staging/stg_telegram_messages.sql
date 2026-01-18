WITH raw_data AS (
    SELECT * FROM {{ source('raw', 'telegram_messages') }}
)

SELECT
    -- Cast ID to integer for better indexing 
    CAST(message_id AS INT) AS message_id,
    channel_name,
    -- Convert string date to actual timestamp 
    CAST(message_date AS TIMESTAMP) AS message_timestamp,
    message_text,
    -- Handle nulls and cast counts 
    COALESCE(CAST(views AS INT), 0) AS view_count,
    COALESCE(CAST(forwards AS INT), 0) AS forward_count,
    -- Derived flags 
    has_media AS has_image,
    LENGTH(message_text) AS message_length
FROM raw_data
-- Filter out empty messages 
WHERE message_text IS NOT NULL