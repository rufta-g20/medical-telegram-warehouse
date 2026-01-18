-- Messages should not have a timestamp in the future 
SELECT *
FROM {{ ref('stg_telegram_messages') }}
WHERE message_timestamp > CURRENT_TIMESTAMP