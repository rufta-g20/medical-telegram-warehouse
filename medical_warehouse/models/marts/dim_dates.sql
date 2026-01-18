SELECT
    DISTINCT
    -- Create an integer key like 20240118
    CAST(TO_CHAR(message_timestamp, 'YYYYMMDD') AS INT) AS date_key,
    CAST(message_timestamp AS DATE) AS full_date,
    -- Extract day of week (0=Sunday, 6=Saturday)
    EXTRACT(DOW FROM message_timestamp) AS day_of_week,
    TO_CHAR(message_timestamp, 'Day') AS day_name,
    EXTRACT(WEEK FROM message_timestamp) AS week_of_year,
    EXTRACT(MONTH FROM message_timestamp) AS month,
    TO_CHAR(message_timestamp, 'Month') AS month_name,
    EXTRACT(YEAR FROM message_timestamp) AS year,
    -- Check if it's Saturday (6) or Sunday (0)
    CASE 
        WHEN EXTRACT(DOW FROM message_timestamp) IN (0, 6) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend
FROM {{ ref('stg_telegram_messages') }}