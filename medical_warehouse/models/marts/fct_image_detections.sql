{{ config(materialized='table') }}

WITH yolo_raw AS (
    SELECT 
        message_id,
        channel_name,
        detected_obj,
        confidence,
        image_category
    FROM {{ source('raw', 'raw_yolo_detections') }}
),

messages AS (
    SELECT 
        message_id,
        channel_name, 
        message_timestamp,
        view_count
    FROM {{ ref('stg_telegram_messages') }}
)

SELECT
    m.message_id,
    m.channel_name, 
    MD5(m.channel_name) AS channel_key,
    TO_CHAR(m.message_timestamp, 'YYYYMMDD')::INT AS date_key,
    y.detected_obj,
    y.confidence,
    y.image_category,
    m.view_count
FROM messages m
INNER JOIN yolo_raw y ON m.message_id = y.message_id