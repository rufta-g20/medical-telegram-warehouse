-- Every detection must have a corresponding record in the messages fact table
SELECT
    y.message_id
FROM {{ ref('fct_image_detections') }} y
LEFT JOIN {{ ref('fct_messages') }} m ON y.message_id = m.message_id
WHERE m.message_id IS NULL