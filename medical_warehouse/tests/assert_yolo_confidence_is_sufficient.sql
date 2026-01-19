-- Detections should always have a confidence score above our threshold
-- Returns records where confidence is too low to fail the test
SELECT
    message_id,
    confidence
FROM {{ ref('fct_image_detections') }}
WHERE confidence < 0.25