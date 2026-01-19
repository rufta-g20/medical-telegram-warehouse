-- Business Logic: If a person is detected, it cannot be a 'product_display'
SELECT
    message_id,
    detected_obj,
    image_category
FROM {{ ref('fct_image_detections') }}
WHERE detected_obj = 'person' 
  AND image_category = 'product_display'