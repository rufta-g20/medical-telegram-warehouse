-- View counts should never be negative
SELECT *
FROM {{ ref('fct_messages') }}
WHERE view_count < 0