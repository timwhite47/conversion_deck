CONVERTED_EVENTS_QUERY = """
SELECT u.distinct_id, e.type, count(e.event_id)
FROM users AS u

LEFT JOIN customers AS c
ON u.email = c.email

LEFT JOIN events AS e
ON e.distinct_id = u.distinct_id

WHERE e.type IS NOT NULL AND e.time < COALESCE(c.created_at, CURRENT_TIMESTAMP)
GROUP BY u.distinct_id, e.type;
"""

CONVERTED_AGE_QUERY = """
SELECT
    u.distinct_id,
    CASE WHEN c.converted_at IS NOT NULL THEN TRUE ELSE FALSE END AS converted,
    extract(DAY FROM c.converted_at) AS account_age
FROM users AS u
LEFT JOIN (
    SELECT email, created_at AS converted_at
    FROM customers AS c
    INNER JOIN subscriptions AS converted
    ON converted.customer_id = c.identifier
) AS c
ON c.email = u.email
GROUP BY u.distinct_id, c.converted_at;
"""
