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
    u.vertical,
    u.camp_deliveries

FROM users AS u
LEFT JOIN (
    SELECT email, created_at AS converted_at
    FROM customers AS c
    INNER JOIN subscriptions AS converted
    ON converted.customer_id = c.identifier
) AS c
ON c.email = u.email
GROUP BY u.distinct_id, c.converted_at, u.vertical, u.camp_deliveries;
"""

CHURNED_AGE_QUERY = """
SELECT
    u.distinct_id,
    LOG(1+extract(DAY FROM COALESCE(churned_at, CURRENT_TIMESTAMP) - converted_at)) AS account_age,
    CASE WHEN churned_at IS NOT NULL THEN TRUE ELSE FALSE END AS churned,
    camp_deliveries,
    vertical

FROM users AS u

LEFT JOIN customers AS c
ON c.email = u.email

INNER JOIN (
    SELECT email, MAX(pe.time) AS converted_at
    FROM customers AS c
    LEFT JOIN payment_events AS pe
    ON pe.customer_id = c.identifier
    WHERE pe.type = 'customer.subscription.created'
    GROUP BY c.email
) AS converted
ON u.email = converted.email

LEFT JOIN (
    SELECT email, MAX(pe.time) AS churned_at
    FROM customers AS c
    LEFT JOIN payment_events AS pe
    ON pe.customer_id = c.identifier
    WHERE pe.type = 'customer.subscription.deleted'
    GROUP BY c.email
) AS churned
ON u.email = churned.email
"""

CHURNED_EVENT_QUERY = """
SELECT
    u.distinct_id,
    e.type,
    count(e.event_id)

FROM users AS u

LEFT JOIN customers AS c
ON c.email = u.email

INNER JOIN (
    SELECT email, MAX(pe.time) AS converted_at
    FROM customers AS c
    LEFT JOIN payment_events AS pe
    ON pe.customer_id = c.identifier
    WHERE pe.type = 'customer.subscription.created'
    GROUP BY c.email
) AS converted
ON u.email = converted.email

LEFT JOIN (
    SELECT email, MAX(pe.time) AS churned_at
    FROM customers AS c
    LEFT JOIN payment_events AS pe
    ON pe.customer_id = c.identifier
    WHERE pe.type = 'customer.subscription.deleted'
    GROUP BY c.email
) AS churned
ON u.email = churned.email

LEFT JOIN events AS e
ON e.distinct_id = u.distinct_id

WHERE e.time > converted_at AND e.time < COALESCE(churned_at, current_timestamp)
GROUP BY e.type, u.distinct_id
"""

CHURN_PREDICTION_QUERY = """
SELECT
    u.distinct_id,
    e.type,
    count(e.event_id),
    LOG(1+extract(DAY FROM CURRENT_TIMESTAMP - converted_at)) AS account_age,
    u.vertical,
    u.camp_deliveries

FROM users AS u

INNER JOIN customers AS c
ON c.email = u.email

INNER JOIN subscriptions AS s
ON s.customer_id = c.identifier

LEFT JOIN events AS e
ON e.distinct_id = u.distinct_id

INNER JOIN (
    SELECT email, MAX(pe.time) AS converted_at
    FROM customers AS c
    LEFT JOIN payment_events AS pe
    ON pe.customer_id = c.identifier
    WHERE pe.type = 'customer.subscription.created'
    GROUP BY c.email
) AS converted
ON u.email = converted.email

WHERE e.type IS NOT NULL
GROUP BY u.distinct_id, e.type, converted_at, u.vertical, u.camp_deliveries;
"""

CONVERSION_PREDICTION_QUERY = """
SELECT DISTINCT u.distinct_id, e.type, count(e.event_id), u.email
FROM users AS u

LEFT JOIN events AS e
ON e.distinct_id = u.distinct_id

WHERE
    u.subscription_type = 'basic' AND
    u.email IS NOT NULL AND
    u.signup_at IS NOT NULL AND
    u.signup_at > to_date('2017-04-01', 'YYYY-MM-DD')


GROUP BY u.distinct_id, e.type, u.email
"""
