CHURN_STATS_QUERY = """
SELECT DISTINCT
    count(likely.distinct_id) AS churn_likely,
    count(possible.distinct_id) AS churn_possible,
    count(unlikely.distinct_id) AS churn_unlikely
FROM churns
LEFT JOIN (
    SELECT distinct_id
    FROM churns
    WHERE churn_proba > 0.85
) AS likely
ON churns.distinct_id = likely.distinct_id

LEFT JOIN (
    SELECT distinct_id
    FROM churns
    WHERE churn_proba < 0.85 AND churn_proba > 0.5
) AS possible
ON churns.distinct_id = possible.distinct_id

LEFT JOIN (
    SELECT distinct_id
    FROM churns
    WHERE churn_proba < 0.5
) AS unlikely
ON unlikely.distinct_id = churns.distinct_id
"""

CONVERSION_STATS_QUERY = """
SELECT DISTINCT
    count(likely.distinct_id) AS conversion_likely,
    count(possible.distinct_id) AS conversion_possible,
    count(unlikely.distinct_id) AS conversion_unlikely
FROM conversions
LEFT JOIN (
    SELECT distinct_id
    FROM conversions
    WHERE conversion_proba > 0.85
) AS likely
ON conversions.distinct_id = likely.distinct_id

LEFT JOIN (
    SELECT distinct_id
    FROM conversions
    WHERE conversion_proba < 0.85 AND conversion_proba > 0.5
) AS possible
ON conversions.distinct_id = possible.distinct_id

LEFT JOIN (
    SELECT distinct_id
    FROM conversions
    WHERE conversion_proba < 0.5
) AS unlikely
ON conversions.distinct_id = unlikely.distinct_id
"""
