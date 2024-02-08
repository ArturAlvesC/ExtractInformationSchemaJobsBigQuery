SELECT
    ROW_NUMBER() OVER(ORDER BY state) AS id,
    state AS job_state
FROM
    `bigquery_metadata.stg_information_schema`
WHERE
    state IS NOT NULL
GROUP BY 2

UNION ALL 

SELECT
    -1 AS id,
    'N/A' AS job_state;