SELECT
    ROW_NUMBER() OVER(ORDER BY statement_type) AS id,
    statement_type AS job_statement_type
FROM
    `bigquery_metadata.stg_information_schema`
WHERE
    statement_type IS NOT NULL
GROUP BY 2

UNION ALL 

SELECT
    -1 AS id,
    'N/A' AS statement_type;