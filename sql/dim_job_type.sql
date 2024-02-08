SELECT
    ROW_NUMBER() OVER(ORDER BY job_type) AS id,
    job_type
FROM
    `bigquery_metadata.stg_information_schema`
WHERE
    job_type IS NOT NULL
GROUP BY 2

UNION ALL 

SELECT
    -1 AS id,
    'N/A' AS job_type;