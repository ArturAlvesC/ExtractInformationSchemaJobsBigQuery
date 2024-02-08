SELECT
    ROW_NUMBER() OVER(ORDER BY user_email) AS id,
    user_email AS job_user_email,
    useful_functions.propper_string(REPLACE(REPLACE(SPLIT(user_email, '@')[SAFE_OFFSET(0)], '.', ' '), '-', ' ')) AS job_user_name,
FROM
    `bigquery_metadata.stg_information_schema`
WHERE
    user_email IS NOT NULL
GROUP BY 2

UNION ALL 

SELECT
    -1 AS id,
    'N/A' AS job_user_email,
    'N/A' AS job_user_name;