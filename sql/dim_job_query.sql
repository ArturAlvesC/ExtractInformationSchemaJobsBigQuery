SELECT
    ROW_NUMBER() OVER(ORDER BY query) AS id,
    query AS job_query,
    CONCAT(ANY_VALUE(dataset_id), '.', ANY_VALUE(table_id)) AS job_destination_dataset_table,
    IF(LEFT(ANY_VALUE(dataset_id), 1) = '_', 'EXIBIÇÃO', 'TRANSFERÊNCIA') AS job_query_type,
    IF(LEFT(ANY_VALUE(dataset_id), 1) = '_', 'temporary_dataset', ANY_VALUE(dataset_id))  AS normalized_dataset,
    IF(LEFT(ANY_VALUE(table_id), 4) = 'anon', 'temporary_table', ANY_VALUE(table_id))  AS normalized_table
FROM
    `bigquery_metadata.stg_information_schema`
WHERE
    query IS NOT NULL
    AND project_id IS NOT NULL
GROUP BY 2

UNION ALL 

SELECT
    -1 AS id,
    'N/A' AS job_query,
    'N/A' AS job_destination_dataset_table,
    'N/A' AS job_query_type,
    'N/A' AS normalized_dataset,
    'N/A' AS normalized_table;