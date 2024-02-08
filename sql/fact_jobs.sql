SELECT
    DATE(j.creation_time) AS date,
    EXTRACT(HOUR FROM j.creation_time) AS hour,

    # DIMENSÕES
    IFNULL(t.id, -1) AS fk_type_id,
    IFNULL(s.id, -1) AS fk_statement_type_id,
    IFNULL(u.id, -1) AS fk_user_id,
    IFNULL(q.id, -1) AS fk_query_id,
    IFNULL(st.id, -1) AS fk_state_id,

    # MÉTRICAS JOB
    COUNT(DISTINCT job_id) AS qty_jobs,
    SUM(duration_in_seconds) AS duration_in_seconds,
    SUM(duration_in_milliseconds) AS duration_in_milliseconds,
    SUM(total_bytes_processed) AS total_bytes_processed,
    SUM(total_bytes_billed) AS total_bytes_billed,
    SUM(total_terabyte_billed) AS total_terabyte_billed,
    SUM(total_job_cost) AS total_job_cost
FROM
    `bigquery_metadata.stg_information_schema` j
LEFT JOIN
    `bigquery_metadata.dim_job_type` t ON j.job_type = t.job_type
LEFT JOIN
    `bigquery_metadata.dim_job_statement_type` s ON j.statement_type = s.job_statement_type
LEFT JOIN
    `bigquery_metadata.dim_job_user` u ON j.user_email = u.job_user_email
LEFT JOIN
    `bigquery_metadata.dim_job_query` q ON j.query = q.job_query
LEFT JOIN
    `bigquery_metadata.dim_job_state` st ON j.state = st.job_state
GROUP BY 1, 2, 3, 4, 5, 6, 7;