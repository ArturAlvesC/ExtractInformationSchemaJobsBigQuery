SELECT
    job_id,
    job_type,
    statement_type,
    user_email,
    TRIM(query) AS query,
    DATETIME(creation_time) AS creation_time,
    DATETIME(start_time) AS start_time,
    DATETIME(end_time) AS end_time,
    DATETIME_DIFF(DATETIME(end_time), DATETIME(start_time), SECOND) AS duration_in_seconds,
    DATETIME_DIFF(DATETIME(end_time), DATETIME(start_time), MILLISECOND) AS duration_in_milliseconds,
    total_bytes_processed,
    total_bytes_billed,
    state,
    destination_table.project_id,
    destination_table.dataset_id,
    destination_table.table_id,
    total_bytes_billed / (1024102410241024) AS total_terabyte_billed,
    total_bytes_billed / (1024102410241024) * 6.25 AS total_job_cost
FROM
    region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT
-- Remover este comentário após primeira carga!
WHERE 
    CAST(creation_time AS DATE) = CAST("{{ ds }}" AS DATE);