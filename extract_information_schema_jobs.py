from __future__ import annotations

import pendulum
import datetime

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = ""
DATASET_ID = "bigquery_metadata"
DATE = '{{ ds }}'

with DAG(
    dag_id="extract_information_schema_jobs",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["bigquery-metadata"],
    params={},
) as dag:

    fact_jobs = BigQueryInsertJobOperator(
        task_id="fact_jobs",
        configuration={
            "query": {
                "query": "sql/fact_jobs.sql",
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": "fact_jobs"
                }
            }
        }
    )

    dim_job_query = BigQueryInsertJobOperator(
        task_id="dim_job_query",
        configuration={
            "query": {
                "query": "sql/dim_job_query.sql",
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": "dim_job_query"
                }
            }
        }
    )

    
    dim_job_statement_type = BigQueryInsertJobOperator(
        task_id="dim_job_statement_type",
        configuration={
            "query": {
                "query": "sql/dim_job_statement_type.sql",
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": "dim_job_statement_type"
                }
            }
        }
    )

    dim_job_user = BigQueryInsertJobOperator(
        task_id="dim_job_user",
        configuration={
            "query": {
                "query": "sql/dim_job_user.sql",
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": "dim_job_user"
                }
            }
        }
    )

    dim_job_state = BigQueryInsertJobOperator(
        task_id="dim_job_state",
        configuration={
            "query": {
                "query": "sql/dim_job_state.sql",
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": "dim_job_state"
                }
            }
        }
    )

    dim_job_type = BigQueryInsertJobOperator(
        task_id="dim_job_type",
        configuration={
            "query": {
                "query": "sql/dim_job_type.sql",
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": "dim_job_type"
                }
            }
        }
    )

    # writeDisposition = WRITE_APPEND
    integrate_stg_information_schema = BigQueryInsertJobOperator(
        task_id="integrate_stg_information_schema",
        configuration={
            "query": {
                "query": "sql/stg_information_schema.sql",
                "useLegacySql": False,
                "writeDisposition": "WRITE_APPEND",
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": "stg_information_schema"
                }
            }
        }
    )

    clear_stg_information_schema = BigQueryInsertJobOperator(
        task_id="clear_stg_information_schema",
        configuration={
            "query": {
            "query": f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.stg_information_schema` WHERE CAST(creation_time AS DATE) = CAST('{DATE}' AS DATE);",
            "useLegacySql": False,
            }
        }
    )

    clear_stg_information_schema >> integrate_stg_information_schema

    integrate_stg_information_schema >> dim_job_state
    integrate_stg_information_schema >> dim_job_type
    integrate_stg_information_schema >> dim_job_statement_type
    integrate_stg_information_schema >> dim_job_query
    integrate_stg_information_schema >> dim_job_user

    dim_job_state >> fact_jobs
    dim_job_type >> fact_jobs
    dim_job_statement_type >> fact_jobs
    dim_job_query >> fact_jobs
    dim_job_user >> fact_jobs