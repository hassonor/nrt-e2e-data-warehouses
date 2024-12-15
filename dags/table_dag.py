from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from pinot_table_operator import PinotTableSubmitOperator

default_args = {
    "owner": "orhasson",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 12),
}

# Using Airflow Variables (if configured) to avoid hardcoding paths and URLs
folder_path = Variable.get(
    "pinot_schema_folder_path", default_var="/opt/airflow/dags/tables"
)
pinot_url = Variable.get(
    "pinot_schema_endpoint", default_var="http://pinot-controller:9000/tables"
)

with DAG(
    dag_id="table_dag",
    default_args=default_args,
    description="A DAG to submit all tables in a folder to Apache Pinot",
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,  # Ensure only one run at a time, if desired
    tags=["schema"],
) as dag:
    """
    # Schema DAG

    This DAG submits all JSON schema files from a specified folder to an Apache Pinot endpoint daily.

    **Operator Used:**
    - `PinotSchemaSubmitOperator`

    **Process:**
    1. Starts a DAG run daily.
    2. The operator reads all JSON schemas from `folder_path`.
    3. For each JSON file, validates and submits it to `pinot_url`.
    4. Logs successes and raises exceptions on failures.

    **Configuration:**
    - `folder_path` and `pinot_url` can be set via Airflow Variables named `pinot_schema_folder_path` and `pinot_schema_endpoint`.
    """

    start = EmptyOperator(
        task_id="start", doc_md="### Start Task\nThis task marks the start of the DAG."
    )

    submit_tables = PinotTableSubmitOperator(
        task_id="submit_tables",
        folder_path=folder_path,
        pinot_url=pinot_url,
        doc_md="""
        ### Submit Schemas Task
        This task uses the `PinotSchemaSubmitOperator` to:
        - Read JSON schema files from `folder_path`.
        - Validate each file's JSON content.
        - POST each schema to `pinot_url`.
        """,
    )

    end = EmptyOperator(
        task_id="end_task",
        doc_md="### End Task\nThis task marks the successful end of the DAG run.",
    )

    start >> submit_tables >> end
