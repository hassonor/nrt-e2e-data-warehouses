from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from kafka_operator import KafkaProduceOperator

# Set up default arguments
start_date = datetime(2024, 12, 14)
default_args = {
    'owner': 'orhasson',
    'depends_on_past': False,
    'start_date': start_date,
}

# Retrieve values from Airflow Variables
kafka_broker = Variable.get("kafka_broker_variable", default_var="kafka_broker:9092")
kafka_topic = Variable.get("kafka_topic_variable", default_var="transaction_facts")
num_records = Variable.get("num_records_variable", default_var="1000")

with DAG(
        dag_id='transaction_facts_generator',
        default_args=default_args,
        description='Transaction fact data generator into Kafka',
        schedule_interval=timedelta(days=1),
        catchup=False,  # Prevents backfill runs
        tags=['fact_data']
) as dag:
    """
    ### Transaction Facts Generator DAG

    This DAG:
    - Starts daily at the specified start_date.
    - Generates a specified number of random transaction records.
    - Sends these transaction records to a Kafka topic using the KafkaProduceOperator.
    """

    start = EmptyOperator(
        task_id='start_task',
        doc_md="### Start Task\nMarks the start of the DAG."
    )

    generate_txn_data = KafkaProduceOperator(
        task_id='generate_txn_fact_data',
        kafka_broker=kafka_broker,
        kafka_topic=kafka_topic,
        num_records=int(num_records),
        doc_md="""\
        ### Generate Transaction Fact Data
        This task produces `num_records` random transaction records and sends them to the specified Kafka topic.
        """
    )

    end = EmptyOperator(
        task_id='end_task',
        doc_md="### End Task\nMarks the end of the DAG."
    )

    start >> generate_txn_data >> end
