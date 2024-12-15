import json
import random
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context
from kafka import KafkaProducer


class KafkaProduceOperator(BaseOperator):
    """
    Airflow Operator to produce random transaction data to a specified Kafka topic.

    This operator:
    - Generates a specified number of random transaction records.
    - Sends each record to a Kafka topic using the provided Kafka broker.
    - Provides detailed logging and error handling to aid in debugging issues.

    :param kafka_broker: Kafka broker address (e.g., 'localhost:9092').
    :param kafka_topic: Kafka topic to which messages should be sent.
    :param num_records: Number of transaction records to produce.
    :param compression: Whether to enable gzip compression for the producer.
    """

    template_fields = ("kafka_broker", "kafka_topic", "num_records")

    def __init__(
        self,
        kafka_broker: str,
        kafka_topic: str,
        num_records: int = 100,
        compression: bool = True,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.num_records = num_records
        self.compression_type = "gzip" if compression else None

    def generate_transaction_data(self, row_num: int) -> Dict[str, Any]:
        """
        Generate a single transaction record with randomized details.

        The record includes:
        - IDs for customer, account, branch
        - A transaction type and currency
        - A random amount and exchange rate
        - A timestamp representing a transaction date within the past year
        """
        customer_id_list = [f"C{i:05d}" for i in range(1, self.num_records + 1)]
        account_id_list = [f"A{i:05d}" for i in range(1, self.num_records + 1)]
        branch_id_list = [f"B{i:05d}" for i in range(1, self.num_records + 1)]

        customer_id = random.choice(customer_id_list)
        account_id = random.choice(account_id_list)
        branch_id = random.choice(branch_id_list)

        transaction_types = ["Credit", "Debit", "Transfer", "Withdrawal", "Deposit"]
        currencies = ["USD", "GBP", "EUR", "NIS"]

        transaction_id = f"T{row_num:06d}"
        transaction_date = int(
            (datetime.now() - timedelta(days=random.randint(0, 365))).timestamp() * 1000
        )
        transaction_type = random.choice(transaction_types)
        currency = random.choice(currencies)
        transaction_amount = round(random.uniform(10.0, 10000.0), 2)
        exchange_rate = round(random.uniform(0.5, 1.5), 4)

        return {
            "transaction_id": transaction_id,
            "transaction_date": transaction_date,
            "account_id": account_id,
            "customer_id": customer_id,
            "transaction_type": transaction_type,
            "currency": currency,
            "branch_id": branch_id,
            "transaction_amount": transaction_amount,
            "exchange_rate": exchange_rate,
        }

    def execute(self, context: Context) -> Any:
        """
        Send the specified number of transaction records to the Kafka topic.

        This method:
        - Initializes a KafkaProducer with the given configuration.
        - Iterates over the required number of records, generating and sending each one.
        - Logs detailed debug information and handles exceptions gracefully.
        - Ensures the KafkaProducer is flushed and closed even if an error occurs.

        :param context: Airflow execution context.
        :return: A summary string indicating how many records were successfully submitted.
        :raises AirflowException: If sending any transaction fails.
        """
        producer = KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            compression_type=self.compression_type,
            linger_ms=10,
            batch_size=32768,
        )

        try:
            for row_num in range(1, self.num_records + 1):
                transaction = self.generate_transaction_data(row_num)
                # Log the full transaction at debug level for troubleshooting.
                self.log.debug("Generated transaction: %s", transaction)

                try:
                    producer.send(self.kafka_topic, transaction)
                    # Log only the transaction_id at info level for cleaner daily logs.
                    self.log.info(
                        "Sent transaction ID: %s", transaction["transaction_id"]
                    )
                except Exception as e:
                    self.log.exception(
                        "Error sending transaction %s", transaction["transaction_id"]
                    )
                    raise AirflowException(
                        f"Error sending transaction {transaction['transaction_id']}"
                    ) from e
        finally:
            producer.flush()
            producer.close()

        self.log.info(
            '%d transaction records have been sent to Kafka topic "%s".',
            self.num_records,
            self.kafka_topic,
        )
        return f"Submitted {self.num_records} transaction(s) successfully."
