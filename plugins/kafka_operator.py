import json
from datetime import datetime, timedelta
import random
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaProducer


class KafkaProduceOperator(BaseOperator):
    @apply_defaults
    def __init__(self, kafka_broker, kafka_topic, num_records=100, compression: bool = True, *args, **kwargs):
        super(KafkaProduceOperator, self).__init__(*args, **kwargs)
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.num_records = num_records

        # Optional compression for performance
        self.compression_type = 'gzip' if compression else None

    def generate_transaction_data(self, row_num):
        # Generate lists of IDs
        customer_id_list = [f"C{str(i).zfill(5)}" for i in range(1, self.num_records + 1)]
        account_id_list = [f"A{str(i).zfill(5)}" for i in range(1, self.num_records + 1)]
        branch_id_list = [f"B{str(i).zfill(5)}" for i in range(1, self.num_records + 1)]

        # Choose one ID from each list randomly
        customer_id = random.choice(customer_id_list)
        account_id = random.choice(account_id_list)
        branch_id = random.choice(branch_id_list)

        # Generate transaction details
        transaction_types = ['Credit', 'Debit', 'Transfer', 'Withdrawal', 'Deposit']
        currencies = ['USD', 'GBP', 'EUR', 'NIS']

        transaction_id = f'T{str(row_num).zfill(6)}'
        transaction_date = int((datetime.now() - timedelta(days=random.randint(0, 365))).timestamp() * 1000)
        transaction_type = random.choice(transaction_types)
        currency = random.choice(currencies)
        transaction_amount = round(random.uniform(10.0, 10000.0), 2)
        exchange_rate = round(random.uniform(0.5, 1.5), 4)

        transaction = {
            "transaction_id": transaction_id,
            "transaction_date": transaction_date,
            "account_id": account_id,
            "customer_id": customer_id,
            "transaction_type": transaction_type,
            "currency": currency,
            "branch_id": branch_id,
            "transaction_amount": transaction_amount,
            "exchange_rate": exchange_rate
        }

        return transaction

    def execute(self, context) -> Any:
        producer = KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            compression_type=self.compression_type,
            linger_ms=10,
            batch_size=32768
        )

        for row_num in range(1, self.num_records + 1):
            transaction = self.generate_transaction_data(row_num)
            producer.send(self.kafka_topic, transaction)
            self.log.info(f'Sent transaction: {transaction}')

        producer.flush()
        self.log.info(f'{self.num_records} transaction records have been sent to Kafka topic "{self.kafka_topic}".')
