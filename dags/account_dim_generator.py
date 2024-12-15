import os
import random
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# DAG Configuration
start_date = datetime(2024, 12, 12)
default_args = {"owner": "orhasson", "depends_on_past": False}

# Parameters
num_rows = 1000
output_file = "/opt/airflow/csvs/account_dim_large_data.csv"


def generate_random_data(row_num):
    """Generate random data for a single account record."""
    try:
        account_id = f"A{row_num:05d}"
        account_type = random.choice(["SAVINGS", "CHECKING"])
        status = random.choice(["ACTIVE", "NOTACTIVE"])
        customer_id = f"C{random.randint(1, 1000):05d}"
        balance = round(random.uniform(100.00, 10000.00), 2)

        now = datetime.now()
        random_date = now - timedelta(days=random.randint(1, 365))
        opening_dates_mills = int(random_date.timestamp() * 1000)

        return (
            account_id,
            account_type,
            status,
            customer_id,
            balance,
            opening_dates_mills,
        )
    except Exception as e:
        print(f"Error generating random data for row {row_num}: {str(e)}")
        raise


def generate_account_dim_data():
    """Generate account dimension data and save to CSV."""
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Initialize lists inside the function
        account_ids = []
        account_types = []
        statuses = []
        customer_ids = []
        balances = []
        opening_dates = []

        # Generate data
        row_num = 1
        while row_num <= num_rows:
            (
                account_id,
                account_type,
                status,
                customer_id,
                balance,
                opening_dates_mills,
            ) = generate_random_data(row_num)
            account_ids.append(account_id)
            account_types.append(account_type)
            statuses.append(status)
            customer_ids.append(customer_id)
            balances.append(balance)
            opening_dates.append(opening_dates_mills)
            row_num += 1

        # Create DataFrame
        df = pd.DataFrame(
            {
                "account_id": account_ids,
                "account_type": account_types,
                "status": statuses,
                "customer_id": customer_ids,
                "balance": balances,
                "opening_dates": opening_dates,
            }
        )

        # Save DataFrame to CSV
        df.to_csv(output_file, index=False)

        print(
            f"CSV file {output_file} with {num_rows} rows has been generated successfully."
        )

        # Print sample data for verification
        print("\nSample data:")
        print(df.head())

    except Exception as e:
        print(f"Error generating account dimension data: {str(e)}")
        raise


# DAG Definition
with DAG(
    "account_dim_generator",
    default_args=default_args,
    description="Generate large account dimension data in a CSV file",
    schedule_interval=timedelta(days=1),
    start_date=start_date,
    catchup=False,
    tags=["dimension"],
) as dag:
    # Task 1: Start
    start = EmptyOperator(task_id="start_task")

    # Task 2: Generate Account Dimension Data
    generate_account_dimension_data = PythonOperator(
        task_id="generate_account_dim_data", python_callable=generate_account_dim_data
    )

    # Task 3: End
    end = EmptyOperator(task_id="end_task")

    # Define task dependencies
    start >> generate_account_dimension_data >> end
