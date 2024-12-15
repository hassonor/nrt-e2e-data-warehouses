import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# DAG Configuration
start_date = datetime(2024, 12, 12)
default_args = {"owner": "orhasson", "depends_on_past": False}

# Parameters
start_year = 2020  # Start year for date generation
end_year = 2025  # End year for date generation
output_file = "/opt/airflow/csvs/date_dim_large_data.csv"


def generate_date_attributes(date):
    """
    Generate date attributes for the date dimension:

    Fields:
    - date_id (BIGINT TIMESTAMP in milliseconds)
    - month (INT)
    - day (INT)
    - year (INT)
    - quarter (INT)
    """
    # Convert datetime to Unix timestamp in milliseconds
    date_id = int(date.timestamp() * 1000)

    # Extract date components
    year = date.year
    month = date.month
    day = date.day

    # Calculate quarter (1-4)
    quarter = (month - 1) // 3 + 1

    return {
        "date_id": date_id,
        "month": month,
        "day": day,
        "year": year,
        "quarter": quarter,
    }


def generate_date_dim_data():
    """Generate date dimension data for the specified date range."""
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        dates_data = []
        current_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31)

        # Generate data for each date in the range
        while current_date <= end_date:
            date_attributes = generate_date_attributes(current_date)
            dates_data.append(date_attributes)
            current_date += timedelta(days=1)

        # Create DataFrame
        df = pd.DataFrame(dates_data)

        # Set proper dtypes
        df = df.astype(
            {
                "date_id": "int64",  # Use int64 for large timestamp values
                "month": "int32",
                "day": "int32",
                "year": "int32",
                "quarter": "int32",
            }
        )

        # Save DataFrame to CSV
        df.to_csv(output_file, index=False)

        print(
            f"CSV file '{output_file}' with {len(dates_data)} rows has been generated successfully."
        )
        print(f"Date range: {start_year}-01-01 to {end_year}-12-31")

    except Exception as e:
        print(f"Error generating date dimension data: {str(e)}")
        raise


# DAG Definition
with DAG(
    "date_dim_generator",
    default_args=default_args,
    description="Generate date dimension data in CSV file",
    schedule_interval=timedelta(days=1),
    start_date=start_date,
    catchup=False,
    tags=["dimension"],
) as dag:
    # Task 1: Start
    start = EmptyOperator(task_id="start_task")

    # Task 2: Generate Date Dimension Data
    generate_date_dim_data = PythonOperator(
        task_id="generate_date_dim_data", python_callable=generate_date_dim_data
    )

    # Task 3: End
    end = EmptyOperator(task_id="end_task")

    # Define task dependencies
    start >> generate_date_dim_data >> end
