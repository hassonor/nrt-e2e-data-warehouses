import os
import random
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# DAG Configuration
start_date = datetime(2024, 12, 12)
default_args = {
    'owner': 'orhasson',
    'depends_on_past': False,
    'backfill': False,
}

# Parameters
num_rows = 50  # Number of rows to generate
output_file = '/opt/airflow/csvs/branch_dim_large_data.csv'

# Sample data for realistic generation
cities = ["London", "Manchester", "Birmingham", "Glasgow", "Edinburgh"]
regions = ["London", "Greater Manchester", "West Midlands", "Scotland", "Scotland"]
postcodes = ["EC1A 1BB", "M1 1AE", "B1 1AA", "G1 1AA", "EH1 1AA"]
street_types = ['High St', 'King St', 'Queen St', 'Church Rd', 'Market St']


def generate_random_data(row_num):
    """Generate random data for a single branch record."""
    try:
        branch_id = f"B{row_num:05d}"
        branch_name = f"Branch {row_num}"
        branch_address = f"{random.randint(1, 999)} {random.choice(street_types)}"
        city = random.choice(cities)
        region = random.choice(regions)
        postcode = random.choice(postcodes)

        # Generate opening date in milliseconds
        now = datetime.now()
        random_date = now - timedelta(days=random.randint(0, 3650))  # Random date within the last 10 years
        opening_date_millis = int(random_date.timestamp() * 1000)

        return branch_id, branch_name, branch_address, city, region, postcode, opening_date_millis
    except Exception as e:
        print(f"Error generating random data for row {row_num}: {str(e)}")
        raise


def generate_branch_dim_data():
    """Generate branch dimension data and save to CSV."""
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Initialize lists inside the function
        branch_ids = []
        branch_names = []
        branch_addresses = []
        cities_list = []
        regions_list = []
        postcodes_list = []
        opening_dates = []

        # Generate data
        row_num = 1
        while row_num <= num_rows:
            data = generate_random_data(row_num)
            branch_ids.append(data[0])
            branch_names.append(data[1])
            branch_addresses.append(data[2])
            cities_list.append(data[3])
            regions_list.append(data[4])
            postcodes_list.append(data[5])
            opening_dates.append(data[6])
            row_num += 1

        # Create DataFrame
        df = pd.DataFrame({
            "branch_id": branch_ids,
            "branch_name": branch_names,
            "branch_address": branch_addresses,
            "city": cities_list,
            "region": regions_list,
            "postcode": postcodes_list,
            "opening_date": opening_dates
        })

        # Save DataFrame to CSV
        df.to_csv(output_file, index=False)

        print(f"CSV file '{output_file}' with {num_rows} rows has been generated successfully.")

        # Print sample data for verification
        print("\nSample data:")
        print(df.head())

    except Exception as e:
        print(f"Error generating branch dimension data: {str(e)}")
        raise


# DAG Definition
with DAG('branch_dim_generator',
         default_args=default_args,
         description='Generate large branch dimension data CSV file',
         schedule_interval=timedelta(days=1),
         start_date=start_date,
         tags=['dimension']) as dag:
    # Task 1: Start
    start = EmptyOperator(
        task_id='start_task'
    )

    # Task 2: Generate Branch Dimension Data
    generate_branch_dim_data = PythonOperator(
        task_id='generate_branch_dim_data',
        python_callable=generate_branch_dim_data
    )

    # Task 3: End
    end = EmptyOperator(
        task_id='end_task'
    )

    # Define task dependencies
    start >> generate_branch_dim_data >> end
