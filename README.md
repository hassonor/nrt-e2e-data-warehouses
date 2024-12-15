# Near Real-Time End-to-End Data Warehouses (`nrt-e2e-data-warehouses`)

This project demonstrates a near real-time data pipeline using Apache Pinot, Apache Superset, Apache Airflow, and
Redpanda
Kafka for ingesting and querying dimension and fact data.

---

## **Getting Started**

### **Prerequisites**

Ensure you have the following installed:

- Docker and Docker Compose
- A web browser for accessing the services

---

## **Setup Instructions**

### **Step 1: Start the Environment**

Run the following command to build and start all services:

```bash
docker compose up --build -d
```

---

## **Access Services**

- **Airflow**: [http://localhost:8180](http://localhost:8180)
    - Login with: `airflow` / `airflow`
- **Redpanda Console**: [http://localhost:8080](http://localhost:8080)
- **Apache Pinot**: [http://localhost:9091](http://localhost:9091)
- **Apache Superset**: [http://localhost:8088](http://localhost:8088)
    - Login with: `admin` / `admin`

---

## **Steps to Run the Pipeline**

### Step 1: Generate Dimension Data

1. In **Airflow**, run the following DAGs **once** to generate dimension data:
    - `account_dim_generator`
    - `branch_dim_generator`
    - `customer_dim_generator`
    - `date_dim_generator`

---

### Step 2: Generate Transaction Facts

1. Run the `transaction_facts_generator` DAG **once**.
2. Navigate to **Redpanda Console** and confirm the `transaction_facts` topic has been created.

---

### Step 3: Configure Pinot

1. Run the following Airflow DAGs **in sequence**:
    - `schema_dag`
    - `table_dag`
    - `load_dag`
2. Navigate to **Apache Pinot**, click on `Tables` and `Query Console`, and verify that data has been consumed from
   Kafka.

---

### Step 4: Configure Superset

1. Navigate to **Apache Superset** at [http://localhost:8088](http://localhost:8088).
    - Login with: `admin` / `admin`
2. Connect a database:
    - Go to `+ -> Data -> Connect a database`.
    - Select `Apache Pinot` as the database.
    - Fill in the **SQLAlchemy URI**:
      ```
      pinot://pinot-broker:8099/query?server=http://pinot-controller:9000
      ```
    - Click `TEST CONNECTION` and ensure "Looks good".
    - Press `Connect`.

---

### Step 5: Query the Data

1. Go to **SQL Lab** > `SQL LAB`.
    - Choose a table and view its schema with `SEE TABLE SCHEMA`.
2. Run the following query in SQL Lab:
   ```sql
   SELECT
     tf.*,
     CONCAT(cd.first_name, ' ', cd.last_name) AS full_name,
     email,
     phone_number,
     registration_date,
     branch_name,
     branch_address,
     city,
     state,
     zipcode,
     account_type,
     status,
     balance
   FROM
     transaction_facts tf
     LEFT JOIN account_dim ad ON tf.account_id = ad.account_id
     LEFT JOIN customer_dim cd ON tf.customer_id = cd.customer_id
     LEFT JOIN branch_dim bd ON tf.branch_id = bd.branch_id;
   ```

3. Save the query results as a dataset:
    - Click `Save Dropdown` > `Save as New`.
    - Name the dataset: **`transaction_fact_combined`**.
    - Click `SAVE & EXPLORE`.

---

### Step 6: Create a Superset Dashboard

#### **Charts to Add:**

1. **Bar Chart**:
    - **X-Axis**: `branch_name`
    - **Metrics**: `transaction_amount (SUM)`
    - **Row Limit**: 10
    - Name the chart: **`Top 10 Profitable Branches`**.
    - Save it to the dashboard.

2. **Big Number Chart**:
    - Metric: **`Count`**
    - Name: **`Total Records`**.
    - Save it to the dashboard.

3. **Pie Chart (Currency Distribution)**:
    - **Dimensions**: `currency`
    - **Metrics**: `transaction_amount`
    - Name: **`Currency Distribution`**.
    - Save it to the dashboard.

4. **Pie Chart (Account Type Distribution)**:
    - **Dimensions**: `account_type`
    - **Metrics**: `transaction_amount`
    - Name: **`Account Type Distribution`**.
    - Under `CUSTOMIZE`, enable:
        - `SHOW TOTAL`
        - Currency Format: `NIS`
    - Save it to the dashboard.

#### **Dashboard Settings**:

1. Go to the dashboard, click `...` > `Edit`.
2. Set the dashboard refresh rate to **10 seconds**.

---

### Step 7: Watch Real-Time Updates

1. In Airflow, run the `transaction_facts_generator` DAG to simulate new transactions.
2. Navigate to **Superset** > Dashboard.
    - Watch as the values update dynamically with the new data from Kafka!

---

### **Production Notes**

- For production, scale Kafka with more brokers to ensure reliability and high throughput.
- And Better way more the csvs from airflow to pinot.

---

### **Services Overview**

- **Airflow**: Orchestrates data pipelines ([http://localhost:8180](http://localhost:8180)).
- **Redpanda (Kafka)**: Real-time message broker ([http://localhost:8080](http://localhost:8080)).
- **Pinot**: OLAP datastore for real-time analytics ([http://localhost:9091](http://localhost:9091)).
- **Superset**: Data visualization and dashboarding ([http://localhost:8088](http://localhost:8088)).

---
