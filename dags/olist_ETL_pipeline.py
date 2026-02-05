from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os
from sqlalchemy import create_engine

# Path di dalam kontainer Docker Airflow
DATA_RAW = "/opt/airflow/data/raw/olist_orders_dataset.csv"
DATA_PROCESSED = "/opt/airflow/data/processed/orders_clean.csv"
DATA_EXTRACT = "/opt/airflow/data/processed/orders_extract.csv"

# URI Database Postgres Airflow
DB_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

def extract_orders():
    # Perbaikan: Tambahkan pengecekan/pembuatan folder agar tidak error 'No such file or directory'
    os.makedirs("/opt/airflow/data/processed", exist_ok=True)
    
    if not os.path.exists(DATA_RAW):
        raise FileNotFoundError(f"File tidak ditemukan: {DATA_RAW}")

    df = pd.read_csv(DATA_RAW)
    df.to_csv(DATA_EXTRACT, index=False)

    print(f"Extract selesai: {len(df)} baris")
    return len(df)

def transform_orders():
    if not os.path.exists(DATA_EXTRACT):
        raise FileNotFoundError(f"File hasil extract belum ada di {DATA_EXTRACT}")

    df = pd.read_csv(DATA_EXTRACT)
    
    # Memilih kolom yang diperlukan
    columns = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_delivered_customer_date"
    ]
    df = df[columns]

    # Konversi ke datetime
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])
    df["order_delivered_customer_date"] = pd.to_datetime(df["order_delivered_customer_date"])

    # Cleaning data yang kosong di kolom kunci
    df = df.dropna(subset=["order_id", "customer_id"])

    df.to_csv(DATA_PROCESSED, index=False)

    print(f"Transform selesai: {df.shape}")
    return df.shape

def load_orders():
    if not os.path.exists(DATA_PROCESSED):
        raise FileNotFoundError(f"Data hasil transform belum ada di {DATA_PROCESSED}")

    df = pd.read_csv(DATA_PROCESSED)
    
    # Membuat koneksi ke Postgres
    engine = create_engine(DB_URI)
    with engine.begin() as conn:
        conn.execute("TRUNCATE TABLE fact_orders;")

    df.to_sql(
    "fact_orders",
    engine,
    if_exists="append",
    index=False
    )

    print("Load ke PostgreSQL tanpa duplikat selesai")

# Definisi DAG
with DAG(
    dag_id="olist_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_orders
    )

    transform_task = PythonOperator(
        task_id="transform_orders",
        python_callable=transform_orders
    )

    load_task = PythonOperator(
        task_id="load_orders",
        python_callable=load_orders
    )

    # Alur kerja (Dependency)
    extract_task >> transform_task >> load_task
    
