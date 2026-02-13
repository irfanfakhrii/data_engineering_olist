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
DATA_DIM_DATE = "/opt/airflow/data/processed/dim_date.csv"
DATA_DIM_CUSTOMER = "/opt/airflow/data/raw/olist_customers_dataset.csv"

# URI Database Postgres Airflow
DB_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

def extract_orders():
    # Perbaikan: Tambahkan pengecekan/pembuatan folder agar tidak error 'No such file or directory'
    os.makedirs("/opt/airflow/data/processed", exist_ok=True)
    
    if not os.path.exists(DATA_RAW):
        raise FileNotFoundError(f"File tidak ditemukan: {DATA_RAW}")

    df = pd.read_csv(DATA_RAW)
    df.to_csv(DATA_EXTRACT, index=False)

    print(f"Extract orders selesai: {len(df)} baris")
    return len(df)

def transform_customer():
    if not os.path.exists(DATA_EXTRACT):
        raise FileNotFoundError("File extract belum ada")
    
    df = pd.read_csv(DATA_EXTRACT)

def transform_customer():
    df = pd.read_csv(DATA_DIM_CUSTOMER)

    print("Daftar kolom:")
    print(df.columns.tolist())

    return

def load_dim_customer():
    if not os.path.exists(DATA_DIM_CUSTOMER):
        raise FileNotFoundError("File dim_customer belum ada")

    df = pd.read_csv(DATA_DIM_CUSTOMER)

    engine = create_engine(DB_URI)

    df = df[
        [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ]
    ]

    existing = pd.read_sql("SELECT customer_id FROM dim_customer", engine)
    existing_ids = set(existing["customer_id"])

    df_new = df[~df["customer_id"].isin(existing_ids)]

    if df_new.empty:
        print("Tidak ada customer baru")
        return

    df_new.to_sql(
        "dim_customer",
        engine,
        if_exists="append",
        index=False
    )

    print("Insert customer baru:", len(df_new))

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

    print(f"Transform orders selesai: {df.shape}")
    return df.shape

def transform_dim_date():
    df = pd.read_csv(DATA_PROCESSED)
    dates = pd.to_datetime(
    df["order_purchase_timestamp"]
    ).dt.date.dropna().unique()

    dim_date = pd.DataFrame({"date": dates})
    dim_date["date_id"] = dim_date["date"].astype(str).str.replace("-", "").astype(int)
    dim_date["year"] = pd.to_datetime(dim_date["date"]).dt.year
    dim_date["month"] = pd.to_datetime(dim_date["date"]).dt.month
    dim_date["day"] = pd.to_datetime(dim_date["date"]).dt.day

    dim_date.to_csv(DATA_DIM_DATE, index=False)

    print("Transform dim_date selesai:", dim_date.shape)

def load_dim_date():
    if not os.path.exists(DATA_DIM_DATE):
        raise FileNotFoundError("dim_date belum ada")

    df = pd.read_csv(DATA_DIM_DATE)

    engine = create_engine(DB_URI)
    
    # ambil date_id yang sudah ada
    existing = pd.read_sql("SELECT date_id FROM dim_date", engine)
    existing_ids = set(existing["date_id"])

    # filter hanya date_id baru
    df_new = df[~df["date_id"].isin(existing_ids)]

    if df_new.empty:
        print("dim_date tidak ada data baru")
        return
    
    # insert ulang data
    df.to_sql("dim_date", engine, if_exists="append", index=False)

    print("load dim_date selesai")

def load_orders():
    df = pd.read_csv(DATA_PROCESSED)
    df["date_id"] = pd.to_datetime(
        df["order_purchase_timestamp"]
    ).dt.strftime("%Y%m%d").astype(int)

    engine = create_engine(DB_URI)

    with engine.begin() as conn:
        conn.execute("TRUNCATE TABLE fact_orders;")
    
    df.to_sql(
        "fact_orders",
        engine,
        if_exists="append",
        index=False
    )
    print("Load fact_orders selesai")

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

    transform_customer_task = PythonOperator(
        task_id="transform_customer",
        python_callable=transform_customer
    )

    load_dim_customer_task = PythonOperator(
        task_id="load_dim_customer",
        python_callable=load_dim_customer
    )

    transform_task = PythonOperator(
        task_id="transform_orders",
        python_callable=transform_orders
    )

    transform_date_task = PythonOperator(
        task_id="transform_dim_date",
        python_callable=transform_dim_date
    )

    load_date_task = PythonOperator (
        task_id="load_dim_date",
        python_callable=load_dim_date
    )

    load_task = PythonOperator(
        task_id="load_orders",
        python_callable=load_orders
    )

    # Alur kerja (Dependency)
    extract_task >> transform_customer_task >> load_dim_customer_task >> transform_task >> transform_date_task >> load_date_task >> load_task
    
