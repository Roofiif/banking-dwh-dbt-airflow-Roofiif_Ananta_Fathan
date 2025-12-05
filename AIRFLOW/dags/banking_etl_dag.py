from datetime import datetime, timedelta
import os
import csv

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
# ============================================================================
# CONSTANTS AND PATHS
# ============================================================================

BASE_DIR = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
DBT_PROJECT_DIR = os.path.join(BASE_DIR, "dbt", "banking_dw")
DUCKDB_PATH = os.path.join(DBT_PROJECT_DIR, "dbt_banking_dw.duckdb")
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")


# ============================================================================
# TASK FUNCTIONS
# ============================================================================

def extract_data_func(**context):
    import duckdb
    
    print("=" * 80)
    print("TASK 1: EXTRACT DATA")
    print("=" * 80)
    
    os.makedirs(DATA_DIR, exist_ok=True)
    
    print(f"Connecting to DuckDB: {DUCKDB_PATH}")
    con = duckdb.connect(DUCKDB_PATH)
    
    query = """
        SELECT
            transaction_id,
            account_id,
            transaction_date,
            transaction_time,
            transaction_type,
            amount,
            balance_after,
            merchant_name,
            merchant_category,
            channel,
            location,
            status
        FROM main.transactions
    """
    
    print("Executing query...")
    df = con.execute(query).fetch_df()
    con.close()
    
    print(f"✓ Successfully extracted {len(df)} records")
    print(f"✓ Columns: {list(df.columns)}")

    # ==========================================
    # FIX: konversi datetime ke string dulu
    # ==========================================
    # Kalau tahu nama kolomnya:
    if "transaction_date" in df.columns:
        # contoh format: 2025-01-31
        df["transaction_date"] = df["transaction_date"].astype(str)

    if "transaction_time" in df.columns:
        df["transaction_time"] = df["transaction_time"].astype(str)

    # Atau lebih generik, untuk semua kolom datetime:
    # import numpy as np
    # for col in df.columns:
    #     if np.issubdtype(df[col].dtype, np.datetime64):
    #         df[col] = df[col].astype(str)

    # Setelah semua datetime jadi string, baru diubah ke list of dict
    records = df.to_dict(orient="records")

    context["ti"].xcom_push(
        key="raw_transactions",
        value=records,
    )
    
    print(f"✓ Data pushed to XCom with key 'raw_transactions'")
    print("=" * 80)


def save_raw_csv_func(**context):
    """
    Task 2: save_raw_csv
    
    Menyimpan data mentah yang diambil dari task extract_data
    ke folder data/raw dalam format CSV.
    
    Output:
        data/raw/transactions_raw.csv
    """
    print("=" * 80)
    print("TASK 2: SAVE RAW CSV")
    print("=" * 80)
    
    # Pastikan direktori raw ada
    os.makedirs(RAW_DIR, exist_ok=True)
    
    # Ambil data dari XCom
    print("Pulling data from XCom...")
    records = context["ti"].xcom_pull(
        key="raw_transactions",
        task_ids="extract_data",
    )
    
    if not records:
        raise ValueError("Tidak ada data yang diambil pada task extract_data")
    
    print(f"✓ Retrieved {len(records)} records from XCom")
    
    # Simpan ke CSV
    output_path = os.path.join(RAW_DIR, "transactions_raw.csv")
    fieldnames = list(records[0].keys())
    
    print(f"Writing to: {output_path}")
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    
    print(f"✓ Successfully saved {len(records)} records to {output_path}")
    print(f"✓ File size: {os.path.getsize(output_path)} bytes")
    print("=" * 80)


def validate_output_func(**context):
    """
    Task 4: validate_output
    
    Melakukan validasi hasil transformasi:
    - Cek apakah file raw ada
    - Cek jumlah record > 0
    - Cek kolom transaction_id tidak null/kosong
    - Simpan snapshot data yang sudah tervalidasi
    
    Output:
        data/processed/transactions_validated_snapshot.csv
    """
    print("=" * 80)
    print("TASK 4: VALIDATE OUTPUT")
    print("=" * 80)
    
    # Pastikan direktori processed ada
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    
    # Cek file raw ada
    raw_path = os.path.join(RAW_DIR, "transactions_raw.csv")
    print(f"Checking file: {raw_path}")
    
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"File raw tidak ditemukan: {raw_path}")
    
    print("✓ File exists")
    
    # Baca dan validasi data
    print("Reading and validating data...")
    rows = []
    with open(raw_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    
    # Validasi 1: Jumlah record > 0
    if len(rows) == 0:
        raise ValueError("Validasi gagal: tidak ada record di transactions_raw.csv")
    
    print(f"✓ Total records: {len(rows)}")
    
    # Validasi 2: transaction_id tidak null/kosong
    null_count = 0
    for idx, row in enumerate(rows, start=1):
        tx_id = row.get("transaction_id")
        if tx_id is None or str(tx_id).strip() == "":
            null_count += 1
            print(f"✗ Warning: transaction_id kosong pada baris ke-{idx}")
    
    if null_count > 0:
        raise ValueError(
            f"Validasi gagal: ditemukan {null_count} transaction_id yang kosong"
        )
    
    print("✓ All transaction_id values are valid (not null/empty)")
    
    # Validasi tambahan: Cek kolom penting
    required_columns = [
        "transaction_id", "account_id", "transaction_date",
        "amount", "transaction_type"
    ]
    
    if rows:
        available_columns = set(rows[0].keys())
        missing_columns = set(required_columns) - available_columns
        
        if missing_columns:
            raise ValueError(
                f"Validasi gagal: kolom yang hilang: {missing_columns}"
            )
        
        print(f"✓ All required columns present: {required_columns}")
    
    # Simpan snapshot data yang tervalidasi
    validated_path = os.path.join(
        PROCESSED_DIR, "transactions_validated_snapshot.csv"
    )
    
    print(f"Saving validated snapshot to: {validated_path}")
    fieldnames = list(rows[0].keys())
    with open(validated_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"✓ Validated snapshot saved: {validated_path}")
    print(f"✓ File size: {os.path.getsize(validated_path)} bytes")
    print("=" * 80)
    print("✓✓✓ ALL VALIDATIONS PASSED ✓✓✓")
    print("=" * 80)


# ============================================================================
# DAG CONFIGURATION
# ============================================================================

default_args = {
    "owner": "ropip",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="banking_etl_dag",
    default_args=default_args,
    description="ETL pipeline for banking transactions with dbt transformation",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["exam", "banking", "dbt", "etl"],
    max_active_runs=1,
) as dag:

    # Task 1: Extract data from DuckDB
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data_func,
    )

    # Task 2: Save raw data to CSV
    save_raw_csv = PythonOperator(
        task_id="save_raw_csv",
        python_callable=save_raw_csv_func,
    )

    # Task 3: Run dbt transformation
    run_dbt_transform = BashOperator(
        task_id="run_dbt_transform",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run --profiles-dir {DBT_PROJECT_DIR}
        """,
    )

    # Task 4: Validate output
    validate_output = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output_func,
    )

    # Define task dependencies
    extract_data >> save_raw_csv >> run_dbt_transform >> validate_output