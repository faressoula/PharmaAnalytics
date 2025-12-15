import pandas as pd
import mysql.connector
import os

# ----------------------
# PATHS
# ----------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")

os.makedirs(PROCESSED_DIR, exist_ok=True)

# ----------------------
# MYSQL CONNECTION
# ----------------------
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",  # adjust if needed
    database="senderra_analytics"
)
cursor = conn.cursor()

# ----------------------
# TRANSFORM FUNCTIONS
# ----------------------
def transform_patients(df):
    # FIX: column mismatch
    if "condition" in df.columns:
        df = df.rename(columns={"condition": "condition_name"})

    df["state"] = df["state"].str.upper()
    df["enrollment_date"] = pd.to_datetime(df["enrollment_date"])

    return df[
        ["patient_id", "enrollment_date", "condition_name", "insurance_type", "state"]
    ]

def transform_prescribers(df):
    return df[
        ["prescriber_id", "clinic_name", "specialty", "region", "assigned_sales_rep"]
    ]

def transform_prescriptions(df):
    df["created_date"] = pd.to_datetime(df["created_date"])
    df["approval_date"] = pd.to_datetime(df["approval_date"], errors="coerce")
    df["refill_flag"] = df["refill_flag"].astype(int)

    return df[
        [
            "rx_id",
            "patient_id",
            "prescriber_id",
            "medication_name",
            "created_date",
            "approval_date",
            "refill_flag",
            "status",
        ]
    ]

def transform_shipments(df):
    df["shipped_date"] = pd.to_datetime(df["shipped_date"])
    df["delivered_date"] = pd.to_datetime(df["delivered_date"], errors="coerce")
    df["delay_flag"] = df["delay_flag"].astype(int)
    df["return_flag"] = df["return_flag"].astype(int)

    return df[
        [
            "shipment_id",
            "rx_id",
            "shipped_date",
            "delivered_date",
            "carrier",
            "delay_flag",
            "return_flag",
        ]
    ]

def transform_sales(df):
    df["activity_date"] = pd.to_datetime(df["activity_date"])

    return df[
        [
            "activity_id",
            "prescriber_id",
            "sales_rep",
            "activity_date",
            "activity_type",
            "outcome",
        ]
    ]

def transform_inventory(df):
    df["expiration_date"] = pd.to_datetime(df["expiration_date"])

    return df[
        ["medication_name", "stock_level", "reorder_point", "expiration_date"]
    ]

def transform_revenue(df):
    df["billing_date"] = pd.to_datetime(df["billing_date"])
    df["adjudicated_amount"] = df["adjudicated_amount"].round(2)

    return df[
        ["rx_id", "adjudicated_amount", "payer", "billing_date"]
    ]

def transform_operations(df):
    df["date"] = pd.to_datetime(df["date"])
    df["avg_shipping_time"] = df["avg_shipping_time"].round(2)

    return df[
        ["date", "orders_processed", "avg_shipping_time", "pending_authorizations"]
    ]

# ----------------------
# CONFIG
# ----------------------
TRANSFORM_FUNCS = {
    "patients.csv": transform_patients,
    "prescribers.csv": transform_prescribers,
    "prescriptions.csv": transform_prescriptions,
    "shipments.csv": transform_shipments,
    "sales_activity.csv": transform_sales,
    "inventory.csv": transform_inventory,
    "revenue.csv": transform_revenue,
    "operations_daily_kpis.csv": transform_operations,
}

TABLE_MAP = {
    "patients.csv": "patients",
    "prescribers.csv": "prescribers",
    "prescriptions.csv": "prescriptions",
    "shipments.csv": "shipments",
    "sales_activity.csv": "sales_activity",
    "inventory.csv": "inventory",
    "revenue.csv": "revenue",
    "operations_daily_kpis.csv": "operations_daily_kpis",
}

# ----------------------
# ETL FUNCTION
# ----------------------
def etl_file(file_name):
    raw_path = os.path.join(RAW_DIR, file_name)

    # SAFETY: skip empty files
    if os.path.getsize(raw_path) == 0:
        print(f" Skipping empty file: {file_name}")
        return

    df = pd.read_csv(raw_path)

    if df.empty:
        print(f" No data in {file_name}")
        return

    df = TRANSFORM_FUNCS[file_name](df)

    table = TABLE_MAP[file_name]
    cols = ",".join(df.columns)
    placeholders = ",".join(["%s"] * len(df.columns))

    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

    cursor.executemany(sql, df.values.tolist())
    conn.commit()

    processed_path = os.path.join(PROCESSED_DIR, file_name)
    df.to_csv(processed_path, index=False)

    print(f" {file_name} → {table} ({len(df)} rows)")

# ----------------------
# RUN ETL
# ----------------------
files = [
    f for f in os.listdir(RAW_DIR)
    if f.endswith(".csv")
    and not f.startswith(".")
    and os.path.isfile(os.path.join(RAW_DIR, f))
]

print("Files to process:", files)

for file in files:
    etl_file(file)

cursor.close()
conn.close()

print(" ETL completed successfully.")
