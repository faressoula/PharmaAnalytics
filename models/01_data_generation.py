import pandas as pd
import numpy as np
from faker import Faker
from datetime import timedelta, date
import random

fake = Faker()
np.random.seed(42)

N_PATIENTS = 500
N_PRESCRIBERS = 80
N_RX = 1200
N_SHIPMENTS = 1000

# -----------------------
# patients
# -----------------------
patients = pd.DataFrame({
    "patient_id": range(1, N_PATIENTS + 1),
    "enrollment_date": [fake.date_between(start_date='-1y', end_date='today') for _ in range(N_PATIENTS)],
    "condition": np.random.choice(["Crohn’s", "RA", "MS", "Psoriasis"], N_PATIENTS),
    "insurance_type": np.random.choice(["Commercial", "Medicare", "Medicaid"], N_PATIENTS),
    "state": np.random.choice(["TX", "CA", "FL", "NY", "IL"], N_PATIENTS)
})

# -----------------------
# prescribers
# -----------------------
prescribers = pd.DataFrame({
    "prescriber_id": range(1, N_PRESCRIBERS + 1),
    "clinic_name": [fake.company() for _ in range(N_PRESCRIBERS)],
    "specialty": np.random.choice(["Gastroenterology", "Rheumatology", "Neurology"], N_PRESCRIBERS),
    "region": np.random.choice(["South", "West", "East", "Midwest"], N_PRESCRIBERS),
    "assigned_sales_rep": np.random.choice(["Rep A", "Rep B", "Rep C", "Rep D"], N_PRESCRIBERS)
})

# -----------------------
# prescriptions
# -----------------------
created_dates = pd.to_datetime(
    np.random.choice(pd.date_range("2024-01-01", "2024-12-01"), N_RX)
)

approval_dates = [
    d + timedelta(days=random.randint(1, 14)) if random.random() > 0.15 else pd.NaT
    for d in created_dates
]

prescriptions = pd.DataFrame({
    "rx_id": range(1, N_RX + 1),
    "patient_id": np.random.choice(patients.patient_id, N_RX),
    "prescriber_id": np.random.choice(prescribers.prescriber_id, N_RX),
    "medication_name": np.random.choice(["Humira", "Stelara", "Skyrizi"], N_RX),
    "created_date": created_dates,
    "approval_date": approval_dates,
    "refill_flag": np.random.choice([True, False], N_RX),
    "status": np.where(pd.isna(approval_dates), "Pending PA", "Approved")
})

# -----------------------
# shipments
# -----------------------
shipments = prescriptions.dropna(subset=["approval_date"]).sample(N_SHIPMENTS)

shipments = pd.DataFrame({
    "shipment_id": range(1, len(shipments) + 1),
    "rx_id": shipments.rx_id.values,
    "shipped_date": shipments.approval_date + pd.to_timedelta(np.random.randint(1, 3, len(shipments)), unit="D"),
    "delivered_date": shipments.approval_date + pd.to_timedelta(np.random.randint(2, 6, len(shipments)), unit="D"),
    "carrier": np.random.choice(["UPS", "FedEx"], len(shipments)),
    "delay_flag": np.random.choice([True, False], len(shipments), p=[0.2, 0.8]),
    "return_flag": np.random.choice([True, False], len(shipments), p=[0.05, 0.95])
})

# -----------------------
# sales activity
# -----------------------
sales_activity = pd.DataFrame({
    "activity_id": range(1, 800),
    "prescriber_id": np.random.choice(prescribers.prescriber_id, 799),
    "sales_rep": np.random.choice(["Rep A", "Rep B", "Rep C", "Rep D"], 799),
    "activity_date": [fake.date_between("-6M", "today") for _ in range(799)],
    "activity_type": np.random.choice(["Call", "Email", "Visit"], 799),
    "outcome": np.random.choice(["Interested", "Follow-up", "No Response"], 799)
})

# -----------------------
# inventory
# -----------------------
inventory = pd.DataFrame({
    "medication_name": ["Humira", "Stelara", "Skyrizi"],
    "stock_level": [320, 210, 150],
    "reorder_point": [100, 80, 60],
    "expiration_date": [
        fake.date_between(date(2025, 1, 1), date(2025, 12, 31)) for _ in range(3)
    ]
})

# -----------------------
# revenue
# -----------------------
revenue = pd.DataFrame({
    "rx_id": prescriptions.rx_id,
    "adjudicated_amount": np.random.uniform(2500, 15000, N_RX).round(2),
    "payer": np.random.choice(["Commercial", "Medicare", "Medicaid"], N_RX),
    "billing_date": prescriptions.created_date + pd.to_timedelta(20, unit="D")
})

# -----------------------
# operations KPIs
# -----------------------
operations_daily_kpis = pd.DataFrame({
    "date": pd.date_range("2024-10-01", periods=90),
    "orders_processed": np.random.randint(20, 120, 90),
    "avg_shipping_time": np.random.uniform(2, 4, 90).round(2),
    "pending_authorizations": np.random.randint(10, 50, 90)
})

# -----------------------
# SAVE FILES
# -----------------------
patients.to_csv("../data/raw/patients.csv", index=False)
prescribers.to_csv("../data/raw/prescribers.csv", index=False)
prescriptions.to_csv("../data/raw/prescriptions.csv", index=False)
shipments.to_csv("../data/raw/shipments.csv", index=False)
sales_activity.to_csv("../data/raw/sales_activity.csv", index=False)
inventory.to_csv("../data/raw/inventory.csv", index=False)
revenue.to_csv("../data/raw/revenue.csv", index=False)
operations_daily_kpis.to_csv("../data/raw/operations_daily_kpis.csv", index=False)

print("Dummy data generated.")
