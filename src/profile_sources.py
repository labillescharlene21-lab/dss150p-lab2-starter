"""Week 2 starter: profile CSV, JSON, Parquet, API payload, and PostgreSQL table.
Complete the TODOs. Do not hard-code expected counts.
"""
from pathlib import Path
import json
import csv
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / 'data'

def profile_csv(path):
    print(f"\n{'='*20} PROFILING CSV: {Path(path).name} {'='*20}")
    with open(path, mode='r', encoding='utf-8') as f:
        reader = list(csv.DictReader(f))
    
    total_rows = len(reader)
    if total_rows == 0:
        print("Empty file.")
        return

    cols = list(reader[0].keys())
    missing_counts = {col: 0 for col in cols}
    customer_ids = []
    seen_rows = set()
    dup_rows = 0

    for row in reader:
        # Check duplicate rows
        row_tuple = tuple(row.items())
        if row_tuple in seen_rows:
            dup_rows += 1
        else:
            seen_rows.add(row_tuple)

        # Check customer_id duplicates if present
        if 'customer_id' in row:
            customer_ids.append(row['customer_id'])

        # Check missing values
        for col in cols:
            val = row[col]
            if val is None or val.strip() == "":
                missing_counts[col] += 1

    dup_customer_ids = len(customer_ids) - len(set(customer_ids))

    print(f"Total Rows: {total_rows}")
    print(f"Columns: {cols}")
    print(f"Missing Values: {missing_counts}")
    print(f"Duplicate Rows: {dup_rows}")
    if 'customer_id' in cols:
        print(f"Duplicate Customer IDs: {dup_customer_ids}")

def profile_json(path):
    print(f"\n{'='*20} PROFILING JSON: {Path(path).name} {'='*20}")
    with open(path, mode='r', encoding='utf-8') as f:
        data = json.load(f)

    # Handle single object or list of objects
    records = data if isinstance(data, list) else [data]
    total_records = len(records)
    print(f"Total Records: {total_records}")

    if total_records == 0:
        return

    all_keys = set()
    nested_fields = set()
    null_counts = {}

    for item in records:
        if isinstance(item, dict):
            for k, v in item.items():
                all_keys.add(k)
                if v is None:
                    null_counts[k] = null_counts.get(k, 0) + 1
                if isinstance(v, (dict, list)):
                    nested_fields.add(k)

    print(f"Keys: {sorted(list(all_keys))}")
    print(f"Nested Fields: {sorted(list(nested_fields))}")
    print(f"Null Counts: {null_counts}")

def profile_parquet(path):
    print(f"\n{'='*20} PROFILING PARQUET: {Path(path).name} {'='*20}")
    p = Path(path)
    file_size_kb = p.stat().st_size / 1024
    df = pd.read_parquet(p)

    print(f"File Size: {file_size_kb:.2f} KB")
    print(f"Shape: {df.shape[0]} rows x {df.shape[1]} columns")
    print("\nData Types:")
    print(df.dtypes)
    print("\nNull Counts:")
    print(df.isnull().sum())

if __name__ == '__main__':
    profile_csv(DATA_DIR / 'customers.csv')
    profile_json(DATA_DIR / 'orders.json')
    profile_parquet(DATA_DIR / 'products.parquet')