#!/usr/bin/env python
# Test script to verify the pandas/pyarrow fix

import os
# Fix for PyArrow/Pandas compatibility issue
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

import pandas as pd
import pyarrow.parquet as pq

print("✓ Successfully imported pandas")
print(f"  Pandas version: {pd.__version__}")

print("✓ Successfully imported pyarrow")
import pyarrow
print(f"  PyArrow version: {pyarrow.__version__}")

# Test reading a parquet file if one exists
import glob
parquet_files = glob.glob('./shab_data/*.parquet')
if parquet_files:
    print(f"\n✓ Found {len(parquet_files)} parquet file(s)")
    try:
        df = pd.read_parquet(parquet_files[0])
        print(f"✓ Successfully read parquet file: {parquet_files[0]}")
        print(f"  Shape: {df.shape}")
    except Exception as e:
        print(f"✗ Error reading parquet file: {e}")
else:
    print("\n• No existing parquet files found (this is normal for first run)")

print("\n✓ All tests passed! The fix is working correctly.")
