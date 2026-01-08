#!/usr/bin/env python3
"""
Demo script to illustrate scheme masterdata functionality.

This demonstrates how the masterdata tracks schemes over time:
- Initial state: All schemes active
- Simulated update: Some schemes removed, some added, some unchanged
- Final state: Shows active, inactive, and new schemes
"""

import pandas as pd
from datetime import datetime, timedelta

print("=" * 70)
print("SCHEME MASTERDATA DEMO")
print("=" * 70)

# Load current masterdata
masterdata_file = "data/processed/scheme_metadata/scheme_masterdata.parquet"
df = pd.read_parquet(masterdata_file)

print(f"\n1. CURRENT MASTERDATA STATE:")
print(f"   Total schemes: {len(df):,}")
print(f"   Active schemes: {df['is_active'].sum():,}")
print(f"   Inactive schemes: {(~df['is_active']).sum():,}")

# Show sample schemes
print(f"\n2. SAMPLE SCHEMES:")
sample_cols = ['scheme_code', 'scheme_name', 'first_seen_date', 'last_seen_date', 'is_active']
print(df[sample_cols].head(5).to_string(index=False))

# Simulate what happens when schemes change
print(f"\n3. SIMULATION - What happens when schemes change:")
print(f"   Scenario: 50 schemes removed, 25 new schemes added")

# Simulate by creating a mock "latest" dataset
latest_mock = df.head(len(df) - 50).copy()  # Remove last 50 schemes
print(f"   - Removed schemes would be marked: is_active=False")
print(f"   - Removed schemes preserved with last_seen_date = previous date")

# Show what an inactive scheme would look like
inactive_sample = df.iloc[-50:].copy()
inactive_sample['is_active'] = False
print(f"\n   Example of inactive scheme:")
print(inactive_sample[sample_cols].head(2).to_string(index=False))

# Simulate new schemes
print(f"\n   - New schemes added with:")
print(f"     * first_seen_date = today")
print(f"     * last_seen_date = today")
print(f"     * is_active = True")

# Summary statistics
print(f"\n4. MASTERDATA BENEFITS:")
print(f"   ✓ Historical Analysis: Query all schemes that ever existed")
print(f"   ✓ Trend Analysis: Track when schemes were launched and closed")
print(f"   ✓ Data Integrity: Never lose scheme information")
print(f"   ✓ Current State: Filter by is_active=True for current schemes")

print(f"\n5. USAGE EXAMPLES:")
print(f"   # Get all active schemes")
print(f"   active_schemes = df[df['is_active'] == True]")
print(f"\n   # Get inactive schemes (historical)")
print(f"   inactive_schemes = df[df['is_active'] == False]")
print(f"\n   # Get schemes launched in last 30 days")
print(f"   recent = df[df['first_seen_date'] >= pd.Timestamp.now() - pd.Timedelta(days=30)]")
print(f"\n   # Get schemes that became inactive recently")
print(f"   recently_closed = df[(~df['is_active']) & (df['last_seen_date'] >= pd.Timestamp.now() - pd.Timedelta(days=30))]")

print("\n" + "=" * 70)
print("Demo complete! Run 07_build_scheme_masterdata.py regularly to maintain this data.")
print("=" * 70)
