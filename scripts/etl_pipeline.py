"""
NYC Taxi ETL Pipeline
=====================
This script performs an Extract, Transform, Load (ETL) process on NYC Yellow Taxi data.
It accepts a dynamic year-month argument to allow orchestration tools to loop through time.

Functions:
    run_etl(): Executes the core ETL logic, extracting data from web sources, 
               transforming it, and loading it into PostgreSQL.
"""

import pandas as pd
from sqlalchemy import create_engine
import os
import sys

def run_etl(year_month):
    """
    Executes the full ETL pipeline for a specific month.
    """
    DB_URL = os.getenv("DATABASE_URL")
    if not DB_URL:
        print("ERROR: DATABASE_URL environment variable is missing!")
        sys.exit(1)
    
    print(f"Starting ETL Process for logical month: {year_month}...")
    
    # --- 1. EXTRACT ---
    print("\n[1/3] EXTRACTING DATA...")
    
    # Dynamically inject the year_month into the URL
    trip_data_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_month}.parquet"
    print(f" -> Downloading trip data from: {trip_data_url}")
    
    try:
        df_trips = pd.read_parquet(trip_data_url)
    except Exception as e:
        print(f"Failed to download data for {year_month}. It might not exist yet. Error: {e}")
        sys.exit(1) # Fail the Airflow task
    
    zone_lookup_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    print(f" -> Downloading zone lookup data from: {zone_lookup_url}")
    df_zones = pd.read_csv(zone_lookup_url)
    
    # --- 2. TRANSFORM ---
    print("\n[2/3] TRANSFORMING DATA...")
    df_transformed = df_trips.merge(
        df_zones, 
        left_on='PULocationID', 
        right_on='LocationID', 
        how='left'
    )
    df_transformed.rename(columns={'Zone': 'Pickup_Zone'}, inplace=True)
    
    # Add a column so students can easily query and see which month's data this is
    df_transformed['logical_month'] = year_month
    
    # --- 3. LOAD ---
    print("\n[3/3] LOADING DATA...")
    engine = create_engine(DB_URL)
    
    rows_to_load = 500
    print(f" -> Loading top {rows_to_load} rows to the 'trips' table...")
    df_transformed.head(rows_to_load).to_sql('trips', engine, if_exists='append', index=False)
    
    print(f"\n Successfully completed ETL process for {year_month}!")

if __name__ == "__main__":
    # Check if a command line argument was passed; default to 2024-01 if not
    if len(sys.argv) > 1:
        target_month = sys.argv[1]
    else:
        target_month = "2024-01"
        
    run_etl(target_month)