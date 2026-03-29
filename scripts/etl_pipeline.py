"""
NYC Taxi ETL Pipeline
=====================
This script performs an Extract, Transform, Load (ETL) process on NYC Yellow Taxi data.
It is designed to be executed inside a Docker container and orchestrated via Airflow/Kubernetes.

Functions:
    run_etl(): Executes the core ETL logic, extracting data from web sources, 
               transforming it, and loading it into PostgreSQL.
"""

import pandas as pd
from sqlalchemy import create_engine
import os
import sys

def run_etl():
    """
    Executes the full ETL pipeline.
    
    Extract: Downloads Jan 2024(Default) Yellow Taxi data AND the Taxi Zone Lookup CSV.
    Transform: Merges the taxi data with the zone names based on Location ID.
    Load: Inserts the first 500 rows into a PostgreSQL database for testing.
    """
    # Securely grab the database URL injected by Docker/Kubernetes
    DB_URL = os.getenv("DATABASE_URL")
    if not DB_URL:
        print("ERROR: DATABASE_URL environment variable is missing!")
        sys.exit(1)
    
    print("🚀 Starting ETL Process...")
    
    # --- 1. EXTRACT ---
    print("\n[1/3] EXTRACTING DATA...")
    
    trip_data_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    print(f" -> Downloading trip data from: {trip_data_url}")
    df_trips = pd.read_parquet(trip_data_url)
    
    zone_lookup_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    print(f" -> Downloading zone lookup data from: {zone_lookup_url}")
    df_zones = pd.read_csv(zone_lookup_url)
    
    # --- 2. TRANSFORM ---
    print("\n[2/3] TRANSFORMING DATA...")
    print(" -> Merging trip data with zone lookups...")
    
    # Left join to bring in the pickup zone name
    df_transformed = df_trips.merge(
        df_zones, 
        left_on='PULocationID', 
        right_on='LocationID', 
        how='left'
    )
    
    # Clean up columns (optional, but good practice to show students)
    # Renaming 'Zone' to 'Pickup_Zone' for clarity
    df_transformed.rename(columns={'Zone': 'Pickup_Zone'}, inplace=True)
    
    # --- 3. LOAD ---
    print("\n[3/3] LOADING DATA...")
    print(f" -> Connecting to database...")
    engine = create_engine(DB_URL)
    
    # Limiting to 500 rows so pipeline runs finish quickly for testing
    rows_to_load = 500
    print(f" -> Loading top {rows_to_load} rows to the 'trips' table...")
    df_transformed.head(rows_to_load).to_sql('trips', engine, if_exists='append', index=False)
    
    print("\n✅ Successfully completed ETL process!")

if __name__ == "__main__":
    run_etl()