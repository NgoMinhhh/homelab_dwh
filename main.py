import dlt
from dlt.sources.sql_database import sql_database
import os

def run_initial_load():
    """
    Performs a full mirror of the ERP MariaDB to the Postgres DWH.
    Uses 'replace' to ensure a clean start.
    """
    
    # 1. Initialize the pipeline
    # dlt looks for DESTINATION__POSTGRES__CREDENTIALS in your environment
    pipeline = dlt.pipeline(
        pipeline_name='erpnext_initial_migration',
        destination='postgres',
        dataset_name='erpnext_raw'  # This becomes the schema in Postgres
    )

    # 2. Configure the source (MariaDB)
    # dlt looks for SOURCES__SQL_DATABASE__CREDENTIALS in your environment
    # By default, this reflects ALL tables in the database.
    source = sql_database()

    # 3. Run the pipeline with 'replace' write disposition
    # This drops existing tables in the 'erp_raw' schema and recreates them.
    print("🚀 Starting initial load from MariaDB to Postgres...")
    load_info = pipeline.run(source, write_disposition="replace")
    
    # 4. Success message and stats
    print(f"✅ Initial load complete!")
    print(load_info)

if __name__ == "__main__":
    run_initial_load()