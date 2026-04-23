import humanize
import dlt
from dlt.sources.sql_database import sql_database
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret

@task(name="dlt-full-load")
def load_entire_database() -> str:
    source_conn = Secret.load("erpnext-mysql-conn").get()
    dest_conn = Secret.load("postgres-dwh-conn").get()

    """Use the sql_database source to completely load all tables in a database"""
    pipeline = dlt.pipeline(
        pipeline_name="erpnext_raw_to_dwh", 
        destination=dlt.destinations.postgres(credentials=dest_conn), 
        dataset_name="erpnext_raw")

    # By default the sql_database source reflects all tables in the schema
    # The database credentials are sourced from the `.dlt/secrets.toml` configuration
    source = sql_database(credentials=source_conn ,engine_kwargs={
        "connect_args":{
            "init_command": "SET GLOBAL max_connections = 100000"
        }   
    })

    # Run the pipeline. For a large db this may take a while
    info = pipeline.run(source, write_disposition="replace")
    duration = humanize.precisedelta(
        pipeline.last_trace.finished_at - pipeline.last_trace.started_at
    )
    return f"{duration}\n{info}"

@flow(name="erpnext-initial-load",log_prints=True)
def load_entire_database_flow() -> None:
    logger = get_run_logger()
    logger.info("Starting ERPNext initial full load into Postgres DWH")
    result = load_entire_database()
    logger.info(f"Load finished: {result} ")

if __name__ == "__main__":
    # Load all tables from the database.
    # Warning: The sample database is very large
    load_entire_database_flow()
