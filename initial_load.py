import humanize
import dlt
from dlt.sources.sql_database import sql_database

def load_entire_database() -> None:
    """Use the sql_database source to completely load all tables in a database"""
    pipeline = dlt.pipeline(pipeline_name="rfam", destination='postgres', dataset_name="erpnext_raw")

    # By default the sql_database source reflects all tables in the schema
    # The database credentials are sourced from the `.dlt/secrets.toml` configuration
    source = sql_database(engine_kwargs={
        "connect_args":{
            "init_command": "SET GLOBAL max_connections = 100000"
        }   
    })

    # Run the pipeline. For a large db this may take a while
    info = pipeline.run(source, write_disposition="replace")
    print(humanize.precisedelta(pipeline.last_trace.finished_at - pipeline.last_trace.started_at))
    print(info)


if __name__ == "__main__":
    # Load all tables from the database.
    # Warning: The sample database is very large
    load_entire_database()
