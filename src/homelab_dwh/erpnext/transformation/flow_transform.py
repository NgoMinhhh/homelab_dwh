from prefect import flow
from prefect_dbt import PrefectDbtRunner
from prefect.blocks.system import Secret
import os   


@flow
def run_dbt():

    env_vars = Secret.load("dbt-profile").get()
    os.environ.update(env_vars)

    PrefectDbtRunner().invoke(["build", "--project-dir", "src/homelab_dwh/erpnext/transformation"])


if __name__ == "__main__":
    run_dbt()