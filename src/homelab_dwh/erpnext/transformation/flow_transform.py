from prefect import flow
from prefect_dbt import PrefectDbtRunner


@flow
def run_dbt():
    PrefectDbtRunner().invoke(["build"])


if __name__ == "__main__":
    run_dbt()