FROM prefecthq/prefect:3-latest

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "dlt[postgres]" \
    pymysql \
    humanize \
    prefect-sqlalchemy