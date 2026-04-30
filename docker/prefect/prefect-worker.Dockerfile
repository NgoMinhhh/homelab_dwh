FROM prefecthq/prefect:3-python3.12

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    libpq5 \
    libpq-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/prefect/app

COPY pyproject.toml uv.lock ./

RUN pip install --no-cache-dir uv \
    && uv pip install --system --no-cache -r pyproject.toml

COPY . .

ENV PYTHONPATH=/opt/prefect/app

USER prefect