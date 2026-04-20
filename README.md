# Homelab Data Warehouse (DWH)
A lightweight, modular Data Warehouse stack built using PostgreSQL, dlt, and Prefect, designed for ingesting and managing operational data from ERP systems in a secure, containerised environment.

## Architecture Overview
This project follows a Hub-and-Spoke model:
Sources → ERPNext (MariaDB)
Ingestion → dlt (Python-based ELT)
Warehouse → PostgreSQL 18
Orchestration → Prefect
Deployment → Docker + Portainer

## Data Pipeline Orchestration & Ingestion (Prefect + dlt)
This repository implements a lightweight, production-oriented data ingestion pipeline using:
- **dlt** → extraction & loading (ELT)
- **Prefect** → orchestration, scheduling, and deployment

The design intentionally separates:
- **Pipeline logic (Python)**
- **Orchestration & deployment (YAML via Prefect)**

## Extract and Load with dlt (Data Load Tool)
###  Core Pipeline Design
Each pipeline script follows a minimal pattern:
1. Load credentials (via Prefect blocks)
2. Create a `dlt` pipeline
3. Define SQL source
4. Apply resource hints (for incremental)
5. Execute `pipeline.run()`
6. Return metadata for logging
### Load Modes
#### Initial Load
```write_disposition="replace"```
- Full refresh
- Recreates all tables
- Used for first-time setup
#### Incremental Load
```write_disposition="merge"```
- Insert + update
- Uses:
    -modified → incremental cursor
    -name → unique key

### Design Decision: Default Merge Strategy
Instead of configuring each table manually, most ERPNext tables:
- include modified
- use name as primary key

As such, it is easier to apply merge and incremental(modified) to almost all tables

## Orchestration with Prefect
### Task & Flow Design
Each pipeline is wrapped as a Prefect task/flow:
- Python handles: pipeline execution (dlt)
- Prefect handles: when and how it runs

### Deployment via prefect.yaml

This project uses YAML-based deployment instead of Python deployment:
- Keeps Python scripts simple
- Avoids embedding orchestration logic in code
- Easier to modify schedules and entrypoints
- Works well with Git-based workflow

### Deployment Workflow
1. Update prefect.yaml
2. Push changes to Git
3. Prefect pulls latest repo at runtime
4. Run / test deployment

### Worker Configuration
This project choose process Worker which runs as a persistent container and executes flows as subprocesses because:
- Lightweight ingestion workloads
- No heavy transformation yet
- Stable dependencies
- Simpler deployment in Docker/Portainer