# DE-assignment

Data Engineering assignment stack for practicing **Airflow orchestration**, **dbt transformation**, **MinIO storage**, and **Trino SQL query engine**.  
(โปรเจกต์นี้ใช้สำหรับฝึก Data Engineering: Orchestration, Transformation, Storage, Query)

---

## 🏗 Stack Overview
- **Apache Airflow** → orchestrator (schedule & run DAGs)  
- **dbt** → transformation (bronze → silver → gold)  
- **MinIO** → S3-compatible object storage (data lake)  
- **Trino** → SQL query engine (query data in MinIO)  
- **Docker Compose** → run all services in containers  

---

## ✅ Prerequisites
- Git  
- Docker ≥ 20.x  
- Docker Compose ≥ v2.x  
- Bash/Shell  
- RAM ≥ 8 GB, CPU ≥ 2–4 cores  
- Open ports:  
  - `8080` (Airflow)  
  - `8081/8082` (Trino)  
  - `9000/9001` (MinIO)  
- `.env` file with:
  - `AIRFLOW_UID`, `AIRFLOW_GID` (default: 50000)  
  - `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`  

> ⚠️ No Python needed on host — everything runs inside containers.  

---

## ⚡ Quick Start
1. Clone the repo  
   ```
   git clone https://github.com/lu69x/de-assignment.git
   cd de-assignment
   ```

2. Create .env file (if missing) and set required variables

3. Run initial setup
    ```
    sudo chmod 755 setup.sh
    ./setup.sh
    ```
    or
    ```
    bash setup.sh
    ```

4. Start all services
    ```
    docker compose -f full-build-docker-compose.yaml up -d
    ```
5. Access services:
  - Airflow UI → http://localhost:8080
  - MinIO Console → http://localhost:9001
  - Trino Coordinator → http://localhost:8081


## System Diagram
```mermaid
flowchart LR
  subgraph Docker_Network["Docker Network (docker-compose)"]
    A[Client / Analyst<br/>• Airflow UI<br/>• Trino SQL Client]:::ext

    subgraph Airflow_Stack["Apache Airflow"]
      A1[Webserver] --- A2[Scheduler] --- A3[Worker/Triggerer]
      A4[(Logs/Configs)]
    end

    subgraph Transform["dbt (models/seeds/macros)"]
      D1[dbt run / build]:::job
    end

    subgraph Storage["MinIO (S3-compatible)"]
      M1[[Bronze]]:::bronze
      M2[[Silver]]:::silver
      M3[[Gold]]:::gold
    end

    subgraph Query["Trino"]
      T1[Coordinator/Cluster]:::svc
      T2[(Catalogs & Connectors<br/>e.g., S3/MinIO, Hive, etc.)]:::meta
    end
  end

  %% Data flow
  A1 <-- manage DAGs / trigger --> A2
  A2 -->|run tasks| A3
  A3 -->|ingest raw files| M1
  A3 -->|invoke| D1
  D1 -->|read from| M1
  D1 -->|transform write| M2
  D1 -->|business marts| M3
  T1 -->|query tables| M2
  T1 -->|query marts| M3
  A:::hidden

  %% External access
  A --- A1
  A --- T1

classDef ext fill:#fff,stroke:#888,stroke-width:1px
classDef job fill:#eef,stroke:#557
classDef svc fill:#efe,stroke:#585
classDef meta fill:#fef,stroke:#a5a
classDef bronze fill:#f9e0,stroke:#d5a
classDef silver fill:#e6f2ff,stroke:#69f
classDef gold fill:#fff4b3,stroke:#cb3
classDef hidden display:none
```