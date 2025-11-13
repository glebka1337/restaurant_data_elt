
# Restaurant ELT Pipeline (Local Demo)

This is a complete, miniature ELT pipeline fully containerized with Docker Compose. It simulates a real-world "Modern Data Stack" (MDS) by extracting data from an API, transforming it, and displaying it on a dashboard.

## 🛠️ Tech Stack

  * **Source (API):** **FastAPI** (Generates fake, but consistent orders)
  * **Orchestrator (EL):** **Apache Airflow** (Extracts data and loads it into the DWH)
  * **Transformer (T):** **dbt** (Transforms raw JSON into clean, ready-to-query views)
  * **Data Viz (BI):** **Streamlit** (Reads dbt views and builds a dashboard)
  * **DWH:** **Postgres** (Used for Airflow's metadata DB, the raw data zone, and the analytics schema)

## 📊 Architecture (How it Works)

1.  **FastAPI** (`localhost:8001`) generates 100 JSON orders via the `/api/v1/new_orders` endpoint.
2.  **Airflow** (`localhost:8080`) runs the `restaurant_order_pipeline` DAG (on a schedule or manually).
3.  **The (E)xtract Task:** Fetches the 100 JSON orders from the API.
4.  **The (L)oad Task:** Loads the raw JSON payload into the `raw_zone.staging_raw_orders` table in Postgres.
5.  **The (T)ransform Task:** Runs `dbt run` (in an isolated virtual environment). This process:
      * Reads from `raw_zone.staging_raw_orders`.
      * Parses the JSON and creates a clean view: `staging.stg_orders`.
6.  **Streamlit** (`localhost:8501`) reads directly from the `staging.stg_orders` dbt view to build its charts.

## ⚙️ How to Run

### 1\. Environment Setup (.env)

1.  Clone the repository.
2.  Copy the example `.env` file:
    ```bash
    cp .env.example .env
    ```
3.  **(CRITICAL)** You must fix file permissions for the Airflow `logs/dags` folders. Get your current user ID and append it to the `.env` file:
    ```bash
    echo "AIRFLOW_UID=$(id -u)" >> .env
    ```

**For reference, this is what `.env.example` contains:**

```ini
# Your host user ID (for fixing dags/logs permissions)
# Find it by running: id -u
AIRFLOW_UID=

# --- Postgres Settings ---
# (Used by Airflow, dbt, and Streamlit)
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
POSTGRES_PORT=5432

# --- Airflow Auto-Configuration ---
# (Airflow will automatically create these on startup)
AIRFLOW_VAR_RESTAURANT_API=http://restaurant-api:8000/api/v1/new_orders
AIRFLOW_CONN_POSTGRES_DWH=postgresql+psycopg2://airflow:airflow@postgres-db:5432/airflow

# --- Airflow Admin User ---
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
_AIRFLOW_WWW_USER_EMAIL=admin@example.com
_AIRFLOW_WWW_USER_FIRSTNAME=Admin
_AIRFLOW_WWW_USER_LASTNAME=User
```

### 2\. Launch the Stack

Run the entire stack with a single command from the project root:

```bash
docker compose up --build -d
```

*(The first build may take 5-10 minutes as it installs Airflow and dbt dependencies).*

### 3\. Access the Services

1.  **Airflow UI (Orchestrator):** `http://localhost:8080`

      * (Login: `admin`, Password: `admin`)
      * Un-pause the `restaurant_order_pipeline` DAG and trigger it (▶).

2.  **Streamlit (Dashboard):** `http://localhost:8501`

      * (Will show data *after* the DAG has run successfully).

3.  **FastAPI (Data Source):** `http://localhost:8001/docs`