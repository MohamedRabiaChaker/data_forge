# DataForge - Configuration-Driven ETL Framework

A modular, configuration-driven ETL framework built on Apache Airflow 3.1.0. Define your data pipelines using simple JSON configurations - no Python code required!

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Supported Connectors](#supported-connectors)
- [Configuration Guide](#configuration-guide)
- [Connection Setup](#connection-setup)
- [Implementation Details](#implementation-details)
- [Extending the Framework](#extending-the-framework)
- [Troubleshooting](#troubleshooting)
- [Production Deployment](#production-deployment)

## Features

- **Configuration-Driven**: Define ETL pipelines using JSON configuration files
- **Auto-Discovery**: Automatically generates Airflow DAGs from config files in `dags/configs/`
- **Modular Architecture**: Extensible source, transform, and destination components
- **Production-Ready**: Transaction-safe loading, schema inference, pre/post hooks
- **Built-in Connectors**: Google Sheets, Shopify, PostgreSQL (see [Supported Connectors](#supported-connectors))
- **Smart Loading**: Optimized strategies for large datasets using temp tables
- **Airflow 3.1.0**: Built on the latest Airflow with TaskFlow API and modern patterns

## Quick Start

### 1. Start Airflow

```bash
docker compose -f docker-compose.dev.yaml up -d
```

### 2. Access the Airflow UI

- URL: http://localhost:8081
- Username: `airflow`
- Password: `airflow`
- Wait 30-60 seconds after startup for the UI to become fully available

### 3. Configure Connections

Set up connections for data sources and destinations in the Airflow UI (**Admin > Connections**). See [Connection Setup](#connection-setup) for detailed instructions.

### 4. Create Your First Pipeline

Copy an example config and customize it:

```bash
cp dags/configs/example_gsheet_to_postgres.json dags/configs/my_pipeline.json
```

Edit `my_pipeline.json` to match your data source and destination.

### 5. Run Your Pipeline

1. Refresh the Airflow UI - your DAG should appear automatically
2. Toggle it **ON** (switch in top-right)
3. Click **Trigger DAG** (play button) to run immediately
4. Monitor progress in the Graph view and check logs

## Architecture

DataForge follows a layered architecture designed for modularity and extensibility:

```
JSON Config File (dags/configs/*.json)
    ↓
Pipeline DAG Generator (pipeline_dag_generator.py)
    ↓
DAG Builder (dag_builder.py)
    ↓
┌─────────────┬─────────────┬─────────────┐
│   Extract   │  Transform  │     Load    │
│   (Source)  │  (Filters)  │(Destination)│
└─────────────┴─────────────┴─────────────┘
```

### Key Components

- **Configuration Layer**: JSON files define pipelines without code
- **Orchestration Layer**: Auto-generates DAGs and wires tasks
- **Factory Layer**: Dynamic component instantiation
- **Hook Layer**: Airflow-native authentication (ShopifyHook, etc.)
- **Component Layer**: Source, Transform, and Destination implementations

For detailed architecture documentation, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

### Project Structure

```
data_forge/
├── dags/
│   ├── configs/              # JSON pipeline configurations (auto-scanned)
│   │   ├── example_gsheet_to_postgres.json
│   │   ├── example_shopify_customers_to_postgres.json
│   │   ├── example_shopify_orders_to_postgres.json
│   │   └── example_shopify_products_to_postgres.json
│   ├── hooks/                # Airflow hooks for external systems
│   │   ├── __init__.py
│   │   └── shopify_hook.py   # Shopify OAuth hook
│   ├── tasks/                # ETL component implementations
│   │   ├── sources/          # Data source extractors
│   │   │   ├── base_source.py
│   │   │   ├── gsheet_source.py
│   │   │   └── shopify_source.py
│   │   ├── transforms/       # Data transformations
│   │   │   ├── base_transform.py
│   │   │   ├── common_transforms.py
│   │   │   └── validation_transforms.py
│   │   └── destinations/     # Data loaders
│   │       ├── base_destination.py
│   │       └── postgres_destination.py
│   ├── factories/            # Factory pattern for component creation
│   │   ├── source_factory.py
│   │   ├── transform_factory.py
│   │   └── destination_factory.py
│   ├── dag_builder.py        # Core ETL orchestration
│   └── pipeline_dag_generator.py  # Auto-generates DAGs from configs
├── plugins/                  # Custom Airflow plugins
├── logs/                     # Airflow logs (auto-generated)
├── docker-compose.dev.yaml   # Main compose configuration
├── Dockerfile                # Custom Airflow image
├── requirements.txt          # Python dependencies
└── .env                      # Environment variables
```

## Supported Connectors

### Sources

| Connector | Description | Features |
|-----------|-------------|----------|
| **Google Sheets** (`gsheet`) | Extract from Google Sheets | Auto header detection, service account auth |
| **Shopify** (`shopify`) | Extract from Shopify Admin API | 7 resources (products, orders, customers), pagination, rate limiting, incremental sync |

### Transforms

| Transform | Description | Use Case |
|-----------|-------------|----------|
| **Normalize Columns** (`normalize_columns`) | Convert to snake_case | `Product Name` → `product_name` |
| **Filter Rows** (`filter_rows`) | Filter by column value | Keep only `status = 'active'` |
| **Select Columns** (`select_columns`) | Include/exclude columns | Remove sensitive fields |

### Destinations

| Connector | Description | Features |
|-----------|-------------|----------|
| **PostgreSQL** (`postgres`) | Load to PostgreSQL | Auto schema inference, smart loading (temp tables for >1000 rows), 3 write modes |

**For detailed connector documentation, parameters, and examples, see [docs/CONNECTORS.md](docs/CONNECTORS.md)**

## Configuration Guide

### Complete Configuration Example

```json
{
  "dag_id": "complete_etl_pipeline",
  "description": "Extract data from Google Sheets and load to PostgreSQL",
  "schedule_interval": "@daily",
  "start_date": "2025-01-01",
  "catchup": false,
  "tags": ["etl", "production", "gsheet"],
  "default_args": {
    "owner": "airflow",
    "retries": 2,
    "retry_delay_minutes": 5,
    "email_on_failure": false,
    "email_on_retry": false
  },
  "source": {
    "type": "gsheet",
    "gcp_conn_id": "google_cloud_default",
    "spreadsheet_id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
    "range_name": "Sheet1!A1:Z1000"
  },
  "transforms": [
    {
      "type": "normalize_columns",
      "lowercase": true,
      "replace_spaces": "_",
      "remove_special_chars": true
    },
    {
      "type": "filter_rows",
      "column": "status",
      "operator": "eq",
      "value": "active"
    },
    {
      "type": "select_columns",
      "include": ["id", "name", "email", "created_at", "status"]
    }
  ],
  "destination": {
    "type": "postgres",
    "postgres_conn_id": "postgres_default",
    "schema": "public",
    "table_name": "imported_data",
    "write_mode": "replace",
    "create_table": true,
    "temp_table_threshold": 1000
  }
}
```

### DAG Parameters

- `dag_id` (required): Unique identifier for the DAG
- `description`: Human-readable description
- `schedule_interval`: Cron expression or preset (`@daily`, `@hourly`, `@weekly`, `null` for manual)
- `start_date`: Start date in YYYY-MM-DD format
- `catchup`: Whether to backfill missed runs (typically `false`)
- `tags`: List of tags for organization
- `default_args`: Default task arguments (retries, owner, etc.)

### Source Configuration

Each source requires:
- `type`: Source type (`gsheet`, `shopify`)
- Connection ID parameter (e.g., `gcp_conn_id`, `shopify_conn_id`)
- Source-specific parameters (spreadsheet_id, resource, etc.)

### Transform Configuration

Transforms are applied sequentially. Each requires:
- `type`: Transform type
- Transform-specific parameters

### Destination Configuration

Each destination requires:
- `type`: Destination type (`postgres`)
- Connection ID parameter
- Destination-specific parameters (table_name, write_mode, etc.)

## Connection Setup

Connections store credentials and configuration for external systems in Airflow.

### Quick Setup

1. Access Airflow UI at http://localhost:8081 (username: `airflow`, password: `airflow`)
2. Navigate to: **Admin > Connections**
3. Click **+** to add connections

### Google Sheets Connection

1. Create Google Cloud service account and download JSON key
2. Share your Google Sheet with the service account email
3. Create connection in Airflow:
   - **Connection Id**: `google_cloud_default`
   - **Connection Type**: `Google Cloud`
   - **Keyfile JSON**: Paste service account JSON

### Shopify Connection

1. Create Custom App in Shopify Admin
2. Configure API scopes (`read_products`, `read_orders`, etc.)
3. Get Admin API access token (starts with `shpat_`)
4. Create connection in Airflow:
   - **Connection Id**: `shopify_default`
   - **Connection Type**: `HTTP`
   - **Host**: `your-shop.myshopify.com`
   - **Password**: Your access token

### PostgreSQL Connection

1. Create connection in Airflow:
   - **Connection Id**: `postgres_default`
   - **Connection Type**: `Postgres`
   - **Host**: `postgres` (for Docker Compose) or your database host
   - **Schema**: `public` or your schema
   - **Login/Password**: Database credentials
   - **Port**: `5432`

**For detailed connection setup instructions, troubleshooting, and best practices, see [docs/CONNECTION_SETUP.md](docs/CONNECTION_SETUP.md)**

## Implementation Details

DataForge implements several production-ready patterns:

### Smart Loading Strategy

PostgreSQL destination uses different strategies based on dataset size:
- **Small (<1000 rows)**: Direct insert for speed
- **Large (≥1000 rows)**: Temp table + atomic swap for safety and performance

### Schema Inference

Automatically detects PostgreSQL column types from data:
- `bool` → `BOOLEAN`
- `int` → `INTEGER`  
- `float` → `DOUBLE PRECISION`
- `str` → `TEXT`

### Transform Chaining

Transforms apply sequentially with data flowing through each step.

### OAuth Architecture

ShopifyHook handles authentication, pagination, and rate limiting automatically.

**For detailed implementation documentation including design patterns, data flow, and architecture deep dive, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**

## Extending the Framework

DataForge uses a factory pattern that makes it easy to add custom sources, transforms, and destinations.

### Quick Example: Adding a Custom Source

1. Create `dags/tasks/sources/my_source.py`:

```python
from dags.tasks.sources.base_source import BaseSource
from typing import List, Dict, Any

class MySource(BaseSource):
    def extract(self) -> List[Dict[str, Any]]:
        # Your extraction logic
        return data
```

2. Register in `dags/factories/source_factory.py`:

```python
SourceFactory.register("my_source", MySource)
```

3. Use in config:

```json
{"source": {"type": "my_source", "param": "value"}}
```

**For complete guides on adding custom sources, transforms, and destinations with best practices and examples, see [docs/EXTENDING.md](docs/EXTENDING.md)**

## Troubleshooting

### DAGs Not Appearing

**Issue:** JSON config created but DAG doesn't appear in UI

**Solutions:**
- Verify JSON file is in `dags/configs/` directory
- Check JSON syntax: `python -m json.tool dags/configs/your_config.json`
- View DAG processor logs: `docker compose -f docker-compose.dev.yaml logs airflow-dag-processor`
- Wait 30-60 seconds for DAG refresh
- Check for Python errors in logs

### Connection Errors

**Google Sheets: "Permission Denied"**
- Verify sheet is shared with service account email
- Check Google Sheets API is enabled in GCP
- Ensure JSON key is valid and properly formatted

**Shopify: "Authentication failed"**
- Verify access token is complete (starts with `shpat_`)
- Check app is installed on the shop
- Ensure required API scopes are configured

**PostgreSQL: "Connection Refused"**
- Use host `postgres` (not `localhost`) for Docker Compose
- Verify PostgreSQL service is running: `docker compose -f docker-compose.dev.yaml ps`
- Check credentials match docker-compose.yaml

### Task Failures

**Check task logs:**
1. Click on task box in Airflow UI
2. Click "Logs" tab
3. Review error messages

**Common issues:**
- Source data format doesn't match expected structure
- Transform configurations are incorrect
- Destination table/schema doesn't exist (use `create_table: true`)
- Missing permissions on destination database

### Performance Issues

**Large dataset loading slowly:**
- PostgreSQL destination automatically uses temp tables for >1000 rows
- Adjust `temp_table_threshold` if needed
- Consider reducing `limit` for Shopify sources
- Space out DAG schedules to avoid resource contention

**Shopify rate limiting:**
- ShopifyHook automatically handles rate limits
- Reduce `limit` parameter to fetch fewer records per page
- Space out DAG schedules

## Production Deployment

### Environment Configuration

1. Create production `.env` file:

```bash
AIRFLOW_UID=50000
AIRFLOW_GID=0
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=<strong_password>
POSTGRES_USER=airflow
POSTGRES_PASSWORD=<strong_password>
POSTGRES_DB=airflow
```

2. Update `docker-compose.dev.yaml` for production:
   - Change executor to CeleryExecutor (already configured)
   - Set strong passwords
   - Configure volume mounts for persistence
   - Enable SSL/TLS for web server

### Secrets Management

Use Airflow secrets backend for production:

**AWS Secrets Manager:**
```yaml
environment:
  AIRFLOW__SECRETS__BACKEND: airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
  AIRFLOW__SECRETS__BACKEND_KWARGS: '{"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables"}'
```

**HashiCorp Vault:**
```yaml
environment:
  AIRFLOW__SECRETS__BACKEND: airflow.providers.hashicorp.secrets.vault.VaultBackend
  AIRFLOW__SECRETS__BACKEND_KWARGS: '{"url": "https://vault.example.com", "token": "..."}'
```

### Monitoring

1. Enable Flower (Celery monitoring):
   ```bash
   docker compose -f docker-compose.dev.yaml --profile flower up -d
   ```
   Access at http://localhost:5555

2. Monitor logs:
   ```bash
   docker compose -f docker-compose.dev.yaml logs -f
   ```

3. Set up alerts for:
   - Task failures
   - Connection errors
   - API rate limiting
   - Resource usage

### Backup & Recovery

**Backup Airflow metadata database:**
```bash
docker compose -f docker-compose.dev.yaml exec postgres \
  pg_dump -U airflow airflow > backup.sql
```

**Backup DAG configs:**
```bash
tar -czf configs_backup.tar.gz dags/configs/
```

**Restore:**
```bash
docker compose -f docker-compose.dev.yaml exec -T postgres \
  psql -U airflow airflow < backup.sql
```

### Scaling

The CeleryExecutor configuration supports horizontal scaling:

1. Add more worker containers:
   ```yaml
   airflow-worker-2:
     <<: *airflow-common
     command: celery worker
   ```

2. Increase worker concurrency:
   ```yaml
   environment:
     AIRFLOW__CELERY__WORKER_CONCURRENCY: 16
   ```

3. Add Redis replicas for high availability

## Useful Commands

```bash
# View all logs
docker compose -f docker-compose.dev.yaml logs -f

# View specific service logs
docker compose -f docker-compose.dev.yaml logs -f airflow-scheduler

# Restart services
docker compose -f docker-compose.dev.yaml restart

# Stop Airflow
docker compose -f docker-compose.dev.yaml down

# Clean up (remove all data and volumes)
docker compose -f docker-compose.dev.yaml down -v

# List DAGs
docker compose -f docker-compose.dev.yaml exec airflow-api-server airflow dags list

# Test a specific DAG
docker compose -f docker-compose.dev.yaml exec airflow-api-server \
  airflow dags test my_dag_id 2025-01-01

# List connections
docker compose -f docker-compose.dev.yaml exec airflow-api-server \
  airflow connections list
```

## Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow 3.0 Migration Guide](https://airflow.apache.org/docs/apache-airflow/stable/upgrading-to-3.html)
- [Google Sheets API](https://developers.google.com/sheets/api)
- [Shopify Admin API](https://shopify.dev/docs/api/admin-rest)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

## License

This project is provided as-is for educational and commercial use.
