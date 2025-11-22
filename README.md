# Airflow Docker Development Environment

A minimal Apache Airflow 3.1.0 development setup using Docker Compose.

## Important Notes

This setup uses **Airflow 3.1.0** which introduces significant changes from Airflow 2.x:
- New SDK-based imports (`from airflow.sdk import dag, task`)
- DAG bundling feature (may require additional configuration for custom DAGs)
- Updated API endpoints and authentication
- See [Airflow 3.0 Migration Guide](https://airflow.apache.org/docs/apache-airflow/stable/upgrading-to-3.html) for details

## Quick Start

1. **Start Airflow**
   ```bash
   docker compose -f docker-compose.dev.yaml up -d
   ```

2. **Access the Airflow UI**
   - URL: http://localhost:8081
   - Username: `airflow`
   - Password: `airflow`
   - Wait 30-60 seconds after startup for the UI to become fully available

3. **Stop Airflow**
   ```bash
   docker compose -f docker-compose.dev.yaml down
   ```

4. **Clean up (remove all data and volumes)**
   ```bash
   docker compose -f docker-compose.dev.yaml down -v
   ```

## Project Structure

```
data_forge/
├── dags/               # Place your DAG files here
├── plugins/            # Place custom Airflow plugins here
├── logs/               # Airflow logs (auto-generated)
├── docker-compose.dev.yaml  # Main compose configuration
├── Dockerfile          # Custom Airflow image
├── requirements.txt    # Python dependencies
└── .env               # Environment variables (create from .env.example)
```

## Adding DAGs

### Airflow 3.x DAG Structure

Airflow 3.1.0 uses a new SDK-based approach for defining DAGs:

```python
from datetime import datetime, timedelta
from airflow.sdk import dag, task

@dag(
    dag_id='my_example_dag',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example'],
)
def my_example_dag():
    @task
    def my_task():
        print("Hello from Airflow 3!")
        return "success"
    
    my_task()

# Instantiate the DAG
my_dag = my_example_dag()
```

### Deployment Steps

1. Create Python files in the `dags/` directory
2. Ensure files use Airflow 3.x imports (`airflow.sdk` instead of `airflow.decorators`)
3. Airflow will process DAGs based on the DAG bundle configuration
4. View and manage DAGs in the web UI at http://localhost:8081

**Note**: Airflow 3.0+ uses a "DAG bundling" system. The default `dags/` folder is configured as a bundle that refreshes periodically. If DAGs don't appear immediately, check the DAG processor logs.

## Adding Python Dependencies

The `requirements.txt` file includes minimal required dependencies for Airflow 3.1.0 to function:
- `psycopg2-binary` - PostgreSQL database driver
- `asyncpg` - Async PostgreSQL driver (required by Airflow 3.x)
- `apache-airflow-providers-celery>=3.3.0` - Celery executor support
- `apache-airflow-providers-fab` - Flask AppBuilder authentication manager

To add your own dependencies:

1. Add packages to `requirements.txt`
2. Rebuild the Docker image:
   ```bash
   docker compose -f docker-compose.dev.yaml build
   docker compose -f docker-compose.dev.yaml up -d
   ```

## Configuration

The setup includes:
- **Executor**: CeleryExecutor (scalable, distributed task execution)
- **Database**: PostgreSQL 13 (metadata store)
- **Message Broker**: Redis 7.2 (task queue)
- **Services**:
  - API Server (Web UI) - Port 8081
  - Scheduler (DAG scheduling)
  - Worker (Task execution)
  - Triggerer (Async task support)
  - DAG Processor (DAG parsing)
  - Flower (Celery monitoring - disabled by default)

## Customization

- **Environment variables**: Edit `.env` file (copy from `.env.example`)
- **Airflow config**: Set `AIRFLOW__SECTION__KEY` environment variables in compose file
- **Ports**: Modify port mappings in `docker-compose.dev.yaml`

## Useful Commands

```bash
# View logs
docker compose -f docker-compose.dev.yaml logs -f

# View logs for specific service
docker compose -f docker-compose.dev.yaml logs -f airflow-scheduler

# Restart services
docker compose -f docker-compose.dev.yaml restart

# Execute Airflow CLI commands
docker compose -f docker-compose.dev.yaml exec airflow-api-server airflow dags list

# Access Celery Flower (monitoring)
docker compose -f docker-compose.dev.yaml --profile flower up -d
# Then visit http://localhost:5555
```

## Troubleshooting

**DAGs not appearing?**
- **Airflow 3.x uses DAG bundling**: Check if DAG processor is scanning the dags folder
- View DAG processor logs: `docker compose -f docker-compose.dev.yaml logs airflow-dag-processor`
- View scheduler logs: `docker compose -f docker-compose.dev.yaml logs airflow-scheduler`
- Ensure DAG files use Airflow 3.x imports (`from airflow.sdk import dag, task`)
- Check for syntax errors: `docker compose -f docker-compose.dev.yaml exec airflow-scheduler python /opt/airflow/dags/your_dag.py`
- Verify files are in container: `docker compose -f docker-compose.dev.yaml exec airflow-scheduler ls -la /opt/airflow/dags/`

**Web UI not loading or timing out?**
- Wait 30-60 seconds after `docker compose up` for all services to initialize
- Check API server health: `docker compose -f docker-compose.dev.yaml ps`
- View API server logs: `docker compose -f docker-compose.dev.yaml logs airflow-api-server`

**Permission errors?**
- Set `AIRFLOW_UID` in `.env` to match your user ID
- Run: `echo "AIRFLOW_UID=$(id -u)" >> .env`
- Restart containers: `docker compose -f docker-compose.dev.yaml down && docker compose -f docker-compose.dev.yaml up -d`

**Port 8081 already in use?**
- Change port mapping in `docker-compose.dev.yaml` (e.g., `"8082:8080"`)

**Old DAGs appearing after cleanup?**
- Remove volumes completely: `docker compose -f docker-compose.dev.yaml down -v`
- Delete local logs: `rm -rf logs/`
- Rebuild images: `docker compose -f docker-compose.dev.yaml build --no-cache`

## Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow Docker Setup](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Writing DAGs](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html)
