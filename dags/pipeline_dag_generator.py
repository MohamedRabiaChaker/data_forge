"""
Dynamic DAG generator that reads JSON configs and creates Airflow DAGs.

This module scans the dags/configs/ directory for JSON configuration files
and automatically generates Airflow DAGs using the DAGBuilder class.

Each JSON file in the configs directory will create one DAG in Airflow.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

from airflow import DAG

from dag_builder import DAGBuilder


def parse_date(date_str: str) -> datetime:
    """
    Parse date string to datetime object.

    Args:
        date_str: Date string in format 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'

    Returns:
        datetime: Parsed datetime object
    """
    try:
        # Try full datetime format first
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        # Fall back to date-only format
        return datetime.strptime(date_str, "%Y-%m-%d")


def parse_retry_delay(minutes: int) -> timedelta:
    """
    Convert minutes to timedelta for retry_delay.

    Args:
        minutes: Number of minutes to wait between retries

    Returns:
        timedelta: Retry delay as timedelta object
    """
    return timedelta(minutes=minutes)


def load_pipeline_config(config_path: str) -> Dict[str, Any]:
    """
    Load and validate pipeline configuration from JSON file.

    Args:
        config_path: Path to JSON configuration file

    Returns:
        Dict[str, Any]: Parsed configuration dictionary

    Raises:
        ValueError: If required fields are missing
        json.JSONDecodeError: If JSON is invalid
    """
    with open(config_path, "r") as f:
        config = json.load(f)

    # Validate required fields
    required_fields = ["dag_id", "source", "destination"]
    missing_fields = [field for field in required_fields if field not in config]

    if missing_fields:
        raise ValueError(
            f"Config {config_path} is missing required fields: {', '.join(missing_fields)}"
        )

    return config


def create_dag_from_config(config: Dict[str, Any]) -> DAG:
    """
    Create an Airflow DAG from a configuration dictionary.

    This function:
    1. Extracts DAG parameters (schedule, start_date, etc.)
    2. Creates a DAG context with default_args
    3. Builds the ETL pipeline using DAGBuilder
    4. Returns the DAG object for Airflow registration

    Args:
        config: Configuration dictionary loaded from JSON

    Returns:
        DAG: Configured Airflow DAG with ETL pipeline

    Example config structure:
        {
            "dag_id": "my_etl_pipeline",
            "description": "ETL pipeline description",
            "schedule": "@daily",
            "start_date": "2025-01-01",
            "catchup": false,
            "tags": ["etl", "example"],
            "default_args": {
                "owner": "airflow",
                "retries": 2,
                "retry_delay_minutes": 5
            },
            "source": {...},
            "transforms": [...],
            "destination": {...}
        }
    """
    # Extract DAG parameters
    dag_id = config["dag_id"]
    description = config.get("description", f"ETL pipeline: {dag_id}")
    schedule = config.get("schedule", config.get("schedule_interval", None))  # Support both for backward compatibility
    start_date = parse_date(config.get("start_date", "2025-01-01"))
    catchup = config.get("catchup", False)
    tags = config.get("tags", ["etl", "auto-generated"])

    # Build default_args
    default_args_config = config.get("default_args", {})
    default_args = {
        "owner": default_args_config.get("owner", "airflow"),
        "depends_on_past": default_args_config.get("depends_on_past", False),
        "retries": default_args_config.get("retries", 1),
    }

    # Add retry_delay if specified
    if "retry_delay_minutes" in default_args_config:
        default_args["retry_delay"] = parse_retry_delay(
            default_args_config["retry_delay_minutes"]
        )

    # Extract ETL configuration
    source_config = config["source"]
    transform_configs = config.get("transforms", [])
    destination_config = config["destination"]

    # Create the DAG
    dag = DAG(
        dag_id=dag_id,
        description=description,
        schedule=schedule,
        start_date=start_date,
        catchup=catchup,
        tags=tags,
        default_args=default_args,
    )

    # Build ETL pipeline within DAG context
    with dag:
        DAGBuilder.build_etl_pipeline(
            source_config=source_config,
            transform_configs=transform_configs,
            destination_config=destination_config,
        )

    return dag


def generate_dags_from_configs(configs_dir_path: str | None = None) -> Dict[str, DAG]:
    """
    Scan configs directory and generate DAGs for all JSON files.

    This function is the main entry point for dynamic DAG generation.
    It scans the configs directory, loads each JSON file, and creates
    a DAG for each valid configuration.

    Args:
        configs_dir_path: Path to configs directory. If None, uses dags/configs/

    Returns:
        Dict[str, DAG]: Dictionary mapping dag_id to DAG object

    Note:
        This function will log errors for invalid configs but continue
        processing other files. Invalid DAGs will not be registered.
    """
    # Determine configs directory
    if configs_dir_path is None:
        # Get the directory where this script is located (dags/)
        dags_dir = Path(__file__).parent
        configs_dir = dags_dir / "configs"
    else:
        configs_dir = Path(configs_dir_path)

    # Check if configs directory exists
    if not configs_dir.exists():
        print(f"Configs directory not found: {configs_dir}")
        print("Creating configs directory...")
        configs_dir.mkdir(parents=True, exist_ok=True)
        return {}

    # Find all JSON files in configs directory
    config_files = list(configs_dir.glob("*.json"))

    if not config_files:
        print(f"No JSON config files found in {configs_dir}")
        return {}

    print(f"Found {len(config_files)} config file(s) in {configs_dir}")

    # Generate DAGs
    dags = {}
    for config_file in config_files:
        try:
            print(f"Loading config: {config_file.name}")
            config = load_pipeline_config(str(config_file))
            dag = create_dag_from_config(config)
            dags[dag.dag_id] = dag
            print(f"  ✓ Generated DAG: {dag.dag_id}")
        except Exception as e:
            print(f"  ✗ Error generating DAG from {config_file.name}: {e}")
            # Continue processing other files
            continue

    print(f"\nSuccessfully generated {len(dags)} DAG(s)")
    return dags


# ============================================================================
# Auto-register DAGs with Airflow
# ============================================================================
# When Airflow scans this file, it will automatically execute this code
# and register all generated DAGs in the global namespace.

# Generate all DAGs from configs directory
_generated_dags = generate_dags_from_configs()

# Register DAGs in global namespace for Airflow to pick up
# This is required for Airflow to discover the DAGs
for dag_id, dag_obj in _generated_dags.items():
    globals()[dag_id] = dag_obj
