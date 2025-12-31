"""Core DAG builder for ETL pipelines using Airflow TaskFlow API."""

from typing import Any, Dict, List

from airflow.sdk.definitions.decorators import task

from factories.destination_factory import DestinationFactory
from factories.source_factory import SourceFactory
from factories.transform_factory import TransformFactory


class DAGBuilder:
    """
    Core ETL orchestration builder using Airflow TaskFlow API.

    This class provides the building blocks for creating ETL pipelines by:
    1. Extracting data from a source
    2. Applying a chain of transformations
    3. Loading data to a destination

    All operations use Airflow's TaskFlow API with the @task decorator
    for automatic XCom serialization and task dependency management.
    """

    @staticmethod
    @task
    def extract_data(source_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract data from configured source.

        This is a TaskFlow task that creates a source instance using the
        SourceFactory and extracts data.

        Args:
            source_config: Source configuration dictionary with 'type' field

        Returns:
            List[Dict[str, Any]]: Extracted data as list of dictionaries

        Example:
            >>> source_config = {
            ...     "type": "gsheet",
            ...     "spreadsheet_id": "abc123",
            ...     "range_name": "Sheet1!A1:Z100"
            ... }
            >>> data = extract_data(source_config)
        """
        source = SourceFactory.create(source_config)
        data = source.extract()

        print(f"Extracted {len(data)} rows from {source_config['type']} source")
        return data

    @staticmethod
    @task
    def transform_data(
        data: List[Dict[str, Any]], transform_configs: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Apply a chain of transformations to data.

        This is a TaskFlow task that creates transform instances using the
        TransformFactory and applies them sequentially.

        Args:
            data: Input data to transform
            transform_configs: List of transform configuration dictionaries

        Returns:
            List[Dict[str, Any]]: Transformed data

        Example:
            >>> data = [{"First Name": "John", "Status": "active"}]
            >>> transform_configs = [
            ...     {"type": "normalize_columns"},
            ...     {"type": "filter_rows", "column": "status", "operator": "eq", "value": "active"}
            ... ]
            >>> transformed = transform_data(data, transform_configs)
        """
        if not transform_configs:
            print("No transformations configured, passing data through")
            return data

        result = data
        for i, transform_config in enumerate(transform_configs):
            transform = TransformFactory.create(transform_config)
            result = transform.transform(result)
            print(
                f"Transform {i + 1}/{len(transform_configs)} ({transform_config['type']}): "
                f"{len(result)} rows"
            )

        return result

    @staticmethod
    @task
    def load_data(
        data: List[Dict[str, Any]], destination_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Load data to configured destination.

        This is a TaskFlow task that creates a destination instance using the
        DestinationFactory and loads data with pre/post hooks.

        Args:
            data: Data to load
            destination_config: Destination configuration dictionary with 'type' field

        Returns:
            Dict[str, Any]: Load result metadata (rows_loaded, status, duration, etc.)

        Example:
            >>> data = [{"id": 1, "name": "John"}]
            >>> destination_config = {
            ...     "type": "postgres",
            ...     "table_name": "users",
            ...     "write_mode": "append"
            ... }
            >>> result = load_data(data, destination_config)
        """
        destination = DestinationFactory.create(destination_config)

        # Execute pre-load hook
        destination.pre_load_hook(data)

        # Load data
        result = destination.load(data)

        # Execute post-load hook
        destination.post_load_hook(result)

        print(
            f"Loaded {result.get('rows_loaded', 0)} rows to "
            f"{destination_config['type']} destination - Status: {result.get('status')}"
        )

        return result

    @classmethod
    def build_etl_pipeline(
        cls,
        source_config: Dict[str, Any],
        transform_configs: List[Dict[str, Any]],
        destination_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Build a complete ETL pipeline with extract, transform, and load tasks.

        This method wires together the three TaskFlow tasks with proper dependencies:
        extract >> transform >> load

        Args:
            source_config: Source configuration
            transform_configs: List of transform configurations
            destination_config: Destination configuration

        Returns:
            Dict[str, Any]: The final load result from the load_data task

        Example usage in a DAG:
            with DAG(...) as dag:
                result = DAGBuilder.build_etl_pipeline(
                    source_config={"type": "gsheet", ...},
                    transform_configs=[{"type": "normalize_columns"}],
                    destination_config={"type": "postgres", ...}
                )
        """
        # Create task instances
        extracted_data = cls.extract_data(source_config)
        transformed_data = cls.transform_data(extracted_data, transform_configs)
        load_result = cls.load_data(transformed_data, destination_config)

        # Return the final result (TaskFlow handles dependencies automatically)
        return load_result
