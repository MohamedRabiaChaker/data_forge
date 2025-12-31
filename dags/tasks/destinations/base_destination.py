"""Base abstract class for all data destinations."""
from abc import ABC, abstractmethod
from typing import Any, Dict, List


class BaseDestination(ABC):
    """
    Abstract base class for data destinations in the ETL framework.
    
    All destination implementations must inherit from this class and implement
    the load() method to write data to their respective target systems.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the destination with configuration.
        
        Args:
            config: Dictionary containing destination-specific configuration
                    (e.g., connection IDs, table names, write modes, credentials)
        """
        self.config = config
    
    @abstractmethod
    def load(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Load data into the destination system.
        
        This method must be implemented by all concrete destination classes.
        It should handle connection management, data writing, transaction
        management, and error handling specific to the destination type.
        
        Args:
            data: List of dictionaries representing rows of data to load.
                  Each dictionary has column names as keys.
        
        Returns:
            Dict[str, Any]: Metadata about the load operation including:
                - rows_loaded: Number of rows successfully loaded
                - status: 'success' or 'partial_success' or 'failed'
                - errors: List of any errors encountered (optional)
                - duration_seconds: Time taken for the load (optional)
        
        Raises:
            Exception: Implementation-specific exceptions for connection
                      failures, write errors, or constraint violations.
        
        Example:
            >>> destination = PostgresDestination(config)
            >>> data = [{'id': 1, 'name': 'John'}, {'id': 2, 'name': 'Jane'}]
            >>> result = destination.load(data)
            >>> print(result)
            {'rows_loaded': 2, 'status': 'success', 'duration_seconds': 1.5}
        """
        pass
    
    def validate_config(self) -> None:
        """
        Validate the destination configuration.
        
        Override this method in concrete implementations to add custom
        validation logic for required configuration parameters.
        
        Raises:
            ValueError: If required configuration parameters are missing
                       or invalid.
        """
        pass
    
    def pre_load_hook(self, data: List[Dict[str, Any]]) -> None:
        """
        Execute operations before loading data.
        
        Override this method to perform setup operations like:
        - Creating tables if they don't exist
        - Truncating tables
        - Creating temp tables
        - Validating schema compatibility
        
        Args:
            data: The data that will be loaded (useful for schema inference)
        """
        pass
    
    def post_load_hook(self, result: Dict[str, Any]) -> None:
        """
        Execute operations after loading data.
        
        Override this method to perform cleanup operations like:
        - Dropping temp tables
        - Running data quality checks
        - Updating metadata tables
        - Sending notifications
        
        Args:
            result: The result dictionary from the load() method
        """
        pass
