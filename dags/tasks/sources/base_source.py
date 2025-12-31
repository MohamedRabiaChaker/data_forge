"""Base abstract class for all data sources."""
from abc import ABC, abstractmethod
from typing import Any, Dict, List


class BaseSource(ABC):
    """
    Abstract base class for data sources in the ETL framework.
    
    All source implementations must inherit from this class and implement
    the extract() method to retrieve data from their respective systems.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the source with configuration.
        
        Args:
            config: Dictionary containing source-specific configuration
                    (e.g., connection IDs, file paths, API endpoints)
        """
        self.config = config
    
    @abstractmethod
    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract data from the source system.
        
        This method must be implemented by all concrete source classes.
        It should handle connection management, data retrieval, and
        error handling specific to the source type.
        
        Returns:
            List[Dict[str, Any]]: List of dictionaries where each dictionary
                                  represents a row of data with column names
                                  as keys.
        
        Raises:
            Exception: Implementation-specific exceptions for connection
                      failures, authentication errors, or data retrieval issues.
        
        Example:
            >>> source = GoogleSheetsSource(config)
            >>> data = source.extract()
            >>> print(data[0])
            {'column1': 'value1', 'column2': 'value2'}
        """
        pass
    
    def validate_config(self) -> None:
        """
        Validate the source configuration.
        
        Override this method in concrete implementations to add custom
        validation logic for required configuration parameters.
        
        Raises:
            ValueError: If required configuration parameters are missing
                       or invalid.
        """
        pass
