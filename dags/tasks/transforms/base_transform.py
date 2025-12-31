"""Base abstract class for all data transformations."""
from abc import ABC, abstractmethod
from typing import Any, Dict, List


class BaseTransform(ABC):
    """
    Abstract base class for data transformations in the ETL framework.
    
    All transform implementations must inherit from this class and implement
    the transform() method to manipulate data between extraction and loading.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the transform with configuration.
        
        Args:
            config: Dictionary containing transform-specific configuration
                    (e.g., transformation rules, field mappings, validation rules)
        """
        self.config = config
    
    @abstractmethod
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform the input data.
        
        This method must be implemented by all concrete transform classes.
        It receives data from the source (or previous transform) and returns
        transformed data to be passed to the next transform or destination.
        
        Args:
            data: List of dictionaries representing rows of data to transform
        
        Returns:
            List[Dict[str, Any]]: Transformed data in the same format (list of dicts)
        
        Raises:
            Exception: Implementation-specific exceptions for transformation
                      failures, validation errors, or data quality issues.
        
        Example:
            >>> transform = NormalizeColumnsTransform(config)
            >>> input_data = [{'First Name': 'John', 'Last Name': 'Doe'}]
            >>> output_data = transform.transform(input_data)
            >>> print(output_data[0])
            {'first_name': 'John', 'last_name': 'Doe'}
        """
        pass
    
    def validate_data(self, data: List[Dict[str, Any]]) -> None:
        """
        Validate the input data before transformation.
        
        Override this method in concrete implementations to add custom
        validation logic (e.g., required fields, data types, ranges).
        
        Args:
            data: List of dictionaries to validate
        
        Raises:
            ValueError: If data validation fails
        """
        if not isinstance(data, list):
            raise ValueError("Data must be a list of dictionaries")
        
        if not all(isinstance(row, dict) for row in data):
            raise ValueError("All data rows must be dictionaries")
