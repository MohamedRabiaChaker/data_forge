"""Factory for creating data destination instances."""
from typing import Any, Dict

from tasks.destinations.base_destination import BaseDestination
from tasks.destinations.postgres_destination import PostgresDestination


class DestinationFactory:
    """
    Factory class for creating data destination instances based on configuration.
    
    This factory implements the Factory Pattern to instantiate the appropriate
    destination class based on the 'type' field in the configuration.
    
    Supported destination types:
        - 'postgres': PostgreSQL database destination
        
    Example usage:
        config = {
            "type": "postgres",
            "postgres_conn_id": "postgres_default",
            "schema": "public",
            "table_name": "sales_data",
            "write_mode": "replace"
        }
        destination = DestinationFactory.create(config)
        result = destination.load(data)
    """
    
    # Registry mapping destination type strings to implementation classes
    _registry = {
        'postgres': PostgresDestination,
        'postgresql': PostgresDestination,
        'pg': PostgresDestination,
    }
    
    @classmethod
    def create(cls, config: Dict[str, Any]) -> BaseDestination:
        """
        Create a destination instance based on configuration.
        
        Args:
            config: Dictionary containing destination configuration with 'type' field
        
        Returns:
            BaseDestination: Instance of the appropriate destination implementation
        
        Raises:
            ValueError: If 'type' field is missing or unsupported
        
        Example:
            >>> config = {"type": "postgres", "table_name": "my_table", "schema": "public"}
            >>> destination = DestinationFactory.create(config)
            >>> isinstance(destination, PostgresDestination)
            True
        """
        if 'type' not in config:
            raise ValueError("Destination configuration must include 'type' field")
        
        destination_type = config['type'].lower()
        
        if destination_type not in cls._registry:
            available_types = ', '.join(cls._registry.keys())
            raise ValueError(
                f"Unknown destination type '{destination_type}'. "
                f"Available types: {available_types}"
            )
        
        # Get the appropriate destination class and instantiate it
        destination_class = cls._registry[destination_type]
        return destination_class(config)
    
    @classmethod
    def register(cls, destination_type: str, destination_class: type) -> None:
        """
        Register a new destination type in the factory.
        
        This allows for dynamic extension of the factory with custom destination implementations.
        
        Args:
            destination_type: String identifier for the destination type
            destination_class: Class implementing BaseDestination interface
        
        Example:
            >>> class CustomDestination(BaseDestination):
            ...     pass
            >>> DestinationFactory.register('custom', CustomDestination)
        """
        if not issubclass(destination_class, BaseDestination):
            raise ValueError(
                f"Destination class must inherit from BaseDestination, got {destination_class.__name__}"
            )
        
        cls._registry[destination_type.lower()] = destination_class
    
    @classmethod
    def get_supported_types(cls) -> list:
        """
        Get list of supported destination types.
        
        Returns:
            list: List of supported destination type strings
        
        Example:
            >>> DestinationFactory.get_supported_types()
            ['postgres', 'postgresql', 'pg']
        """
        return list(cls._registry.keys())
