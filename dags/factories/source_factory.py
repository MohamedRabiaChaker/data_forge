"""Factory for creating data source instances."""
from typing import Any, Dict

from tasks.sources.base_source import BaseSource
from tasks.sources.gsheet_source import GoogleSheetsSource
from tasks.sources.shopify_source import ShopifySource


class SourceFactory:
    """
    Factory class for creating data source instances based on configuration.
    
    This factory implements the Factory Pattern to instantiate the appropriate
    source class based on the 'type' field in the configuration.
    
    Supported source types:
        - 'gsheet': Google Sheets source
        
    Example usage:
        config = {
            "type": "gsheet",
            "gcp_conn_id": "google_cloud_default",
            "spreadsheet_id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
            "range_name": "Sheet1!A1:Z1000"
        }
        source = SourceFactory.create(config)
        data = source.extract()
    """
    
    # Registry mapping source type strings to implementation classes
    _registry = {
        'gsheet': GoogleSheetsSource,
        'google_sheets': GoogleSheetsSource,
        'shopify': ShopifySource,
    }
    
    @classmethod
    def create(cls, config: Dict[str, Any]) -> BaseSource:
        """
        Create a source instance based on configuration.
        
        Args:
            config: Dictionary containing source configuration with 'type' field
        
        Returns:
            BaseSource: Instance of the appropriate source implementation
        
        Raises:
            ValueError: If 'type' field is missing or unsupported
        
        Example:
            >>> config = {"type": "gsheet", "spreadsheet_id": "abc123", "range_name": "Sheet1"}
            >>> source = SourceFactory.create(config)
            >>> isinstance(source, GoogleSheetsSource)
            True
        """
        if 'type' not in config:
            raise ValueError("Source configuration must include 'type' field")
        
        source_type = config['type'].lower()
        
        if source_type not in cls._registry:
            available_types = ', '.join(cls._registry.keys())
            raise ValueError(
                f"Unknown source type '{source_type}'. "
                f"Available types: {available_types}"
            )
        
        # Get the appropriate source class and instantiate it
        source_class = cls._registry[source_type]
        return source_class(config)
    
    @classmethod
    def register(cls, source_type: str, source_class: type) -> None:
        """
        Register a new source type in the factory.
        
        This allows for dynamic extension of the factory with custom source implementations.
        
        Args:
            source_type: String identifier for the source type
            source_class: Class implementing BaseSource interface
        
        Example:
            >>> class CustomSource(BaseSource):
            ...     pass
            >>> SourceFactory.register('custom', CustomSource)
        """
        if not issubclass(source_class, BaseSource):
            raise ValueError(
                f"Source class must inherit from BaseSource, got {source_class.__name__}"
            )
        
        cls._registry[source_type.lower()] = source_class
    
    @classmethod
    def get_supported_types(cls) -> list:
        """
        Get list of supported source types.
        
        Returns:
            list: List of supported source type strings
        
        Example:
            >>> SourceFactory.get_supported_types()
            ['gsheet', 'google_sheets']
        """
        return list(cls._registry.keys())
