"""Factory for creating data transform instances."""

from typing import Any, Dict

from tasks.transforms.base_transform import BaseTransform
from tasks.transforms.common_transforms import (
    FilterRowsTransform,
    NormalizeColumnsTransform,
    SelectColumnsTransform,
)
from tasks.transforms.validation_transforms import (
    ValidateDuplicateIdsTransform,
    ValidateRequiredFieldsTransform,
    ValidateDataTypesTransform,
)


class TransformFactory:
    """
    Factory class for creating data transform instances based on configuration.

    This factory implements the Factory Pattern to instantiate the appropriate
    transform class based on the 'type' field in the configuration.

    Supported transform types:
        - 'normalize_columns': Normalize column names to snake_case
        - 'filter_rows': Filter rows based on conditions
        - 'select_columns': Select or exclude specific columns
        - 'validate_duplicate_ids': Validate uniqueness of ID columns
        - 'validate_required_fields': Validate presence of required fields
        - 'validate_data_types': Validate and coerce data types

    Example usage:
        config = {
            "type": "normalize_columns",
            "lowercase": True,
            "replace_spaces": "_"
        }
        transform = TransformFactory.create(config)
        transformed_data = transform.transform(data)
    """

    # Registry mapping transform type strings to implementation classes
    _registry = {
        "normalize_columns": NormalizeColumnsTransform,
        "normalize": NormalizeColumnsTransform,
        "snake_case": NormalizeColumnsTransform,
        "filter_rows": FilterRowsTransform,
        "filter": FilterRowsTransform,
        "select_columns": SelectColumnsTransform,
        "select": SelectColumnsTransform,
        "validate_duplicate_ids": ValidateDuplicateIdsTransform,
        "validate_required_fields": ValidateRequiredFieldsTransform,
        "validate_data_types": ValidateDataTypesTransform,
    }

    @classmethod
    def create(cls, config: Dict[str, Any]) -> BaseTransform:
        """
        Create a transform instance based on configuration.

        Args:
            config: Dictionary containing transform configuration with 'type' field

        Returns:
            BaseTransform: Instance of the appropriate transform implementation

        Raises:
            ValueError: If 'type' field is missing or unsupported

        Example:
            >>> config = {"type": "normalize_columns", "lowercase": True}
            >>> transform = TransformFactory.create(config)
            >>> isinstance(transform, NormalizeColumnsTransform)
            True
        """
        if "type" not in config:
            raise ValueError("Transform configuration must include 'type' field")

        transform_type = config["type"].lower()

        if transform_type not in cls._registry:
            available_types = ", ".join(set(cls._registry.keys()))
            raise ValueError(
                f"Unknown transform type '{transform_type}'. "
                f"Available types: {available_types}"
            )

        # Get the appropriate transform class and instantiate it
        transform_class = cls._registry[transform_type]
        return transform_class(config)

    @classmethod
    def create_chain(cls, configs: list) -> list:
        """
        Create a chain of transforms from a list of configurations.

        This is useful for building multi-step transformation pipelines.

        Args:
            configs: List of transform configuration dictionaries

        Returns:
            list: List of instantiated transform instances

        Example:
            >>> configs = [
            ...     {"type": "normalize_columns"},
            ...     {"type": "filter_rows", "column": "status", "operator": "eq", "value": "active"}
            ... ]
            >>> transforms = TransformFactory.create_chain(configs)
            >>> len(transforms)
            2
        """
        return [cls.create(config) for config in configs]

    @classmethod
    def register(cls, transform_type: str, transform_class: type) -> None:
        """
        Register a new transform type in the factory.

        This allows for dynamic extension of the factory with custom transform implementations.

        Args:
            transform_type: String identifier for the transform type
            transform_class: Class implementing BaseTransform interface

        Example:
            >>> class CustomTransform(BaseTransform):
            ...     pass
            >>> TransformFactory.register('custom', CustomTransform)
        """
        if not issubclass(transform_class, BaseTransform):
            raise ValueError(
                f"Transform class must inherit from BaseTransform, got {transform_class.__name__}"
            )

        cls._registry[transform_type.lower()] = transform_class

    @classmethod
    def get_supported_types(cls) -> list:
        """
        Get list of supported transform types.

        Returns:
            list: List of unique supported transform type strings

        Example:
            >>> types = TransformFactory.get_supported_types()
            >>> 'normalize_columns' in types
            True
        """
        return list(set(cls._registry.keys()))
