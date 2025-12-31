"""Common data transformation implementations."""
import re
from typing import Any, Dict, List

try:
    from .base_transform import BaseTransform
except ImportError:
    from base_transform import BaseTransform


class NormalizeColumnsTransform(BaseTransform):
    """
    Normalize column names to snake_case format.
    
    This transform converts column names from various formats (camelCase, PascalCase,
    spaces, hyphens) into standardized snake_case format suitable for database columns.
    
    Configuration:
        lowercase (bool): Convert to lowercase (default: True)
        replace_spaces (str): Character to replace spaces with (default: '_')
        remove_special_chars (bool): Remove special characters (default: True)
        max_length (int): Maximum column name length, None for no limit (default: None)
        
    Example config:
        {
            "type": "normalize_columns",
            "lowercase": True,
            "replace_spaces": "_",
            "remove_special_chars": True
        }
    
    Examples:
        'First Name' -> 'first_name'
        'Email Address' -> 'email_address'
        'customer-ID' -> 'customer_id'
        'phoneNumber' -> 'phone_number'
        'TotalAmount' -> 'total_amount'
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize column normalization transform."""
        super().__init__(config)
        
        # Get configuration with defaults
        self.lowercase = config.get('lowercase', True)
        self.replace_spaces = config.get('replace_spaces', '_')
        self.remove_special_chars = config.get('remove_special_chars', True)
        self.max_length = config.get('max_length', None)
    
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform data by normalizing all column names.
        
        Args:
            data: List of dictionaries with potentially non-standard column names
        
        Returns:
            List[Dict[str, Any]]: Same data with normalized column names
        
        Example:
            Input:  [{'First Name': 'John', 'Email Address': 'john@example.com'}]
            Output: [{'first_name': 'John', 'email_address': 'john@example.com'}]
        """
        self.validate_data(data)
        
        if not data:
            return []
        
        # Build column name mapping
        original_columns = list(data[0].keys())
        normalized_mapping = {
            col: self._normalize_column_name(col)
            for col in original_columns
        }
        
        # Transform all rows
        result = []
        for row in data:
            normalized_row = {
                normalized_mapping[col]: value
                for col, value in row.items()
            }
            result.append(normalized_row)
        
        return result
    
    def _normalize_column_name(self, column_name: str) -> str:
        """
        Normalize a single column name to snake_case.
        
        Args:
            column_name: Original column name
        
        Returns:
            str: Normalized column name in snake_case
        """
        # Start with original name
        normalized = column_name
        
        # Convert camelCase and PascalCase to snake_case
        # Insert underscore before uppercase letters that follow lowercase letters
        normalized = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', normalized)
        
        # Replace spaces with configured character
        normalized = normalized.replace(' ', self.replace_spaces)
        
        # Replace hyphens with underscores
        normalized = normalized.replace('-', '_')
        
        # Remove special characters if configured
        if self.remove_special_chars:
            # Keep only alphanumeric and underscores
            normalized = re.sub(r'[^a-zA-Z0-9_]', '', normalized)
        
        # Convert to lowercase if configured
        if self.lowercase:
            normalized = normalized.lower()
        
        # Remove duplicate underscores
        normalized = re.sub(r'_+', '_', normalized)
        
        # Remove leading/trailing underscores
        normalized = normalized.strip('_')
        
        # Apply max length if specified
        if self.max_length and len(normalized) > self.max_length:
            normalized = normalized[:self.max_length]
        
        # Ensure the column name is not empty
        if not normalized:
            normalized = 'column'
        
        return normalized


class FilterRowsTransform(BaseTransform):
    """
    Filter rows based on column value conditions.
    
    Configuration:
        column (str): Column name to filter on
        operator (str): Comparison operator ('eq', 'ne', 'gt', 'lt', 'gte', 'lte', 'contains', 'not_contains')
        value (Any): Value to compare against
        
    Example config:
        {
            "type": "filter_rows",
            "column": "status",
            "operator": "eq",
            "value": "active"
        }
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize filter rows transform."""
        super().__init__(config)
        self.validate_filter_config()
        
        self.column = config['column']
        self.operator = config['operator']
        self.value = config['value']
    
    def validate_filter_config(self) -> None:
        """Validate filter configuration."""
        required_fields = ['column', 'operator', 'value']
        missing_fields = [field for field in required_fields if field not in self.config]
        
        if missing_fields:
            raise ValueError(
                f"Missing required configuration fields: {', '.join(missing_fields)}"
            )
        
        valid_operators = ['eq', 'ne', 'gt', 'lt', 'gte', 'lte', 'contains', 'not_contains']
        if self.config['operator'] not in valid_operators:
            raise ValueError(
                f"Invalid operator '{self.config['operator']}'. "
                f"Must be one of: {', '.join(valid_operators)}"
            )
    
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter rows based on configured condition.
        
        Args:
            data: List of dictionaries to filter
        
        Returns:
            List[Dict[str, Any]]: Filtered data containing only rows that match condition
        """
        self.validate_data(data)
        
        if not data:
            return []
        
        result = []
        for row in data:
            if self._row_matches_condition(row):
                result.append(row)
        
        return result
    
    def _row_matches_condition(self, row: Dict[str, Any]) -> bool:
        """Check if a row matches the filter condition."""
        if self.column not in row:
            return False
        
        row_value = row[self.column]
        
        if self.operator == 'eq':
            return row_value == self.value
        elif self.operator == 'ne':
            return row_value != self.value
        elif self.operator == 'gt':
            return row_value > self.value
        elif self.operator == 'lt':
            return row_value < self.value
        elif self.operator == 'gte':
            return row_value >= self.value
        elif self.operator == 'lte':
            return row_value <= self.value
        elif self.operator == 'contains':
            return str(self.value) in str(row_value)
        elif self.operator == 'not_contains':
            return str(self.value) not in str(row_value)
        
        return False


class SelectColumnsTransform(BaseTransform):
    """
    Select or exclude specific columns from the data.
    
    Configuration:
        columns (List[str]): List of column names
        mode (str): 'include' to keep only these columns, 'exclude' to remove them (default: 'include')
        
    Example config:
        {
            "type": "select_columns",
            "columns": ["name", "email", "phone"],
            "mode": "include"
        }
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize select columns transform."""
        super().__init__(config)
        self.validate_select_config()
        
        self.columns = config['columns']
        self.mode = config.get('mode', 'include')
    
    def validate_select_config(self) -> None:
        """Validate select configuration."""
        if 'columns' not in self.config:
            raise ValueError("Missing required configuration field: columns")
        
        if not isinstance(self.config['columns'], list):
            raise ValueError("Configuration field 'columns' must be a list")
        
        mode = self.config.get('mode', 'include')
        if mode not in ['include', 'exclude']:
            raise ValueError(f"Invalid mode '{mode}'. Must be 'include' or 'exclude'")
    
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Select or exclude columns from data.
        
        Args:
            data: List of dictionaries
        
        Returns:
            List[Dict[str, Any]]: Data with selected columns only
        """
        self.validate_data(data)
        
        if not data:
            return []
        
        result = []
        for row in data:
            if self.mode == 'include':
                # Keep only specified columns
                new_row = {
                    col: row[col]
                    for col in self.columns
                    if col in row
                }
            else:  # exclude mode
                # Keep all columns except specified ones
                new_row = {
                    col: value
                    for col, value in row.items()
                    if col not in self.columns
                }
            
            result.append(new_row)
        
        return result
