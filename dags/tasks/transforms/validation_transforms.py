"""Data validation transforms for quality assurance."""
from collections import Counter
from typing import Any, Dict, List, Set

try:
    from .base_transform import BaseTransform
except ImportError:
    from base_transform import BaseTransform


class ValidateDuplicateIdsTransform(BaseTransform):
    """
    Validate that ID columns have no duplicates in the source data.
    
    This transform is critical for SCD Type 2 historization and other scenarios
    where record uniqueness is required. It detects duplicate IDs before they
    reach the destination, preventing data quality issues.
    
    Configuration:
        id_columns (list): List of column names that should be unique (required)
        action (str): Action to take when duplicates are found (default: 'fail')
            - 'fail': Raise exception and stop pipeline
            - 'warn': Log warning but continue pipeline
            - 'deduplicate': Remove duplicates and continue
        keep (str): If action='deduplicate', which record to keep (default: 'first')
            - 'first': Keep first occurrence
            - 'last': Keep last occurrence
        log_duplicates (bool): Log details about duplicate records (default: True)
        
    Example config:
        {
            "type": "validate_duplicate_ids",
            "id_columns": ["eeid"],
            "action": "fail",
            "log_duplicates": True
        }
        
    Example config with deduplication:
        {
            "type": "validate_duplicate_ids",
            "id_columns": ["employee_id", "department_id"],
            "action": "deduplicate",
            "keep": "last",
            "log_duplicates": True
        }
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize duplicate ID validation transform."""
        super().__init__(config)
        self.validate_validation_config()
        
        self.id_columns = config['id_columns']
        self.action = config.get('action', 'fail').lower()
        self.keep = config.get('keep', 'first').lower()
        self.log_duplicates = config.get('log_duplicates', True)
    
    def validate_validation_config(self) -> None:
        """Validate the validation transform configuration."""
        if 'id_columns' not in self.config:
            raise ValueError("Missing required configuration field: id_columns")
        
        if not isinstance(self.config['id_columns'], list):
            raise ValueError("Configuration field 'id_columns' must be a list")
        
        if not self.config['id_columns']:
            raise ValueError("Configuration field 'id_columns' cannot be empty")
        
        action = self.config.get('action', 'fail').lower()
        if action not in ['fail', 'warn', 'deduplicate']:
            raise ValueError(
                f"Invalid action '{action}'. Must be 'fail', 'warn', or 'deduplicate'"
            )
        
        keep = self.config.get('keep', 'first').lower()
        if keep not in ['first', 'last']:
            raise ValueError(f"Invalid keep '{keep}'. Must be 'first' or 'last'")
    
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate data for duplicate IDs.
        
        Args:
            data: List of dictionaries to validate
        
        Returns:
            List[Dict[str, Any]]: Original data (or deduplicated if action='deduplicate')
        
        Raises:
            ValueError: If duplicates found and action='fail'
        """
        self.validate_data(data)
        
        if not data:
            return []
        
        # Check that all ID columns exist in data
        missing_columns = [col for col in self.id_columns if col not in data[0]]
        if missing_columns:
            raise ValueError(
                f"ID columns not found in data: {', '.join(missing_columns)}"
            )
        
        # Find duplicates
        duplicates_info = self._find_duplicates(data)
        
        if duplicates_info['has_duplicates']:
            duplicate_count = duplicates_info['duplicate_count']
            duplicate_ids = duplicates_info['duplicate_ids']
            
            # Log duplicate details if enabled
            if self.log_duplicates:
                self._log_duplicate_details(duplicate_ids, data)
            
            # Handle based on action
            if self.action == 'fail':
                id_cols_str = ', '.join(self.id_columns)
                raise ValueError(
                    f"Duplicate IDs found in source data!\n"
                    f"  ID columns: {id_cols_str}\n"
                    f"  Total records: {len(data)}\n"
                    f"  Duplicate IDs: {duplicate_count}\n"
                    f"  Duplicate records: {duplicates_info['duplicate_record_count']}\n"
                    f"  Example duplicates: {list(duplicate_ids)[:5]}\n\n"
                    f"Please clean the source data to remove duplicates before loading.\n"
                    f"Set 'action': 'deduplicate' in config to automatically remove duplicates."
                )
            
            elif self.action == 'warn':
                id_cols_str = ', '.join(self.id_columns)
                print(f"⚠️  WARNING: Duplicate IDs detected!")
                print(f"  ID columns: {id_cols_str}")
                print(f"  Total records: {len(data)}")
                print(f"  Duplicate IDs: {duplicate_count}")
                print(f"  Duplicate records: {duplicates_info['duplicate_record_count']}")
                print(f"  Pipeline will continue with duplicates present.")
                return data
            
            elif self.action == 'deduplicate':
                deduplicated_data = self._deduplicate(data, duplicate_ids)
                removed_count = len(data) - len(deduplicated_data)
                print(f"✓ Deduplication completed")
                print(f"  Original records: {len(data)}")
                print(f"  Removed duplicates: {removed_count}")
                print(f"  Final records: {len(deduplicated_data)}")
                return deduplicated_data
        
        # No duplicates found
        print(f"✓ Duplicate ID validation passed: {len(data)} unique records")
        return data
    
    def _find_duplicates(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Find duplicate IDs in the data.
        
        Args:
            data: List of dictionaries to check
        
        Returns:
            Dict containing duplicate information
        """
        # Build ID tuples for each record
        id_tuples = []
        for row in data:
            id_tuple = tuple(str(row.get(col, '')) for col in self.id_columns)
            id_tuples.append(id_tuple)
        
        # Count occurrences
        id_counts = Counter(id_tuples)
        
        # Find duplicates (IDs that appear more than once)
        duplicate_ids = {id_tuple for id_tuple, count in id_counts.items() if count > 1}
        
        # Count duplicate records
        duplicate_record_count = sum(
            count for id_tuple, count in id_counts.items() if count > 1
        )
        
        return {
            'has_duplicates': len(duplicate_ids) > 0,
            'duplicate_count': len(duplicate_ids),
            'duplicate_ids': duplicate_ids,
            'duplicate_record_count': duplicate_record_count,
            'id_counts': id_counts
        }
    
    def _log_duplicate_details(self, duplicate_ids: Set[tuple], data: List[Dict[str, Any]]) -> None:
        """
        Log detailed information about duplicate records.
        
        Args:
            duplicate_ids: Set of duplicate ID tuples
            data: Original data
        """
        print("\n" + "=" * 80)
        print("DUPLICATE ID DETAILS")
        print("=" * 80)
        
        # Log up to 10 duplicate IDs with their records
        for i, id_tuple in enumerate(list(duplicate_ids)[:10]):
            print(f"\nDuplicate #{i+1}: {dict(zip(self.id_columns, id_tuple))}")
            
            # Find all records with this ID
            matching_records = [
                row for row in data
                if tuple(str(row.get(col, '')) for col in self.id_columns) == id_tuple
            ]
            
            print(f"  Occurrences: {len(matching_records)}")
            for j, record in enumerate(matching_records):
                print(f"    Record {j+1}: {record}")
        
        if len(duplicate_ids) > 10:
            print(f"\n... and {len(duplicate_ids) - 10} more duplicate IDs")
        
        print("=" * 80 + "\n")
    
    def _deduplicate(self, data: List[Dict[str, Any]], duplicate_ids: Set[tuple]) -> List[Dict[str, Any]]:
        """
        Remove duplicate records from data.
        
        Args:
            data: Original data with duplicates
            duplicate_ids: Set of ID tuples that have duplicates
        
        Returns:
            Deduplicated data
        """
        seen_ids = set()
        result = []
        
        # Process data in order (first to last) or reverse (last to first)
        data_to_process = data if self.keep == 'first' else reversed(data)
        
        for row in data_to_process:
            id_tuple = tuple(str(row.get(col, '')) for col in self.id_columns)
            
            if id_tuple not in seen_ids:
                seen_ids.add(id_tuple)
                result.append(row)
        
        # If we processed in reverse, reverse back
        if self.keep == 'last':
            result.reverse()
        
        return result


class ValidateRequiredFieldsTransform(BaseTransform):
    """
    Validate that required fields are present and non-null.
    
    Configuration:
        required_fields (list): List of column names that must be present (required)
        allow_empty_strings (bool): Allow empty strings as valid values (default: False)
        action (str): Action to take when validation fails (default: 'fail')
            - 'fail': Raise exception and stop pipeline
            - 'warn': Log warning but continue pipeline
            - 'filter': Remove invalid records and continue
        
    Example config:
        {
            "type": "validate_required_fields",
            "required_fields": ["employee_id", "email", "department"],
            "allow_empty_strings": false,
            "action": "fail"
        }
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize required fields validation transform."""
        super().__init__(config)
        self.validate_required_config()
        
        self.required_fields = config['required_fields']
        self.allow_empty_strings = config.get('allow_empty_strings', False)
        self.action = config.get('action', 'fail').lower()
    
    def validate_required_config(self) -> None:
        """Validate the configuration."""
        if 'required_fields' not in self.config:
            raise ValueError("Missing required configuration field: required_fields")
        
        if not isinstance(self.config['required_fields'], list):
            raise ValueError("Configuration field 'required_fields' must be a list")
        
        if not self.config['required_fields']:
            raise ValueError("Configuration field 'required_fields' cannot be empty")
        
        action = self.config.get('action', 'fail').lower()
        if action not in ['fail', 'warn', 'filter']:
            raise ValueError(
                f"Invalid action '{action}'. Must be 'fail', 'warn', or 'filter'"
            )
    
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate that required fields are present.
        
        Args:
            data: List of dictionaries to validate
        
        Returns:
            List[Dict[str, Any]]: Original data (or filtered if action='filter')
        
        Raises:
            ValueError: If required fields are missing and action='fail'
        """
        self.validate_data(data)
        
        if not data:
            return []
        
        # Find invalid records
        invalid_records = []
        valid_records = []
        
        for i, row in enumerate(data):
            missing_fields = []
            for field in self.required_fields:
                if field not in row:
                    missing_fields.append(f"{field} (missing)")
                elif row[field] is None:
                    missing_fields.append(f"{field} (null)")
                elif not self.allow_empty_strings and isinstance(row[field], str) and row[field].strip() == '':
                    missing_fields.append(f"{field} (empty)")
            
            if missing_fields:
                invalid_records.append({
                    'index': i,
                    'missing_fields': missing_fields,
                    'record': row
                })
            else:
                valid_records.append(row)
        
        # Handle based on action
        if invalid_records:
            if self.action == 'fail':
                raise ValueError(
                    f"Required field validation failed!\n"
                    f"  Total records: {len(data)}\n"
                    f"  Invalid records: {len(invalid_records)}\n"
                    f"  First 5 invalid records:\n" +
                    '\n'.join([
                        f"    Record {r['index']}: Missing {', '.join(r['missing_fields'])}"
                        for r in invalid_records[:5]
                    ])
                )
            
            elif self.action == 'warn':
                print(f"⚠️  WARNING: {len(invalid_records)} records missing required fields")
                print(f"  Pipeline will continue with invalid records present.")
                return data
            
            elif self.action == 'filter':
                print(f"✓ Filtered {len(invalid_records)} records with missing required fields")
                print(f"  Valid records: {len(valid_records)}")
                return valid_records
        
        # All records valid
        print(f"✓ Required fields validation passed: {len(data)} records")
        return data


class ValidateDataTypesTransform(BaseTransform):
    """
    Validate and optionally coerce data types for specified columns.
    
    Configuration:
        column_types (dict): Dictionary mapping column names to expected types (required)
            Supported types: 'string', 'int', 'float', 'bool', 'date', 'datetime'
        coerce (bool): Attempt to coerce values to correct type (default: False)
        action (str): Action to take when validation fails (default: 'fail')
            - 'fail': Raise exception and stop pipeline
            - 'warn': Log warning but continue pipeline
            - 'filter': Remove invalid records and continue
        
    Example config:
        {
            "type": "validate_data_types",
            "column_types": {
                "employee_id": "string",
                "salary": "float",
                "age": "int",
                "is_active": "bool"
            },
            "coerce": true,
            "action": "fail"
        }
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize data type validation transform."""
        super().__init__(config)
        self.validate_type_config()
        
        self.column_types = config['column_types']
        self.coerce = config.get('coerce', False)
        self.action = config.get('action', 'fail').lower()
    
    def validate_type_config(self) -> None:
        """Validate the configuration."""
        if 'column_types' not in self.config:
            raise ValueError("Missing required configuration field: column_types")
        
        if not isinstance(self.config['column_types'], dict):
            raise ValueError("Configuration field 'column_types' must be a dictionary")
        
        supported_types = ['string', 'int', 'float', 'bool', 'date', 'datetime']
        for col, dtype in self.config['column_types'].items():
            if dtype not in supported_types:
                raise ValueError(
                    f"Unsupported type '{dtype}' for column '{col}'. "
                    f"Supported types: {', '.join(supported_types)}"
                )
        
        action = self.config.get('action', 'fail').lower()
        if action not in ['fail', 'warn', 'filter']:
            raise ValueError(
                f"Invalid action '{action}'. Must be 'fail', 'warn', or 'filter'"
            )
    
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate (and optionally coerce) data types.
        
        Args:
            data: List of dictionaries to validate
        
        Returns:
            List[Dict[str, Any]]: Original data (coerced if enabled, filtered if action='filter')
        
        Raises:
            ValueError: If type validation fails and action='fail'
        """
        self.validate_data(data)
        
        if not data:
            return []
        
        if self.coerce:
            # Coerce types
            coerced_data = []
            coercion_errors = []
            
            for i, row in enumerate(data):
                coerced_row = row.copy()
                row_errors = []
                
                for col, expected_type in self.column_types.items():
                    if col in coerced_row:
                        try:
                            coerced_row[col] = self._coerce_value(coerced_row[col], expected_type)
                        except (ValueError, TypeError) as e:
                            row_errors.append(f"{col}: {str(e)}")
                
                if row_errors:
                    coercion_errors.append({'index': i, 'errors': row_errors, 'record': row})
                else:
                    coerced_data.append(coerced_row)
            
            if coercion_errors:
                if self.action == 'fail':
                    raise ValueError(
                        f"Type coercion failed!\n"
                        f"  Total records: {len(data)}\n"
                        f"  Failed records: {len(coercion_errors)}\n"
                        f"  First 5 failures:\n" +
                        '\n'.join([
                            f"    Record {e['index']}: {', '.join(e['errors'])}"
                            for e in coercion_errors[:5]
                        ])
                    )
                elif self.action == 'warn':
                    print(f"⚠️  WARNING: {len(coercion_errors)} records failed type coercion")
                    return data
                elif self.action == 'filter':
                    print(f"✓ Filtered {len(coercion_errors)} records with type coercion errors")
                    return coerced_data
            
            print(f"✓ Type validation and coercion passed: {len(coerced_data)} records")
            return coerced_data
        else:
            # Just validate without coercion
            invalid_records = []
            
            for i, row in enumerate(data):
                row_errors = []
                
                for col, expected_type in self.column_types.items():
                    if col in row and not self._check_type(row[col], expected_type):
                        row_errors.append(f"{col}: expected {expected_type}, got {type(row[col]).__name__}")
                
                if row_errors:
                    invalid_records.append({'index': i, 'errors': row_errors, 'record': row})
            
            if invalid_records:
                if self.action == 'fail':
                    raise ValueError(
                        f"Type validation failed!\n"
                        f"  Total records: {len(data)}\n"
                        f"  Invalid records: {len(invalid_records)}\n"
                        f"  First 5 invalid records:\n" +
                        '\n'.join([
                            f"    Record {e['index']}: {', '.join(e['errors'])}"
                            for e in invalid_records[:5]
                        ])
                    )
                elif self.action == 'warn':
                    print(f"⚠️  WARNING: {len(invalid_records)} records have type mismatches")
                    return data
                elif self.action == 'filter':
                    valid_data = [row for i, row in enumerate(data) if not any(inv['index'] == i for inv in invalid_records)]
                    print(f"✓ Filtered {len(invalid_records)} records with type errors")
                    return valid_data
            
            print(f"✓ Type validation passed: {len(data)} records")
            return data
    
    def _check_type(self, value: Any, expected_type: str) -> bool:
        """Check if value matches expected type."""
        if value is None:
            return True  # Allow None for any type
        
        if expected_type == 'string':
            return isinstance(value, str)
        elif expected_type == 'int':
            return isinstance(value, int) and not isinstance(value, bool)
        elif expected_type == 'float':
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        elif expected_type == 'bool':
            return isinstance(value, bool)
        # date and datetime would require datetime module checks
        return True
    
    def _coerce_value(self, value: Any, target_type: str) -> Any:
        """Coerce value to target type."""
        if value is None:
            return None
        
        if target_type == 'string':
            return str(value)
        elif target_type == 'int':
            return int(value)
        elif target_type == 'float':
            return float(value)
        elif target_type == 'bool':
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'y')
            return bool(value)
        
        return value
