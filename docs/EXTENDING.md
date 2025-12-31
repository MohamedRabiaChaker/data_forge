# Extending DataForge

Guide for adding custom sources, transforms, and destinations to the DataForge ETL framework.

## Overview

DataForge uses a factory pattern that makes it easy to add new components. The framework automatically discovers and registers your custom components.

## Adding a Custom Source

### 1. Create Source Class

Create a new file in `dags/tasks/sources/` (e.g., `my_source.py`):

```python
from dags.tasks.sources.base_source import BaseSource
from typing import List, Dict, Any

class MyCustomSource(BaseSource):
    """
    Extract data from My Custom API.
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        # Extract config parameters
        self.api_key = config.get('api_key')
        self.endpoint = config.get('endpoint')
        self.conn_id = config.get('my_conn_id', 'my_default')
    
    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract data from the source.
        
        Returns:
            List of dictionaries, where each dict is a row
        """
        from airflow.hooks.base import BaseHook
        
        # Get connection details
        conn = BaseHook.get_connection(self.conn_id)
        
        # Your extraction logic here
        data = []
        # ... fetch data from API ...
        # Example:
        # response = requests.get(
        #     f"{conn.host}/{self.endpoint}",
        #     headers={'Authorization': f'Bearer {conn.password}'}
        # )
        # data = response.json()
        
        self.log.info(f"Extracted {len(data)} records from {self.endpoint}")
        return data
```

### 2. Register Source

Add to `dags/factories/source_factory.py`:

```python
from dags.tasks.sources.my_source import MyCustomSource

# Add to the factory registration
SourceFactory.register("my_source", MyCustomSource)
```

### 3. Use in Configuration

```json
{
  "source": {
    "type": "my_source",
    "my_conn_id": "my_api_connection",
    "endpoint": "data/export",
    "api_key": "optional_if_in_connection"
  }
}
```

### 4. Create Airflow Connection

In Airflow UI (Admin > Connections):
- **Connection Id**: `my_api_connection`
- **Connection Type**: `HTTP`
- **Host**: `https://api.example.com`
- **Password**: `your_api_key`

## Adding a Custom Transform

### 1. Create Transform Class

Create a new file in `dags/tasks/transforms/` (e.g., `my_transform.py`):

```python
from dags.tasks.transforms.base_transform import BaseTransform
from typing import List, Dict, Any

class MyCustomTransform(BaseTransform):
    """
    Custom data transformation.
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        # Extract config parameters
        self.param1 = config.get('param1', 'default_value')
        self.param2 = config.get('param2', False)
    
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform the data.
        
        Args:
            data: Input data (list of dicts)
            
        Returns:
            Transformed data (list of dicts)
        """
        transformed = []
        
        for row in data:
            # Your transformation logic here
            new_row = row.copy()
            
            # Example: Add computed field
            if 'price' in row and 'quantity' in row:
                new_row['total'] = row['price'] * row['quantity']
            
            # Example: Format date
            if 'created_at' in row:
                # Convert to desired format
                pass
            
            transformed.append(new_row)
        
        self.log.info(f"Transformed {len(transformed)} records")
        return transformed
```

### 2. Register Transform

Add to `dags/factories/transform_factory.py`:

```python
from dags.tasks.transforms.my_transform import MyCustomTransform

# Add to the factory registration
TransformFactory.register("my_transform", MyCustomTransform)
```

### 3. Use in Configuration

```json
{
  "transforms": [
    {
      "type": "my_transform",
      "param1": "value1",
      "param2": true
    }
  ]
}
```

## Adding a Custom Destination

### 1. Create Destination Class

Create a new file in `dags/tasks/destinations/` (e.g., `my_destination.py`):

```python
from dags.tasks.destinations.base_destination import BaseDestination
from typing import List, Dict, Any

class MyCustomDestination(BaseDestination):
    """
    Load data to My Custom destination.
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        # Extract config parameters
        self.conn_id = config.get('my_conn_id', 'my_default')
        self.target = config.get('target')
        self.write_mode = config.get('write_mode', 'append')
    
    def pre_load_hook(self, **context):
        """
        Execute before loading data.
        Optional - override if needed.
        """
        self.log.info(f"Preparing to load to {self.target}")
        # Example: Create target if doesn't exist
        # Example: Truncate if write_mode == 'truncate'
    
    def load(self, data: List[Dict[str, Any]], **context) -> None:
        """
        Load data to the destination.
        
        Args:
            data: Data to load (list of dicts)
            context: Airflow context
        """
        from airflow.hooks.base import BaseHook
        
        if not data:
            self.log.warning("No data to load")
            return
        
        # Get connection
        conn = BaseHook.get_connection(self.conn_id)
        
        # Your loading logic here
        # Example:
        # client = MyAPIClient(conn.host, conn.password)
        # if self.write_mode == 'replace':
        #     client.delete_all(self.target)
        # client.bulk_insert(self.target, data)
        
        self.log.info(f"Loaded {len(data)} records to {self.target}")
    
    def post_load_hook(self, **context):
        """
        Execute after loading data.
        Optional - override if needed.
        """
        self.log.info(f"Load complete to {self.target}")
        # Example: Update metadata table
        # Example: Send notification
```

### 2. Register Destination

Add to `dags/factories/destination_factory.py`:

```python
from dags.tasks.destinations.my_destination import MyCustomDestination

# Add to the factory registration
DestinationFactory.register("my_destination", MyCustomDestination)
```

### 3. Use in Configuration

```json
{
  "destination": {
    "type": "my_destination",
    "my_conn_id": "my_dest_connection",
    "target": "my_table",
    "write_mode": "replace"
  }
}
```

## Advanced Examples

### Source with Pagination

```python
class PaginatedAPISource(BaseSource):
    def extract(self) -> List[Dict[str, Any]]:
        all_data = []
        page = 1
        
        while True:
            response = self._fetch_page(page)
            data = response.json()
            
            if not data:
                break
            
            all_data.extend(data)
            page += 1
            
            # Respect rate limits
            time.sleep(0.5)
        
        return all_data
    
    def _fetch_page(self, page: int):
        # Fetch single page
        pass
```

### Transform with Data Validation

```python
class ValidatingTransform(BaseTransform):
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        validated = []
        errors = 0
        
        for row in data:
            if self._validate_row(row):
                validated.append(row)
            else:
                errors += 1
                self.log.warning(f"Invalid row: {row}")
        
        self.log.info(f"Validated {len(validated)} rows, {errors} errors")
        return validated
    
    def _validate_row(self, row: Dict[str, Any]) -> bool:
        # Validation logic
        required_fields = ['id', 'email', 'created_at']
        return all(field in row for field in required_fields)
```

### Destination with Transaction Safety

```python
class TransactionalDestination(BaseDestination):
    def load(self, data: List[Dict[str, Any]], **context) -> None:
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        
        with hook.get_conn() as conn:
            conn.autocommit = False
            try:
                with conn.cursor() as cursor:
                    # Your insert logic
                    cursor.executemany(
                        f"INSERT INTO {self.table} VALUES (%s, %s, %s)",
                        [(r['id'], r['name'], r['value']) for r in data]
                    )
                conn.commit()
                self.log.info(f"Committed {len(data)} rows")
            except Exception as e:
                conn.rollback()
                self.log.error(f"Error loading data: {e}")
                raise
```

## Best Practices

### 1. Logging

Always log important events:

```python
self.log.info("Starting extraction")
self.log.warning(f"Missing field in row: {row}")
self.log.error(f"Failed to connect: {error}")
```

### 2. Error Handling

Handle errors gracefully:

```python
try:
    data = self._fetch_data()
except ConnectionError as e:
    self.log.error(f"Connection failed: {e}")
    raise
except ValueError as e:
    self.log.warning(f"Invalid data, skipping: {e}")
    data = []
```

### 3. Configuration Validation

Validate config in `__init__`:

```python
def __init__(self, config: Dict[str, Any]):
    super().__init__(config)
    
    # Validate required params
    if 'api_key' not in config:
        raise ValueError("api_key is required")
    
    # Validate param types
    if not isinstance(config.get('limit', 100), int):
        raise TypeError("limit must be an integer")
    
    self.api_key = config['api_key']
    self.limit = config.get('limit', 100)
```

### 4. Type Hints

Use type hints for clarity:

```python
from typing import List, Dict, Any, Optional

def extract(self) -> List[Dict[str, Any]]:
    pass

def transform(
    self, 
    data: List[Dict[str, Any]], 
    filters: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    pass
```

### 5. Docstrings

Document your components:

```python
class MySource(BaseSource):
    """
    Extract data from Custom API.
    
    Configuration:
        api_key (str): API authentication key
        endpoint (str): API endpoint to call
        limit (int): Max records per request (default: 100)
    
    Example:
        {
            "type": "my_source",
            "api_key": "abc123",
            "endpoint": "data/export",
            "limit": 250
        }
    """
```

## Testing Your Components

### Unit Test Example

Create `tests/test_my_source.py`:

```python
import unittest
from dags.tasks.sources.my_source import MyCustomSource

class TestMyCustomSource(unittest.TestCase):
    def test_extract(self):
        config = {
            'my_conn_id': 'test_conn',
            'endpoint': 'test/endpoint'
        }
        source = MyCustomSource(config)
        
        # Mock the API call
        # Test extraction logic
        
    def test_config_validation(self):
        with self.assertRaises(ValueError):
            MyCustomSource({})  # Missing required config
```

### Integration Test

Test with a real DAG:

```python
# test_my_pipeline.py
from datetime import datetime
from airflow.decorators import dag
from dags.dag_builder import ETLDagBuilder

@dag(
    dag_id='test_my_source',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
)
def test_my_source():
    config = {
        'source': {
            'type': 'my_source',
            'endpoint': 'test/data'
        },
        'destination': {
            'type': 'postgres',
            'table_name': 'test_output'
        }
    }
    
    builder = ETLDagBuilder('test_my_source', config)
    builder.build_etl_pipeline()

test_dag = test_my_source()
```

## Component Lifecycle

Understanding the execution flow:

```
1. Pipeline Config Loaded
   ↓
2. Factory.create() instantiates component
   ↓
3. Component.__init__() validates config
   ↓
4. Component method called:
   - Source: extract()
   - Transform: transform(data)
   - Destination: pre_load_hook() → load(data) → post_load_hook()
   ↓
5. Data passed to next component
```

## Need Help?

- Check existing components in `dags/tasks/` for examples
- Review base classes for required methods
- See factory files for registration pattern
- Test with small datasets first
