# Architecture Deep Dive

Detailed explanation of DataForge's internal architecture and design patterns.

## System Overview

DataForge implements a layered architecture that separates concerns and enables extensibility:

```
┌─────────────────────────────────────────────────────────┐
│         Configuration Layer (JSON Files)                │
│         dags/configs/*.json                             │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│         Orchestration Layer                             │
│         - pipeline_dag_generator.py                     │
│         - dag_builder.py                                │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│         Factory Layer                                   │
│         - source_factory.py                             │
│         - transform_factory.py                          │
│         - destination_factory.py                        │
└─────────────────────────────────────────────────────────┘
                        ↓
┌───────────────┬─────────────────┬───────────────────────┐
│  Hook Layer   │ Component Layer │   Task Execution      │
│  - shopify    │  - sources      │   - extract()         │
│  - (future)   │  - transforms   │   - transform()       │
│               │  - destinations │   - load()            │
└───────────────┴─────────────────┴───────────────────────┘
```

## Layer Breakdown

### 1. Configuration Layer

**Location**: `dags/configs/*.json`

**Purpose**: Declarative pipeline definitions that non-developers can create and modify.

**Structure**:
```json
{
  "dag_id": "unique_pipeline_id",
  "description": "What this pipeline does",
  "schedule_interval": "@daily",
  "start_date": "2025-01-01",
  "catchup": false,
  "tags": ["category"],
  "default_args": { /* Airflow task defaults */ },
  "source": { /* Extract config */ },
  "transforms": [ /* Transform configs */ ],
  "destination": { /* Load config */ }
}
```

**Design Pattern**: Configuration as Code
- Separates pipeline logic from implementation
- Enables version control of pipeline definitions
- Allows non-technical users to create pipelines

### 2. Orchestration Layer

#### pipeline_dag_generator.py

**Purpose**: Auto-discovery and DAG generation

**How it works**:
1. Scans `dags/configs/` directory on Airflow startup
2. Reads all `.json` files
3. Parses each file to extract configuration
4. Creates Airflow DAG object using `dag_builder.py`
5. Registers DAG in global namespace for Airflow discovery

**Key Code**:
```python
def generate_dags_from_configs():
    config_dir = Path(__file__).parent / "configs"
    
    for config_file in config_dir.glob("*.json"):
        with open(config_file) as f:
            config = json.load(f)
        
        dag_id = config['dag_id']
        builder = ETLDagBuilder(dag_id, config)
        dag = builder.build_etl_pipeline()
        
        globals()[dag_id] = dag  # Register DAG
```

**Design Pattern**: Factory + Auto-Discovery
- No manual DAG registration required
- New pipelines appear automatically
- Supports dynamic pipeline generation

#### dag_builder.py

**Purpose**: ETL pipeline orchestration and task wiring

**Core Components**:

1. **ETLDagBuilder Class**: Main orchestrator
2. **Three TaskFlow Tasks**:
   - `extract_data()` - Calls source.extract()
   - `transform_data()` - Applies transform chain
   - `load_data()` - Calls destination.load()

**Task Dependencies**:
```python
extract_data() >> transform_data() >> load_data()
```

**Data Flow** (via XCom):
```python
@task
def extract_data():
    source = SourceFactory.create(source_config)
    data = source.extract()
    return data  # Stored in XCom

@task
def transform_data(data):  # Receives from XCom
    for transform_config in transforms:
        transform = TransformFactory.create(transform_config)
        data = transform.transform(data)
    return data

@task
def load_data(data):  # Receives from XCom
    destination = DestinationFactory.create(dest_config)
    destination.load(data)
```

**Design Pattern**: Pipeline Pattern + Dependency Injection
- Clear separation of extract/transform/load
- Uses Airflow TaskFlow API for data passing
- Factories inject concrete implementations

### 3. Factory Layer

**Location**: `dags/factories/`

**Purpose**: Dynamic component instantiation based on configuration

**Pattern**: Registry Pattern

**Implementation**:
```python
class SourceFactory:
    _sources = {}
    
    @classmethod
    def register(cls, source_type: str, source_class):
        cls._sources[source_type] = source_class
    
    @classmethod
    def create(cls, config: Dict) -> BaseSource:
        source_type = config['type']
        source_class = cls._sources.get(source_type)
        if not source_class:
            raise ValueError(f"Unknown source: {source_type}")
        return source_class(config)

# Registration
SourceFactory.register("gsheet", GoogleSheetsSource)
SourceFactory.register("shopify", ShopifySource)
```

**Benefits**:
- Decouples configuration from implementation
- Easy to add new components
- Centralized component registry
- Type-safe instantiation

### 4. Hook Layer

**Location**: `dags/hooks/`

**Purpose**: Airflow-native authentication and API interaction

**Example - ShopifyHook**:

```python
class ShopifyHook(HttpHook):
    def __init__(self, shopify_conn_id: str = 'shopify_default'):
        super().__init__(http_conn_id=shopify_conn_id)
        self.shopify_conn_id = shopify_conn_id
    
    def paginate(self, endpoint: str, params: Dict, limit: int):
        """Automatic pagination with rate limiting"""
        while True:
            response = self.run(endpoint, data=params)
            data = response.json()
            
            yield from data
            
            # Check for next page
            if 'Link' not in response.headers:
                break
            
            # Rate limiting
            time.sleep(0.5)
```

**Design Pattern**: Adapter Pattern
- Adapts external APIs to Airflow's connection system
- Provides consistent interface across different APIs
- Handles authentication, pagination, rate limiting

### 5. Component Layer

**Location**: `dags/tasks/`

**Purpose**: Concrete ETL implementations

#### Base Classes

**BaseSource**:
```python
from abc import ABC, abstractmethod

class BaseSource(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.log = logging.getLogger(__name__)
    
    @abstractmethod
    def extract(self) -> List[Dict[str, Any]]:
        """Extract data and return as list of dicts"""
        pass
```

**BaseTransform**:
```python
class BaseTransform(ABC):
    @abstractmethod
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform data and return modified list"""
        pass
```

**BaseDestination**:
```python
class BaseDestination(ABC):
    def pre_load_hook(self, **context):
        """Optional: Execute before load"""
        pass
    
    @abstractmethod
    def load(self, data: List[Dict[str, Any]], **context) -> None:
        """Load data to destination"""
        pass
    
    def post_load_hook(self, **context):
        """Optional: Execute after load"""
        pass
```

**Design Pattern**: Template Method Pattern
- Base classes define the interface
- Subclasses implement specific behavior
- Consistent API across all components

## Data Flow

### Complete Pipeline Execution

```
1. Airflow Scheduler reads pipeline_dag_generator.py
   ↓
2. Generator scans dags/configs/*.json
   ↓
3. For each config:
   - Create ETLDagBuilder
   - Register DAG in globals()
   ↓
4. DAG appears in Airflow UI
   ↓
5. User triggers DAG
   ↓
6. extract_data task runs:
   - SourceFactory.create(config['source'])
   - source.extract()
   - Return data → XCom
   ↓
7. transform_data task runs:
   - Receive data from XCom
   - For each transform config:
     * TransformFactory.create(transform_config)
     * data = transform.transform(data)
   - Return data → XCom
   ↓
8. load_data task runs:
   - Receive data from XCom
   - DestinationFactory.create(config['destination'])
   - destination.pre_load_hook()
   - destination.load(data)
   - destination.post_load_hook()
   ↓
9. Pipeline complete
```

### XCom Data Passing

Airflow's XCom (cross-communication) is used to pass data between tasks:

```python
# Task 1: Returns data
@task
def extract():
    return [{"id": 1}, {"id": 2}]

# Task 2: Receives data as parameter
@task
def transform(data):
    return [{"id": d["id"], "processed": True} for d in data]

# Task 3: Receives data
@task
def load(data):
    print(f"Loading {len(data)} records")

# Wiring
extract() >> transform() >> load()
```

**Note**: XCom has size limits (~48KB in PostgreSQL). For large datasets, consider using external storage (S3, GCS) and passing references.

## Design Patterns Used

### 1. Factory Pattern
- **Where**: `dags/factories/`
- **Why**: Dynamic component creation based on configuration
- **Benefit**: Easy extensibility without modifying core code

### 2. Template Method Pattern
- **Where**: Base classes (`BaseSource`, `BaseTransform`, `BaseDestination`)
- **Why**: Define skeleton algorithm, let subclasses implement steps
- **Benefit**: Consistent interface, flexible implementation

### 3. Registry Pattern
- **Where**: Factory registrations
- **Why**: Centralized component registration
- **Benefit**: Loose coupling, runtime component discovery

### 4. Adapter Pattern
- **Where**: Hooks (e.g., `ShopifyHook`)
- **Why**: Adapt external APIs to Airflow's connection system
- **Benefit**: Consistent authentication, easy testing

### 5. Pipeline Pattern
- **Where**: Task flow (extract → transform → load)
- **Why**: Sequential data processing stages
- **Benefit**: Clear separation of concerns, easy debugging

### 6. Strategy Pattern
- **Where**: Write modes in PostgresDestination
- **Why**: Different loading strategies based on configuration
- **Benefit**: Flexible behavior without conditionals

## Smart Loading Strategy (PostgreSQL)

### Small Dataset Strategy (<1000 rows)

```python
def _load_small_dataset(self, data):
    cursor.executemany(
        f"INSERT INTO {schema}.{table} VALUES (...)",
        rows
    )
```

**Benefits**:
- Simple, direct approach
- Low overhead
- Fast for small datasets

### Large Dataset Strategy (≥1000 rows)

```python
def _load_large_dataset(self, data):
    # 1. Create temp table
    cursor.execute(f"CREATE TEMP TABLE tmp_{table} (...)")
    
    # 2. Bulk insert into temp
    cursor.executemany(f"INSERT INTO tmp_{table} VALUES (...)", rows)
    
    # 3. Atomic swap
    cursor.execute("BEGIN")
    cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
    cursor.execute(f"ALTER TABLE tmp_{table} SET SCHEMA {schema}")
    cursor.execute(f"ALTER TABLE {schema}.tmp_{table} RENAME TO {table}")
    cursor.execute("COMMIT")
```

**Benefits**:
- **Atomicity**: All-or-nothing replacement
- **No downtime**: Old table exists until swap
- **Performance**: Bulk inserts much faster
- **Safety**: Transaction rollback on failure

### Why Two Strategies?

- **Small datasets**: Overhead of temp table not worth it
- **Large datasets**: Temp table strategy essential for performance
- **Configurable threshold**: Adjust based on your use case

## Schema Inference Algorithm

```python
def infer_schema(data: List[Dict[str, Any]]) -> Dict[str, str]:
    schema = {}
    
    for row in data:
        for column, value in row.items():
            if column not in schema:
                # Infer type from first non-null value
                if isinstance(value, bool):
                    schema[column] = "BOOLEAN"
                elif isinstance(value, int):
                    schema[column] = "INTEGER"
                elif isinstance(value, float):
                    schema[column] = "DOUBLE PRECISION"
                else:
                    schema[column] = "TEXT"
    
    return schema
```

**Limitations**:
- Type from first non-null value
- No mixed types (int/float → pick first)
- No date/datetime inference (treated as TEXT)
- No NULL handling (nullable by default)

**Future Enhancements**:
- Sample multiple rows for type consensus
- Detect date/datetime patterns
- Handle mixed types intelligently
- Support custom type mappings

## Performance Considerations

### 1. XCom Size Limits

**Problem**: XCom stores data in Airflow's metadata DB
**Limit**: ~48KB with PostgreSQL backend
**Solution**: For large datasets, use external storage:

```python
@task
def extract():
    data = source.extract()
    # Store in S3/GCS
    s3_key = upload_to_s3(data)
    return {"s3_key": s3_key}  # Only pass reference

@task
def transform(ref):
    data = download_from_s3(ref["s3_key"])
    # Transform...
```

### 2. Memory Usage

**Issue**: Loading entire dataset into memory
**Mitigation**:
- Use generators where possible
- Process in batches
- Stream data for very large datasets

### 3. Database Connections

**Issue**: Connection pool exhaustion
**Mitigation**:
- Use connection context managers
- Close connections explicitly
- Configure pool size in Airflow

## Error Handling Strategy

### 1. Validation Errors (Early Failure)

```python
def __init__(self, config):
    if 'required_param' not in config:
        raise ValueError("required_param is missing")
```

**Why**: Fail fast before execution starts

### 2. Extraction Errors (Retry)

```python
@task(retries=3, retry_delay=timedelta(minutes=5))
def extract_data():
    # Airflow handles retries automatically
```

**Why**: Transient API failures should be retried

### 3. Transform Errors (Skip/Warn)

```python
def transform(self, data):
    results = []
    for row in data:
        try:
            results.append(self._transform_row(row))
        except ValueError as e:
            self.log.warning(f"Skipping invalid row: {e}")
    return results
```

**Why**: Don't fail entire pipeline for bad records

### 4. Load Errors (Rollback)

```python
try:
    cursor.execute("BEGIN")
    # ... load data ...
    cursor.execute("COMMIT")
except Exception:
    cursor.execute("ROLLBACK")
    raise
```

**Why**: Ensure data consistency

## Security Considerations

### 1. Connection Credentials

**Storage**: Airflow Connections (encrypted in metadata DB)
**Access**: Only via Airflow's connection system
**Best Practice**: Use secrets backend (AWS Secrets Manager, Vault)

### 2. SQL Injection Prevention

```python
# ❌ BAD - SQL injection risk
cursor.execute(f"SELECT * FROM {table_name}")

# ✅ GOOD - Parameterized query
cursor.execute("SELECT * FROM %s", (table_name,))
```

### 3. API Key Handling

```python
# ❌ BAD - Keys in config
{"api_key": "secret123"}

# ✅ GOOD - Keys in connection
conn = BaseHook.get_connection(conn_id)
api_key = conn.password
```

## Future Architecture Enhancements

1. **Streaming Support**: Process data in chunks, not all-at-once
2. **Parallel Transforms**: Run independent transforms concurrently
3. **Data Validation Layer**: Built-in schema validation
4. **Metadata Tracking**: Track pipeline runs, data lineage
5. **Plugin System**: Load external components dynamically
6. **Config Validation**: JSON schema validation for configs
7. **Testing Framework**: Built-in testing utilities

## Conclusion

DataForge's architecture prioritizes:
- **Modularity**: Clear separation of concerns
- **Extensibility**: Easy to add new components
- **Configuration over Code**: Non-developers can create pipelines
- **Production-Ready**: Transaction safety, error handling, retries
- **Airflow-Native**: Leverages Airflow's features fully
