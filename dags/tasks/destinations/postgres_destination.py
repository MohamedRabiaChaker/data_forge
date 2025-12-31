"""PostgreSQL destination implementation with temp table support and historization."""

import hashlib
import logging
import time
from datetime import datetime
from typing import Any, Dict, List

from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from .base_destination import BaseDestination
except ImportError:
    from base_destination import BaseDestination


class PostgresDestination(BaseDestination):
    """
    Load data into PostgreSQL database using Airflow's Postgres provider.

    Features:
    - Automatic temp table strategy for datasets > 1000 rows
    - Schema auto-creation from data
    - Support for truncate/append/replace/historize modes
    - SCD Type 2 historization with hash-based change detection
    - Transaction management with rollback on failure

    Configuration:
        postgres_conn_id (str): Airflow connection ID for PostgreSQL (default: 'postgres_default')
        schema (str): Database schema name (default: 'public')
        table_name (str): Target table name
        write_mode (str): 'append', 'truncate', 'replace', or 'historize' (default: 'historize')
        create_table (bool): Auto-create table if it doesn't exist (default: True)
        temp_table_threshold (int): Row count threshold for using temp tables (default: 1000)
        historization (dict): Historization configuration (required if write_mode='historize')
            enabled (bool): Enable historization (default: True)
            id_columns (list): List of columns that uniquely identify a record (required)
            hash_algorithm (str): 'md5' or 'sha256' (default: 'md5')
            track_deletes (bool): Mark deleted records with _deleted_at (default: True)

    Example config:
        {
            "type": "postgres",
            "postgres_conn_id": "postgres_default",
            "schema": "public",
            "table_name": "employees",
            "write_mode": "historize",
            "create_table": True,
            "historization": {
                "enabled": true,
                "id_columns": ["eeid"],
                "hash_algorithm": "md5",
                "track_deletes": true
            }
        }
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize PostgreSQL destination with configuration."""
        super().__init__(config)
        self.validate_config()

        # Initialize logger
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.setLevel(logging.INFO)

        # Get configuration with defaults
        self.postgres_conn_id = config.get("postgres_conn_id", "postgres_default")
        self.schema = config.get("schema", "public")
        self.table_name = config["table_name"]
        self.write_mode = config.get("write_mode", "historize")
        self.create_table = config.get("create_table", True)
        self.temp_table_threshold = config.get("temp_table_threshold", 1000)

        # Historization configuration
        self.historization_config = config.get("historization", {})
        self.historization_enabled = self.historization_config.get("enabled", True) if self.write_mode == "historize" else False
        self.id_columns = self.historization_config.get("id_columns", [])
        self.hash_algorithm = self.historization_config.get("hash_algorithm", "md5")
        self.track_deletes = self.historization_config.get("track_deletes", True)

        # Internal state
        self.temp_table_name = None
        self.staging_table_name = None
        self.use_temp_table = False

    def validate_config(self) -> None:
        """
        Validate required configuration parameters.

        Raises:
            ValueError: If required parameters are missing or invalid
        """
        if not self.config.get("table_name"):
            raise ValueError("Missing required configuration field: table_name")

        write_mode = self.config.get("write_mode", "historize")
        if write_mode not in ["append", "truncate", "replace", "historize"]:
            raise ValueError(
                f"Invalid write_mode '{write_mode}'. Must be 'append', 'truncate', 'replace', or 'historize'"
            )

        # Validate historization config if write_mode is historize
        if write_mode == "historize":
            historization = self.config.get("historization", {})
            if not historization.get("id_columns"):
                raise ValueError(
                    "historization.id_columns is required when write_mode='historize'"
                )
            hash_algo = historization.get("hash_algorithm", "md5")
            if hash_algo not in ["md5", "sha256"]:
                raise ValueError(
                    f"Invalid hash_algorithm '{hash_algo}'. Must be 'md5' or 'sha256'"
                )

    def _calculate_record_hash(self, record: Dict[str, Any]) -> str:
        """
        Calculate hash of entire record for change detection.

        Args:
            record: Dictionary containing record data (excluding system columns)

        Returns:
            Hexadecimal hash string
        """
        # Sort keys to ensure consistent hash
        sorted_items = sorted(record.items())
        record_str = "|".join(f"{k}:{v}" for k, v in sorted_items)

        if self.hash_algorithm == "sha256":
            return hashlib.sha256(record_str.encode()).hexdigest()
        else:
            return hashlib.md5(record_str.encode()).hexdigest()

    def _calculate_id_hash(self, record: Dict[str, Any]) -> str:
        """
        Calculate hash of primary key columns for fast lookups.

        Args:
            record: Dictionary containing record data

        Returns:
            Hexadecimal hash string
        """
        # Extract ID column values
        id_values = [str(record.get(col, "")) for col in self.id_columns]
        id_str = "|".join(id_values)

        if self.hash_algorithm == "sha256":
            return hashlib.sha256(id_str.encode()).hexdigest()
        else:
            return hashlib.md5(id_str.encode()).hexdigest()

    def _enrich_with_system_columns(
        self, data: List[Dict[str, Any]], context: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Add system columns to each record for historization.

        Args:
            data: List of records to enrich
            context: Airflow context (optional, for pipeline metadata)

        Returns:
            Enriched data with system columns
        """
        enriched_data = []
        current_timestamp = datetime.now()
        
        # Extract pipeline metadata from context if available
        pipeline_run_id = None
        if context:
            dag_run = context.get("dag_run")
            if dag_run:
                pipeline_run_id = f"{dag_run.dag_id}_{dag_run.run_id}"

        for record in data:
            # Calculate hashes
            record_hash = self._calculate_record_hash(record)
            id_hash = self._calculate_id_hash(record)

            # Create enriched record with system columns
            enriched_record = {
                **record,
                "_record_hash": record_hash,
                "_id_hash": id_hash,
                "_valid_from": current_timestamp,
                "_valid_to": None,
                "_deleted_at": None,
                "_loaded_at": current_timestamp,
                "_pipeline_run_id": pipeline_run_id,
                "_source_system": self.config.get("type", "unknown"),
                "_load_batch_id": f"{self.table_name}_{int(time.time())}",
            }
            enriched_data.append(enriched_record)

        return enriched_data

    def pre_load_hook(self, data: List[Dict[str, Any]]) -> None:
        """
        Execute pre-load operations.

        - Determines if temp table strategy should be used
        - Creates temp/staging table if needed
        - Creates target table if it doesn't exist
        - Truncates target table if write_mode is 'truncate'

        Args:
            data: The data that will be loaded (used for schema inference)
        """
        if not data:
            return

        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # For historization mode, always use staging table
        if self.write_mode == "historize":
            self.staging_table_name = f"{self.table_name}_staging_{int(time.time())}"
            
            # Create target table with system columns if needed
            if self.create_table:
                self._create_table_from_data(
                    hook=hook, 
                    table_name=self.table_name, 
                    data=data, 
                    is_temp=False,
                    include_system_columns=True
                )
            
            # DON'T create staging table here - it will be created in load() method
            # using the same connection to ensure TEMPORARY table visibility
        else:
            # Original logic for non-historized loads
            # Determine if we should use temp table strategy
            self.use_temp_table = len(data) > self.temp_table_threshold

            if self.use_temp_table:
                # Generate temp table name
                self.temp_table_name = f"{self.table_name}_temp_{int(time.time())}"

                # Create temp table with same schema
                if self.create_table:
                    self._create_table_from_data(
                        hook=hook, table_name=self.temp_table_name, data=data, is_temp=True
                    )
            else:
                # Create target table if needed
                if self.create_table:
                    self._create_table_from_data(
                        hook=hook, table_name=self.table_name, data=data, is_temp=False
                    )

                # Handle truncate mode for direct load
                if self.write_mode == "truncate":
                    hook.run(f"TRUNCATE TABLE {self.schema}.{self.table_name}")

    def load(self, data: List[Dict[str, Any]], context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Load data into PostgreSQL database.

        Args:
            data: List of dictionaries representing rows to load
            context: Airflow context (optional, for pipeline metadata)

        Returns:
            Dict with load metadata including rows_loaded, status, and duration_seconds

        Raises:
            Exception: If load operation fails
        """
        start_time = time.time()

        if not data:
            return {
                "rows_loaded": 0,
                "status": "success",
                "duration_seconds": 0,
                "message": "No data to load",
            }

        try:
            hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

            # Handle historization mode
            if self.write_mode == "historize":
                self.log.info("=" * 80)
                self.log.info("HISTORIZATION MODE: Starting SCD Type 2 load")
                self.log.info("=" * 80)
                
                # Enrich data with system columns
                enriched_data = self._enrich_with_system_columns(data, context)
                self.log.info(f"Enriched {len(enriched_data)} rows with system columns")
                
                # Get a single connection to use for all operations
                # This is CRITICAL for TEMPORARY table visibility
                conn = hook.get_conn()
                cursor = conn.cursor()
                
                try:
                    # Create staging TEMPORARY table using the same connection
                    self.log.info(f"Creating staging table {self.staging_table_name}")
                    self._create_staging_table_with_conn(cursor, enriched_data)
                    self.log.info(f"Staging table {self.staging_table_name} created successfully")
                    # Insert into staging table (temp table, no schema prefix)
                    columns = list(enriched_data[0].keys())
                    rows = [[row.get(col) for col in columns] for row in enriched_data]
                    
                    self.log.info(f"Inserting {len(rows)} rows into staging table {self.staging_table_name}")
                    
                    # Build and execute INSERT using the same cursor
                    col_names = ", ".join(columns)
                    placeholders = ", ".join(["%s"] * len(columns))
                    insert_sql = f"INSERT INTO {self.staging_table_name} ({col_names}) VALUES ({placeholders})"
                    
                    cursor.executemany(insert_sql, rows)
                    conn.commit()
                    self.log.info(f"Successfully inserted {len(rows)} rows into staging table")
                    
                    # Merge staging data into target using SCD Type 2 logic
                    # Pass the same connection so temp table is visible
                    merge_stats = self._merge_historized_data(
                        hook, self.staging_table_name, self.table_name, conn
                    )
                    
                    # Clean up staging table (temp table, no schema prefix)
                    self.log.info(f"Dropping staging table {self.staging_table_name}")
                    cursor.execute(f"DROP TABLE IF EXISTS {self.staging_table_name}")
                    conn.commit()
                    
                    cursor.close()
                    conn.close()
                    
                    duration = time.time() - start_time
                    
                    self.log.info("=" * 80)
                    self.log.info("HISTORIZATION MODE: Completed successfully")
                    self.log.info(f"Stats: {merge_stats}")
                    self.log.info("=" * 80)
                    
                    return {
                        "rows_loaded": len(data),
                        "status": "success",
                        "duration_seconds": round(duration, 2),
                        "table": f"{self.schema}.{self.table_name}",
                        "historization_stats": merge_stats,
                        "write_mode": "historize"
                    }
                except Exception as e:
                    self.log.error(f"Historization failed: {str(e)}")
                    try:
                        conn.rollback()
                        cursor.close()
                        conn.close()
                    except:
                        pass
                    raise
            
            # Original logic for non-historized loads
            # Determine target table for insert
            target_table = (
                self.temp_table_name if self.use_temp_table else self.table_name
            )

            # Build INSERT statement
            columns = list(data[0].keys())
            column_list = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))

            insert_sql = f"""
                INSERT INTO {self.schema}.{target_table} ({column_list})
                VALUES ({placeholders})
            """

            # Prepare rows for bulk insert
            rows = [[row.get(col) for col in columns] for row in data]

            # Execute bulk insert
            hook.insert_rows(
                table=f"{self.schema}.{target_table}",
                rows=rows,
                target_fields=columns,
                commit_every=1000,
            )

            # If using temp table, swap it with the target table
            if self.use_temp_table:
                self._swap_tables(hook)

            duration = time.time() - start_time

            return {
                "rows_loaded": len(data),
                "status": "success",
                "duration_seconds": round(duration, 2),
                "table": f"{self.schema}.{self.table_name}",
                "used_temp_table": self.use_temp_table,
            }

        except Exception as e:
            duration = time.time() - start_time
            return {
                "rows_loaded": 0,
                "status": "failed",
                "duration_seconds": round(duration, 2),
                "error": str(e),
            }

    def post_load_hook(self, result: Dict[str, Any]) -> None:
        """
        Execute post-load cleanup operations.

        - Drops temp/staging tables if they were used
        - Logs load statistics

        Args:
            result: The result dictionary from load() method
        """
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        # Clean up staging table for historization (temp table, no schema prefix)
        if self.staging_table_name:
            try:
                hook.run(f"DROP TABLE IF EXISTS {self.staging_table_name}")
            except Exception as e:
                print(f"Warning: Could not drop staging table: {e}")
        
        # Clean up temp table for regular loads
        if self.use_temp_table and self.temp_table_name:
            try:
                # Temp table should already be dropped by _swap_tables, but clean up if needed
                hook.run(f"DROP TABLE IF EXISTS {self.schema}.{self.temp_table_name}")
            except Exception as e:
                print(f"Warning: Could not drop temp table: {e}")

    def _create_table_from_data(
        self,
        hook: PostgresHook,
        table_name: str,
        data: List[Dict[str, Any]],
        is_temp: bool = False,
        include_system_columns: bool = False,
    ) -> None:
        """
        Create table with schema inferred from data.

        Args:
            hook: PostgresHook instance
            table_name: Name of table to create
            data: Sample data for schema inference
            is_temp: Whether this is a temporary table
            include_system_columns: Whether to include historization system columns
        """
        if not data:
            return

        # Infer column types from first row
        sample_row = data[0]
        columns_def = []

        for col_name, value in sample_row.items():
            # Skip system columns in inference (they'll be added separately)
            if col_name.startswith("_"):
                continue

            # Simple type inference
            if isinstance(value, bool):
                col_type = "BOOLEAN"
            elif isinstance(value, int):
                col_type = "BIGINT"
            elif isinstance(value, float):
                col_type = "DOUBLE PRECISION"
            else:
                col_type = "TEXT"

            columns_def.append(f"{col_name} {col_type}")

        # Add system columns for historization if enabled
        if include_system_columns and self.historization_enabled:
            system_columns = [
                "_record_hash TEXT NOT NULL",
                "_id_hash TEXT NOT NULL",
                "_valid_from TIMESTAMP NOT NULL",
                "_valid_to TIMESTAMP DEFAULT NULL",
                "_deleted_at TIMESTAMP DEFAULT NULL",
                "_loaded_at TIMESTAMP DEFAULT NOW()",
                "_pipeline_run_id TEXT",
                "_source_system TEXT",
                "_load_batch_id TEXT",
            ]
            columns_def.extend(system_columns)

        columns_sql = ", ".join(columns_def)
        temp_keyword = "TEMPORARY" if is_temp else ""
        
        # PostgreSQL doesn't allow schema prefix for TEMPORARY tables
        table_ref = table_name if is_temp else f"{self.schema}.{table_name}"

        create_sql = f"""
            CREATE {temp_keyword} TABLE IF NOT EXISTS {table_ref} (
                {columns_sql}
            )
        """

        hook.run(create_sql)

        # Create indexes for historized tables
        if include_system_columns and self.historization_enabled and not is_temp:
            self._create_indexes(hook, table_name)

    def _create_staging_table_with_conn(self, cursor, enriched_data: List[Dict[str, Any]]) -> None:
        """
        Create staging TEMPORARY table using an existing cursor/connection.
        This is critical for ensuring the temp table is visible in subsequent operations.
        
        Args:
            cursor: Database cursor from an active connection
            enriched_data: Sample data with all columns including system columns
        """
        if not enriched_data:
            return
        
        sample_row = enriched_data[0]
        columns_def = []
        
        for col_name, value in sample_row.items():
            # Infer type from value
            if isinstance(value, bool):
                col_type = "BOOLEAN"
            elif isinstance(value, int):
                col_type = "BIGINT"
            elif isinstance(value, float):
                col_type = "DOUBLE PRECISION"
            elif isinstance(value, datetime):
                col_type = "TIMESTAMP"
            else:
                col_type = "TEXT"
            
            # System columns should be nullable for flexibility
            if col_name.startswith("_"):
                columns_def.append(f"{col_name} {col_type}")
            else:
                columns_def.append(f"{col_name} {col_type}")
        
        columns_sql = ", ".join(columns_def)
        
        # TEMPORARY tables don't use schema prefix
        create_sql = f"""
            CREATE TEMPORARY TABLE {self.staging_table_name} (
                {columns_sql}
            )
        """
        
        cursor.execute(create_sql)

    def _create_indexes(self, hook: PostgresHook, table_name: str) -> None:
        """
        Create indexes for historization queries.

        Args:
            hook: PostgresHook instance
            table_name: Name of table to create indexes on
        """
        indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_id_hash ON {self.schema}.{table_name}(_id_hash)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_record_hash ON {self.schema}.{table_name}(_record_hash)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_valid_to ON {self.schema}.{table_name}(_valid_to)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_deleted_at ON {self.schema}.{table_name}(_deleted_at)",
        ]

        for index_sql in indexes:
            try:
                hook.run(index_sql)
            except Exception as e:
                print(f"Warning: Could not create index: {e}")

    def _swap_tables(self, hook: PostgresHook) -> None:
        """
        Swap temp table with target table atomically.

        Strategy:
        - For 'replace' mode: DROP target table, RENAME temp table
        - For 'append' mode: INSERT FROM temp INTO target, DROP temp
        - For 'truncate' mode: TRUNCATE target, INSERT FROM temp, DROP temp

        Args:
            hook: PostgresHook instance
        """
        if self.write_mode == "replace":
            # Atomic swap: drop old, rename temp
            swap_sql = f"""
                BEGIN;
                DROP TABLE IF EXISTS {self.schema}.{self.table_name};
                ALTER TABLE {self.schema}.{self.temp_table_name} 
                    RENAME TO {self.table_name};
                COMMIT;
            """
        else:
            # Truncate if needed, then insert from temp
            truncate_sql = ""
            if self.write_mode == "truncate":
                truncate_sql = f"TRUNCATE TABLE {self.schema}.{self.table_name};"

            swap_sql = f"""
                BEGIN;
                {truncate_sql}
                INSERT INTO {self.schema}.{self.table_name}
                SELECT * FROM {self.schema}.{self.temp_table_name};
                DROP TABLE {self.schema}.{self.temp_table_name};
                COMMIT;
            """

        hook.run(swap_sql)

    def _merge_historized_data(
        self, hook: PostgresHook, staging_table: str, target_table: str, conn=None
    ) -> Dict[str, int]:
        """
        Merge staging data into historized target table using SCD Type 2 logic.

        Logic:
        1. Restore: Clear _deleted_at for records that reappear
        2. Supersede: Set _valid_to for records with changed hashes
        3. Insert: Add new versions of changed records + completely new records
        4. Delete: Set _deleted_at for records missing from staging

        Args:
            hook: PostgresHook instance
            staging_table: Name of staging table with new data (temp table, no schema)
            target_table: Name of target historized table
            conn: Optional database connection to use (required for temp tables)

        Returns:
            Dictionary with counts of restored, superseded, inserted, and deleted records
        """
        current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Staging table is TEMPORARY, so no schema prefix
        # Target table is permanent, so use schema prefix
        target_table_ref = f"{self.schema}.{target_table}"
        
        # Use provided connection or get a new one
        if conn is None:
            conn = hook.get_conn()
            should_close_conn = True
        else:
            should_close_conn = False

        # Step 1: Restore previously deleted records that reappear in source
        restore_sql = f"""
            UPDATE {target_table_ref} t
            SET _deleted_at = NULL,
                _valid_from = '{current_timestamp}'::TIMESTAMP
            WHERE t._deleted_at IS NOT NULL
              AND t._valid_to IS NULL
              AND EXISTS (
                  SELECT 1 FROM {staging_table} s
                  WHERE s._id_hash = t._id_hash
              )
        """
        
        # Step 2: Supersede changed records (hash mismatch)
        supersede_sql = f"""
            UPDATE {target_table_ref} t
            SET _valid_to = '{current_timestamp}'::TIMESTAMP
            WHERE t._valid_to IS NULL
              AND t._deleted_at IS NULL
              AND EXISTS (
                  SELECT 1 FROM {staging_table} s
                  WHERE s._id_hash = t._id_hash
                    AND s._record_hash != t._record_hash
              )
        """
        
        # Step 3: Insert new versions of changed records + new records
        # Get all columns except system columns for the insert
        # Use the provided connection to access temp table
        sample_query = f"SELECT * FROM {staging_table} LIMIT 1"
        
        # Get column names using the provided connection
        cursor = conn.cursor()
        cursor.execute(sample_query)
        all_columns = [desc[0] for desc in cursor.description]
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            
            # Filter out system columns from source (they'll be regenerated)
            data_columns = [col for col in all_columns if not col.startswith("_")]
            data_cols_str = ", ".join(data_columns)
            
            # Build system column values for insert
            insert_sql = f"""
                INSERT INTO {target_table_ref} (
                    {data_cols_str},
                    _record_hash, _id_hash, _valid_from, _valid_to, 
                    _deleted_at, _loaded_at, _pipeline_run_id, 
                    _source_system, _load_batch_id
                )
                SELECT 
                    {data_cols_str},
                    s._record_hash, s._id_hash, '{current_timestamp}'::TIMESTAMP, 
                    NULL, NULL, '{current_timestamp}'::TIMESTAMP, 
                    s._pipeline_run_id, s._source_system, s._load_batch_id
                FROM {staging_table} s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {target_table_ref} t
                    WHERE t._id_hash = s._id_hash
                      AND t._record_hash = s._record_hash
                      AND t._valid_to IS NULL
                      AND t._deleted_at IS NULL
                )
            """
        else:
            insert_sql = ""
        
        # Step 4: Mark deleted records (missing from staging)
        delete_sql = ""
        if self.track_deletes:
            delete_sql = f"""
                UPDATE {target_table_ref} t
                SET _deleted_at = '{current_timestamp}'::TIMESTAMP
                WHERE t._valid_to IS NULL
                  AND t._deleted_at IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM {staging_table} s
                      WHERE s._id_hash = t._id_hash
                  )
            """
        
        # Execute merge in transaction
        # CRITICAL: Use the same connection that created the temp table
        self.log.info("=" * 80)
        self.log.info("STARTING MERGE OPERATION")
        self.log.info("=" * 80)
        
        # Track actual row counts from SQL operations
        restored_count = 0
        superseded_count = 0
        inserted_count = 0
        deleted_count = 0
        
        try:
            # Execute each statement separately using the same cursor/connection
            self.log.info("Starting transaction...")
            cursor = conn.cursor()
            cursor.execute("BEGIN")
            self.log.info("Transaction started successfully")
            
            if restore_sql:
                self.log.info("Executing restore SQL...")
                cursor.execute(restore_sql)
                restored_count = cursor.rowcount
                self.log.info(f"Restore completed: {restored_count} rows affected")
            
            self.log.info("Executing supersede SQL...")
            cursor.execute(supersede_sql)
            superseded_count = cursor.rowcount
            self.log.info(f"Supersede completed: {superseded_count} rows affected")
            
            if insert_sql:
                self.log.info("Executing insert SQL...")
                cursor.execute(insert_sql)
                inserted_count = cursor.rowcount
                self.log.info(f"Insert completed: {inserted_count} rows affected")
            
            if delete_sql:
                self.log.info("Executing delete SQL...")
                cursor.execute(delete_sql)
                deleted_count = cursor.rowcount
                self.log.info(f"Delete completed: {deleted_count} rows affected")
            
            self.log.info("Committing transaction...")
            cursor.execute("COMMIT")
            cursor.close()
            conn.commit()  # Also commit on connection level
            self.log.info("Transaction committed successfully")
            self.log.info("=" * 80)
            self.log.info("MERGE OPERATION COMPLETED")
            self.log.info(f"Operation summary - Restored: {restored_count}, Superseded: {superseded_count}, Inserted: {inserted_count}, Deleted: {deleted_count}")
            self.log.info("=" * 80)
            
        except Exception as e:
            self.log.error("=" * 80)
            self.log.error(f"MERGE OPERATION FAILED: {str(e)}")
            self.log.error("=" * 80)
            try:
                cursor.execute("ROLLBACK")
                conn.rollback()
            except:
                pass
            raise
        
        # Only close connection if we created it (not provided by caller)
        if should_close_conn:
            conn.close()
        
        return {
            "restored": restored_count,
            "superseded": superseded_count,
            "inserted": inserted_count,
            "deleted": deleted_count,
        }

