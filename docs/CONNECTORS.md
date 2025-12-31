# Connector Reference

Complete reference for all supported sources, transforms, and destinations in DataForge.

## Sources

### Google Sheets (`gsheet`)

Extract data from Google Sheets using service account authentication.

#### Configuration

```json
{
  "type": "gsheet",
  "gcp_conn_id": "google_cloud_default",
  "spreadsheet_id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
  "range_name": "Sheet1!A1:Z1000"
}
```

#### Parameters

- `type` (required): `"gsheet"`
- `gcp_conn_id` (required): Airflow connection ID for Google Cloud Platform
- `spreadsheet_id` (required): Google Sheets spreadsheet ID (from URL)
- `range_name` (required): Range in A1 notation (e.g., `Sheet1!A1:Z1000`)

#### Features

- Automatic header detection (first row)
- Handles empty sheets gracefully
- Handles missing columns
- Supports any range in A1 notation
- Uses Google Sheets API via GSheetsHook

#### Connection Required

Google Cloud Platform connection with service account JSON key. See [Connection Setup](CONNECTION_SETUP.md#google-cloud--google-sheets-connection).

#### Example

```json
{
  "source": {
    "type": "gsheet",
    "gcp_conn_id": "google_cloud_default",
    "spreadsheet_id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
    "range_name": "Sales Data!A1:Z"
  }
}
```

---

### Shopify (`shopify`)

Extract data from Shopify Admin API using OAuth authentication.

#### Configuration

```json
{
  "type": "shopify",
  "shopify_conn_id": "shopify_default",
  "resource": "products",
  "limit": 250,
  "status": "active",
  "updated_at_min": "2025-01-01T00:00:00Z"
}
```

#### Parameters

- `type` (required): `"shopify"`
- `shopify_conn_id` (required): Airflow connection ID for Shopify
- `resource` (required): Resource type to extract (see below)
- `limit` (optional): Records per page (default: 250, max: 250)
- `status` (optional): Filter by status (`active`, `draft`, `archived`, `any`)
- `fields` (optional): List of fields to include (reduces payload size)
- `created_at_min` (optional): Filter by creation date (ISO 8601)
- `created_at_max` (optional): Filter by creation date (ISO 8601)
- `updated_at_min` (optional): Filter by update date (ISO 8601) - for incremental sync
- `updated_at_max` (optional): Filter by update date (ISO 8601)

#### Supported Resources

| Resource | Description | Common Use Case |
|----------|-------------|-----------------|
| `products` | Products, variants, images | Product catalog sync |
| `orders` | Orders, line items, transactions | Sales analytics |
| `customers` | Customer information | CRM integration |
| `inventory_items` | Inventory levels | Inventory management |
| `collections` | Product collections | Catalog organization |
| `custom_collections` | Manual collections | Marketing segments |
| `smart_collections` | Automated collections | Dynamic grouping |

#### Features

- **Automatic Pagination**: Fetches all records automatically
- **Rate Limiting**: Respects Shopify API limits (2 req/sec)
- **Incremental Sync**: Use `updated_at_min` for delta loads
- **Field Selection**: Optimize payload with `fields` parameter
- **Status Filtering**: Filter by record status
- **OAuth Authentication**: Secure token-based auth via ShopifyHook

#### Connection Required

HTTP connection with Shopify Admin API access token. See [Connection Setup](CONNECTION_SETUP.md#shopify-connection).

#### Examples

**Full Product Sync:**
```json
{
  "source": {
    "type": "shopify",
    "shopify_conn_id": "shopify_default",
    "resource": "products",
    "limit": 250,
    "status": "active"
  }
}
```

**Incremental Order Sync:**
```json
{
  "source": {
    "type": "shopify",
    "shopify_conn_id": "shopify_default",
    "resource": "orders",
    "limit": 250,
    "status": "any",
    "updated_at_min": "{{ (execution_date - macros.timedelta(hours=2)).isoformat() }}"
  }
}
```

**Optimized Customer Extract:**
```json
{
  "source": {
    "type": "shopify",
    "shopify_conn_id": "shopify_default",
    "resource": "customers",
    "fields": ["id", "email", "first_name", "last_name", "created_at", "updated_at"],
    "limit": 250
  }
}
```

---

## Transforms

### Normalize Columns (`normalize_columns`)

Convert column names to a standard format (snake_case).

#### Configuration

```json
{
  "type": "normalize_columns",
  "lowercase": true,
  "replace_spaces": "_",
  "remove_special_chars": true
}
```

#### Parameters

- `type` (required): `"normalize_columns"`
- `lowercase` (optional): Convert to lowercase (default: true)
- `replace_spaces` (optional): Character to replace spaces with (default: `"_"`)
- `remove_special_chars` (optional): Remove special characters (default: true)

#### Examples

**Before:**
- `Product Name`
- `Total-Price (USD)`
- `Customer's Email`

**After (with default settings):**
- `product_name`
- `total_price_usd`
- `customers_email`

---

### Filter Rows (`filter_rows`)

Filter rows based on column value conditions.

#### Configuration

```json
{
  "type": "filter_rows",
  "column": "status",
  "operator": "eq",
  "value": "active"
}
```

#### Parameters

- `type` (required): `"filter_rows"`
- `column` (required): Column name to filter on
- `operator` (required): Comparison operator (see below)
- `value` (required): Value to compare against

#### Supported Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `eq` | Equal to | `"status" == "active"` |
| `ne` | Not equal to | `"status" != "draft"` |
| `gt` | Greater than | `"price" > 100` |
| `lt` | Less than | `"price" < 1000` |
| `gte` | Greater than or equal | `"quantity" >= 10` |
| `lte` | Less than or equal | `"quantity" <= 100` |
| `contains` | Contains substring | `"email" contains "@gmail.com"` |
| `in` | Value in list | `"category" in ["shoes", "boots"]` |

#### Examples

**Filter Active Records:**
```json
{
  "type": "filter_rows",
  "column": "status",
  "operator": "eq",
  "value": "active"
}
```

**Filter by Price Range:**
```json
{
  "type": "filter_rows",
  "column": "price",
  "operator": "gte",
  "value": 50
}
```

**Filter by Email Domain:**
```json
{
  "type": "filter_rows",
  "column": "email",
  "operator": "contains",
  "value": "@company.com"
}
```

---

### Select Columns (`select_columns`)

Include or exclude specific columns from the dataset.

#### Configuration

**Include Mode:**
```json
{
  "type": "select_columns",
  "include": ["id", "name", "email", "created_at"]
}
```

**Exclude Mode:**
```json
{
  "type": "select_columns",
  "exclude": ["internal_notes", "password_hash", "ssn"]
}
```

#### Parameters

- `type` (required): `"select_columns"`
- `include` (optional): List of columns to include (whitelist)
- `exclude` (optional): List of columns to exclude (blacklist)

**Note:** Use either `include` or `exclude`, not both.

#### Examples

**Keep Only Essential Columns:**
```json
{
  "type": "select_columns",
  "include": ["id", "email", "first_name", "last_name", "created_at"]
}
```

**Remove Sensitive Data:**
```json
{
  "type": "select_columns",
  "exclude": ["password", "ssn", "credit_card", "internal_notes"]
}
```

---

## Destinations

### PostgreSQL (`postgres`)

Load data into PostgreSQL with intelligent loading strategies.

#### Configuration

```json
{
  "type": "postgres",
  "postgres_conn_id": "postgres_default",
  "schema": "public",
  "table_name": "imported_data",
  "write_mode": "replace",
  "create_table": true,
  "temp_table_threshold": 1000
}
```

#### Parameters

- `type` (required): `"postgres"`
- `postgres_conn_id` (required): Airflow connection ID for PostgreSQL
- `schema` (required): Database schema (e.g., `"public"`)
- `table_name` (required): Target table name
- `write_mode` (required): Write strategy (see below)
- `create_table` (optional): Auto-create table if doesn't exist (default: false)
- `temp_table_threshold` (optional): Row count threshold for temp table strategy (default: 1000)

#### Write Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `append` | Add rows to existing table | Incremental loads, event logs |
| `truncate` | Clear table, then insert | Daily full refresh |
| `replace` | Drop and recreate table (atomic) | Full refresh with schema changes |

#### Features

**Auto Schema Inference:**
Automatically detects PostgreSQL column types from data:
- `bool` → `BOOLEAN`
- `int` → `INTEGER`
- `float` → `DOUBLE PRECISION`
- `str` → `TEXT`

**Smart Loading Strategy:**
- **Small datasets (<1000 rows)**: Direct insert
- **Large datasets (≥1000 rows)**: Temp table + atomic swap

**Transaction Safety:**
All operations wrapped in transactions with automatic rollback on failure.

**Pre/Post Hooks:**
Execute custom SQL before and after data load.

#### Connection Required

PostgreSQL connection. See [Connection Setup](CONNECTION_SETUP.md#postgresql-connection).

#### Examples

**Append Mode (Incremental):**
```json
{
  "destination": {
    "type": "postgres",
    "postgres_conn_id": "postgres_default",
    "schema": "public",
    "table_name": "orders",
    "write_mode": "append",
    "create_table": true
  }
}
```

**Replace Mode (Full Refresh):**
```json
{
  "destination": {
    "type": "postgres",
    "postgres_conn_id": "postgres_default",
    "schema": "analytics",
    "table_name": "products",
    "write_mode": "replace",
    "create_table": true,
    "temp_table_threshold": 1000
  }
}
```

**Truncate Mode (Daily Snapshot):**
```json
{
  "destination": {
    "type": "postgres",
    "postgres_conn_id": "postgres_default",
    "schema": "public",
    "table_name": "daily_sales",
    "write_mode": "truncate",
    "create_table": false
  }
}
```

---

## Transform Chaining

Transforms are applied sequentially. Data flows through each transform in order:

```json
{
  "transforms": [
    {
      "type": "normalize_columns",
      "lowercase": true
    },
    {
      "type": "filter_rows",
      "column": "status",
      "operator": "eq",
      "value": "active"
    },
    {
      "type": "select_columns",
      "include": ["id", "name", "email", "created_at"]
    }
  ]
}
```

**Execution order:**
1. Normalize column names to snake_case
2. Filter rows where status = 'active'
3. Keep only specified columns

---

## Adding Custom Connectors

See [Extending DataForge](EXTENDING.md) for instructions on adding custom sources, transforms, and destinations.
