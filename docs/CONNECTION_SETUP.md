# Connection Setup Guide

This guide provides detailed instructions for configuring connections in Airflow for the DataForge ETL framework.

## Overview

Connections in Airflow store credentials and configuration for external systems. The ETL framework uses connections to authenticate with data sources and destinations.

## Accessing the Connections UI

1. Start Airflow: `docker compose -f docker-compose.dev.yaml up -d`
2. Open browser: http://localhost:8081
3. Login with username: `airflow`, password: `airflow`
4. Navigate to: **Admin > Connections**

## Google Cloud / Google Sheets Connection

### Prerequisites

1. **Create a Google Cloud Project**
   - Go to https://console.cloud.google.com/
   - Create a new project or select existing

2. **Enable Google Sheets API**
   - In Google Cloud Console, go to **APIs & Services > Library**
   - Search for "Google Sheets API"
   - Click **Enable**

3. **Create Service Account**
   - Go to **IAM & Admin > Service Accounts**
   - Click **Create Service Account**
   - Name it (e.g., "airflow-etl")
   - Grant role: **Editor** (or minimum required permissions)
   - Click **Done**

4. **Generate JSON Key**
   - Click on the service account you created
   - Go to **Keys** tab
   - Click **Add Key > Create New Key**
   - Select **JSON** format
   - Download the key file (keep it secure!)

5. **Share Google Sheet with Service Account**
   - Open your Google Sheet
   - Click **Share** button
   - Add the service account email (found in the JSON key, looks like `xxx@xxx.iam.gserviceaccount.com`)
   - Grant **Viewer** or **Editor** permission

### Configure in Airflow

1. In Airflow UI, go to **Admin > Connections**
2. Click the **+** button to add a new connection
3. Fill in the details:

   - **Connection Id**: `google_cloud_default`
   - **Connection Type**: `Google Cloud`
   - **Keyfile Path**: Leave empty
   - **Keyfile JSON**: Paste the entire contents of your service account JSON key
   - **Project Id**: Your Google Cloud project ID (optional)
   - **Scopes**: Leave default or add: `https://www.googleapis.com/auth/spreadsheets.readonly`

4. Click **Save**

### Get Your Spreadsheet ID

From your Google Sheets URL:
```
https://docs.google.com/spreadsheets/d/1ABC-xyz123_DEF456/edit
                                     ^^^^^^^^^^^^^^^^^^^
                                     This is your spreadsheet_id
```

### Troubleshooting

**"Permission Denied" Error**
- Verify sheet is shared with service account email
- Check Google Sheets API is enabled in GCP
- Ensure JSON key is valid and properly formatted

**"Invalid Credentials" Error**
- Ensure the JSON key is copied completely (including `{` and `}`)
- Verify the connection ID matches what's in your config
- Check that the service account hasn't been deleted or disabled

## Shopify Connection

### Prerequisites

1. **Create a Custom App in Shopify Admin**
   - Go to Settings > Apps and sales channels > Develop apps
   - Click "Create an app" and give it a name (e.g., "Airflow ETL")
   
2. **Configure Admin API Scopes**
   - Click "Configure Admin API scopes"
   - Select required scopes based on your data needs:
     - `read_products` - For extracting products
     - `read_orders` - For extracting orders
     - `read_customers` - For extracting customers
     - `read_inventory` - For extracting inventory
   - Click "Save"
   
3. **Install the App**
   - Click "Install app" button
   - Confirm installation
   
4. **Get Admin API Access Token**
   - After installation, go to "API credentials" tab
   - Click "Reveal token once" under "Admin API access token"
   - Copy the token (starts with `shpat_`) - **this is shown only once!**

### Configure in Airflow

1. In Airflow UI, go to **Admin > Connections**
2. Click the **+** button to add a new connection
3. Fill in the details:

   - **Connection Id**: `shopify_default`
   - **Connection Type**: `HTTP`
   - **Host**: `your-shop.myshopify.com` (your shop URL without https://)
   - **Login**: Leave empty (optional: API key for reference)
   - **Password**: `shpat_xxxxx` (your Admin API access token)
   - **Extra**: 
     ```json
     {
       "api_version": "2024-01"
     }
     ```

4. Click **Save**

### Get Your Shop URL

Your shop URL is the subdomain you chose when creating your Shopify store:
```
https://your-shop.myshopify.com/admin
           ^^^^^^^^^^^^^^^^^^
           This is your shop URL (without https://)
```

### Troubleshooting

**"Authentication failed" Error**
- Verify you copied the complete access token (starts with `shpat_`)
- Check that you revealed and copied the token correctly (it's only shown once!)
- If you lost the token, you can regenerate it in Shopify Admin API credentials

**"Access forbidden" Error**
- Check that you configured the correct API scopes in Shopify
- Ensure the app is installed
- Verify you're requesting data that matches your scopes (e.g., `read_products` for products)

**"Shop not found" Error**
- Verify the Host field is correct: `your-shop.myshopify.com` (without https://)
- Check for typos in your shop URL
- Ensure your shop is active

**"Invalid API version" Error**
- Update the `api_version` in the Extra field
- Shopify supports versions like: `2024-01`, `2024-04`, `2024-07`, `2024-10`
- See [Shopify API Versioning](https://shopify.dev/docs/api/usage/versioning)

## PostgreSQL Connection

### Using Docker Compose PostgreSQL (Local Development)

If using the included PostgreSQL database in `docker-compose.dev.yaml`:

1. In Airflow UI, go to **Admin > Connections**
2. Click **+** to add a new connection
3. Fill in the details:

   - **Connection Id**: `postgres_default`
   - **Connection Type**: `Postgres`
   - **Host**: `postgres` (service name from docker-compose)
   - **Schema**: `airflow` (or create your own schema)
   - **Login**: `airflow`
   - **Password**: `airflow`
   - **Port**: `5432`

4. Click **Save**

### Using External PostgreSQL

For production or external PostgreSQL:

1. In Airflow UI, go to **Admin > Connections**
2. Click **+** to add a new connection
3. Fill in the details:

   - **Connection Id**: `postgres_default` (or custom name)
   - **Connection Type**: `Postgres`
   - **Host**: Your PostgreSQL host (e.g., `mydb.example.com`)
   - **Schema**: Your target schema (e.g., `public`, `analytics`)
   - **Login**: PostgreSQL username
   - **Password**: PostgreSQL password
   - **Port**: `5432` (or custom port)

4. Click **Save**

**Security Note**: For production, use Airflow's secrets backend or environment variables instead of storing passwords in the UI.

### Troubleshooting

**"Connection Refused" Error**
- If using Docker Compose Postgres, use host `postgres` (not `localhost`)
- Verify the PostgreSQL service is running: `docker compose -f docker-compose.dev.yaml ps`
- Check credentials match the docker-compose.yaml configuration
- Ensure port 5432 is correct

**"Database Does Not Exist" Error**
- Create the database first or use existing one (e.g., `airflow`)
- Or update the Schema field in the connection
- Ensure the user has CREATE permissions if `create_table: true`

**"Permission Denied" Error**
- Verify the user has necessary permissions:
  ```sql
  GRANT ALL PRIVILEGES ON SCHEMA public TO airflow;
  GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
  ```
- Check the schema exists
- Ensure user has CREATE TABLE permissions if using `create_table: true`

## Connection Best Practices

### 1. Use Descriptive Connection IDs

Instead of generic names, use descriptive ones:
- ❌ `postgres_default`
- ✅ `postgres_analytics_prod`
- ✅ `gcp_reporting_service_account`

### 2. Separate Dev/Prod Connections

Create separate connections for each environment:
- `postgres_dev`
- `postgres_staging`
- `postgres_prod`

### 3. Secure Secrets Management (Production)

For production deployments, use:
- **AWS Secrets Manager**: `AIRFLOW__SECRETS__BACKEND=airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend`
- **HashiCorp Vault**: `AIRFLOW__SECRETS__BACKEND=airflow.providers.hashicorp.secrets.vault.VaultBackend`
- **Google Secret Manager**: `AIRFLOW__SECRETS__BACKEND=airflow.providers.google.cloud.secrets.secret_manager.SecretManagerBackend`

## Testing Connections

### Using Airflow CLI

Test connections from the command line:

```bash
# List all connections
docker compose -f docker-compose.dev.yaml exec airflow-api-server \
  airflow connections list

# Test a specific connection
docker compose -f docker-compose.dev.yaml exec airflow-api-server \
  airflow connections get google_cloud_default
```

### Using Python Task

Create a test DAG to verify connections:

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id='test_connections',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
)
def test_connections():
    @task
    def test_gsheet():
        from airflow.providers.google.suite.hooks.sheets import GSheetsHook
        hook = GSheetsHook(gcp_conn_id='google_cloud_default')
        return "GSheets connection OK"
    
    @task
    def test_postgres():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id='postgres_default')
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
        return f"Postgres connection OK: {result}"
    
    test_gsheet()
    test_postgres()

test_connections()
```
