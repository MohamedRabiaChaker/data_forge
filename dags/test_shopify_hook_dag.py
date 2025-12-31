"""Test DAG for Shopify Hook connection."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'test_shopify_hook',
    default_args=default_args,
    description='Test Shopify Hook OAuth and API connection',
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['test', 'shopify'],
) as dag:

    @task
    def test_connection():
        """Test Shopify connection and API access."""
        import sys
        sys.path.insert(0, '/opt/airflow/dags')
        
        from hooks.shopify_hook import ShopifyHook
        
        print("=" * 60)
        print("SHOPIFY HOOK CONNECTION TEST")
        print("=" * 60)
        
        # Initialize hook
        print("\n1. Initializing ShopifyHook...")
        hook = ShopifyHook(shopify_conn_id='shopify_default')
        print(f"   ✓ Hook initialized")
        print(f"   Shop URL: {hook.shop_url}")
        print(f"   API Version: {hook.api_version}")
        print(f"   Base URL: {hook.base_url}")
        
        # Test connection
        print("\n2. Testing connection...")
        success, message = hook.test_connection()
        print(f"   {message}")
        
        if not success:
            raise Exception(f"Connection test failed: {message}")
        
        # Get shop info
        print("\n3. Fetching shop information...")
        shop_info = hook.get_shop_info()
        shop = shop_info.get('shop', {})
        
        print(f"   ✓ Successfully connected!")
        print(f"   Shop Name: {shop.get('name')}")
        print(f"   Shop Owner: {shop.get('shop_owner')}")
        print(f"   Domain: {shop.get('domain')}")
        print(f"   Email: {shop.get('email')}")
        print(f"   Currency: {shop.get('currency')}")
        
        # Test pagination with products
        print("\n4. Testing pagination with products...")
        products = list(hook.paginate('products.json', limit=5))
        print(f"   ✓ Fetched {len(products)} products")
        
        if products:
            print(f"   Sample product: {products[0].get('title', 'N/A')}")
        
        print("\n" + "=" * 60)
        print("ALL TESTS PASSED!")
        print("=" * 60)
        
        return {
            'status': 'success',
            'shop_name': shop.get('name'),
            'product_count': len(products),
        }
    
    # Run the test
    test_connection()
