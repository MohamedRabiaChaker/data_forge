#!/usr/bin/env python3
"""Test script to verify Shopify hook connection works."""

import sys
import os

# Add dags directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dags'))

# Suppress Airflow warnings
os.environ['AIRFLOW__CORE__LOAD_EXAMPLES'] = 'False'
os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = '/opt/airflow/dags'

from airflow.models import Connection
from airflow import settings

def test_shopify_connection():
    """Test Shopify connection using ShopifyHook."""
    print("=" * 60)
    print("SHOPIFY CONNECTION TEST")
    print("=" * 60)
    
    try:
        # Get session
        session = settings.Session()
        
        # Query the connection
        conn = session.query(Connection).filter(
            Connection.conn_id == 'shopify_default'
        ).first()
        
        if not conn:
            print("✗ Connection 'shopify_default' not found in Airflow")
            print("\nPlease create the connection in Airflow UI:")
            print("  Admin > Connections > Add")
            print("  Connection ID: shopify_default")
            print("  Connection Type: HTTP")
            print("  Host: your-shop.myshopify.com")
            print("  Login: your_api_key")
            print("  Password: your_access_token")
            print('  Extra: {"api_version": "2024-01"}')
            session.close()
            return False
            
        print(f"✓ Connection found: {conn.conn_id}")
        print(f"  Connection Type: {conn.conn_type}")
        print(f"  Host: {conn.host}")
        print(f"  Login (API Key): {conn.login[:10]}..." if conn.login else "  Login: None")
        print(f"  Password configured: {bool(conn.password)}")
        print(f"  Extra: {conn.extra}")
        
        session.close()
        
        # Now test with the hook
        print("\nTesting ShopifyHook import...")
        from hooks.shopify_hook import ShopifyHook
        print("✓ ShopifyHook imported successfully")
        
        print("\nInitializing ShopifyHook...")
        hook = ShopifyHook(shopify_conn_id='shopify_default')
        print(f"✓ Hook initialized")
        print(f"  Shop URL: {hook.shop_url}")
        print(f"  API Version: {hook.api_version}")
        print(f"  Base URL: {hook.base_url}")
        
        print("\nTesting connection to Shopify API...")
        success, message = hook.test_connection()
        
        if success:
            print(f"✓ {message}")
            
            # Try to get shop info
            print("\nFetching shop information...")
            shop_info = hook.get_shop_info()
            shop = shop_info.get('shop', {})
            
            print(f"✓ Successfully connected to Shopify!")
            print(f"  Shop Name: {shop.get('name')}")
            print(f"  Shop Owner: {shop.get('shop_owner')}")
            print(f"  Domain: {shop.get('domain')}")
            print(f"  Email: {shop.get('email')}")
            print(f"  Currency: {shop.get('currency')}")
            print(f"  Timezone: {shop.get('timezone')}")
            
            return True
        else:
            print(f"✗ {message}")
            return False
            
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_shopify_connection()
    sys.exit(0 if success else 1)
