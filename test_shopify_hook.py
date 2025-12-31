#!/usr/bin/env python3
"""
Unit tests for ShopifyHook.

Tests basic functionality without requiring actual Shopify API access.
For integration tests with real API, use the test DAG.
"""

import sys
import os
from unittest.mock import Mock, patch, MagicMock

# Add dags directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dags'))


def test_shopify_hook_import():
    """Test that ShopifyHook can be imported."""
    print("=" * 80)
    print("TEST 1: ShopifyHook Import")
    print("=" * 80)
    
    try:
        from hooks.shopify_hook import ShopifyHook
        print("✓ ShopifyHook imported successfully")
        print(f"  Hook name: {ShopifyHook.hook_name}")
        print(f"  Connection type: {ShopifyHook.conn_type}")
        print(f"  Default connection: {ShopifyHook.default_conn_name}")
        return True
    except Exception as e:
        print(f"✗ Failed to import ShopifyHook: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_shopify_hook_initialization():
    """Test ShopifyHook initialization with mocked connection."""
    print("\n" + "=" * 80)
    print("TEST 2: ShopifyHook Initialization")
    print("=" * 80)
    
    try:
        from hooks.shopify_hook import ShopifyHook
        
        # Create a mock connection
        mock_conn = Mock()
        mock_conn.host = "test-shop.myshopify.com"
        mock_conn.login = "test_api_key"
        mock_conn.password = "test_access_token"
        mock_conn.extra_dejson = {"api_version": "2024-01"}
        
        # Patch get_connection to return our mock
        with patch.object(ShopifyHook, 'get_connection', return_value=mock_conn):
            hook = ShopifyHook(shopify_conn_id='test_shopify')
            
            assert hook.shop_url == "test-shop.myshopify.com", "Shop URL mismatch"
            assert hook.client_id == "test_api_key", "Client ID mismatch"
            assert hook.client_secret == "test_access_token", "Client secret mismatch"
            assert hook.api_version == "2024-01", "API version mismatch"
            assert hook.base_url == "https://test-shop.myshopify.com/admin/api/2024-01", "Base URL mismatch"
            
            print("✓ Hook initialized correctly")
            print(f"  Shop URL: {hook.shop_url}")
            print(f"  API Version: {hook.api_version}")
            print(f"  Base URL: {hook.base_url}")
            return True
            
    except Exception as e:
        print(f"✗ Initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_supported_resources():
    """Test that all supported Shopify resources are defined."""
    print("\n" + "=" * 80)
    print("TEST 3: Supported Resources")
    print("=" * 80)
    
    try:
        from hooks.shopify_hook import ShopifyHook
        
        expected_resources = {
            'products', 'orders', 'customers', 'inventory_items',
            'inventory_levels', 'locations', 'variants'
        }
        
        actual_resources = set(ShopifyHook.SUPPORTED_RESOURCES.keys())
        
        if actual_resources == expected_resources:
            print(f"✓ All {len(expected_resources)} resources supported:")
            for resource in sorted(expected_resources):
                endpoint = ShopifyHook.SUPPORTED_RESOURCES[resource]
                print(f"  - {resource}: {endpoint}")
            return True
        else:
            missing = expected_resources - actual_resources
            extra = actual_resources - expected_resources
            
            if missing:
                print(f"✗ Missing resources: {missing}")
            if extra:
                print(f"⚠  Extra resources: {extra}")
            return False
            
    except Exception as e:
        print(f"✗ Resource check failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_rate_limiting_config():
    """Test rate limiting configuration."""
    print("\n" + "=" * 80)
    print("TEST 4: Rate Limiting Configuration")
    print("=" * 80)
    
    try:
        from hooks.shopify_hook import ShopifyHook
        
        # Create a mock connection
        mock_conn = Mock()
        mock_conn.host = "test-shop.myshopify.com"
        mock_conn.login = "test_api_key"
        mock_conn.password = "test_access_token"
        mock_conn.extra_dejson = {}
        
        with patch.object(ShopifyHook, 'get_connection', return_value=mock_conn):
            hook = ShopifyHook()
            
            # Check default rate limiting settings
            assert hook.rate_limit_delay == 0.5, "Default rate limit delay should be 0.5s"
            assert hook.max_retries == 3, "Default max retries should be 3"
            assert hook.retry_delay == 2, "Default retry delay should be 2s"
            
            print("✓ Rate limiting configured correctly")
            print(f"  Rate limit delay: {hook.rate_limit_delay}s (2 req/sec)")
            print(f"  Max retries: {hook.max_retries}")
            print(f"  Retry delay: {hook.retry_delay}s")
            return True
            
    except Exception as e:
        print(f"✗ Rate limiting check failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_api_version_override():
    """Test that API version can be overridden."""
    print("\n" + "=" * 80)
    print("TEST 5: API Version Override")
    print("=" * 80)
    
    try:
        from hooks.shopify_hook import ShopifyHook
        
        # Test 1: Default version
        mock_conn = Mock()
        mock_conn.host = "test-shop.myshopify.com"
        mock_conn.login = "test_api_key"
        mock_conn.password = "test_access_token"
        mock_conn.extra_dejson = {}
        
        with patch.object(ShopifyHook, 'get_connection', return_value=mock_conn):
            hook = ShopifyHook()
            assert hook.api_version == "2024-01", "Default API version should be 2024-01"
            print("  ✓ Default version: 2024-01")
        
        # Test 2: Version from connection extra
        mock_conn.extra_dejson = {"api_version": "2023-10"}
        with patch.object(ShopifyHook, 'get_connection', return_value=mock_conn):
            hook = ShopifyHook()
            assert hook.api_version == "2023-10", "Should use version from connection extra"
            print("  ✓ Version from connection extra: 2023-10")
        
        # Test 3: Version from parameter (highest priority)
        with patch.object(ShopifyHook, 'get_connection', return_value=mock_conn):
            hook = ShopifyHook(api_version="2023-07")
            assert hook.api_version == "2023-07", "Should use version from parameter"
            print("  ✓ Version from parameter: 2023-07")
        
        print("\n✓ API version override works correctly")
        return True
        
    except Exception as e:
        print(f"✗ API version test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_pagination_params():
    """Test pagination parameter generation."""
    print("\n" + "=" * 80)
    print("TEST 6: Pagination Parameters")
    print("=" * 80)
    
    try:
        from hooks.shopify_hook import ShopifyHook
        
        mock_conn = Mock()
        mock_conn.host = "test-shop.myshopify.com"
        mock_conn.login = "test_api_key"
        mock_conn.password = "test_access_token"
        mock_conn.extra_dejson = {}
        
        with patch.object(ShopifyHook, 'get_connection', return_value=mock_conn):
            hook = ShopifyHook()
            
            # Test default limit
            params = {}
            hook._add_pagination_params(params, limit=None, page_info=None)
            assert params.get('limit') == 250, "Default limit should be 250"
            print("  ✓ Default limit: 250")
            
            # Test custom limit
            params = {}
            hook._add_pagination_params(params, limit=50, page_info=None)
            assert params.get('limit') == 50, "Custom limit should be 50"
            print("  ✓ Custom limit: 50")
            
            # Test page_info
            params = {}
            hook._add_pagination_params(params, limit=100, page_info="next_page_token")
            assert params.get('page_info') == "next_page_token", "Page info should be set"
            assert params.get('limit') == 100, "Limit should still be set"
            print("  ✓ Page info parameter: next_page_token")
            
        print("\n✓ Pagination parameters work correctly")
        return True
        
    except Exception as e:
        print(f"✗ Pagination test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("\n" + "=" * 80)
    print("SHOPIFY HOOK UNIT TEST SUITE")
    print("=" * 80 + "\n")
    
    tests = [
        test_shopify_hook_import,
        test_shopify_hook_initialization,
        test_supported_resources,
        test_rate_limiting_config,
        test_api_version_override,
        test_pagination_params,
    ]
    
    results = []
    for test_func in tests:
        try:
            passed = test_func()
            results.append((test_func.__name__, passed))
        except Exception as e:
            print(f"\n✗ Test {test_func.__name__} raised unexpected exception:")
            print(f"  {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_func.__name__, False))
    
    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    
    passed_count = sum(1 for _, passed in results if passed)
    total_count = len(results)
    
    for test_name, passed in results:
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"  {status}: {test_name}")
    
    print(f"\nTotal: {passed_count}/{total_count} tests passed")
    
    if passed_count == total_count:
        print("\n✅ All tests passed!")
        return 0
    else:
        print(f"\n❌ {total_count - passed_count} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
