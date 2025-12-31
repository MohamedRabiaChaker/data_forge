"""Shopify source implementation using ShopifyHook for OAuth authentication."""

from typing import Any, Dict, List, Optional

try:
    from ..sources.base_source import BaseSource
except ImportError:
    from base_source import BaseSource

# Import ShopifyHook
import sys
from pathlib import Path

# Add parent directory to path to import hooks
parent_dir = Path(__file__).parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

from hooks.shopify_hook import ShopifyHook


class ShopifySource(BaseSource):
    """
    Extract data from Shopify using ShopifyHook with OAuth authentication.
    
    This source uses Airflow connections to manage Shopify credentials securely.
    Tokens are generated fresh for each DAG run.

    Configuration:
        shopify_conn_id (str): Airflow connection ID for Shopify (default: 'shopify_default')
        resource (str): Resource type to extract ('products', 'orders', 'customers', etc.)
        limit (int): Number of items per page, max 250 (default: 250)
        fields (list, optional): Specific fields to retrieve
        updated_at_min (str, optional): ISO 8601 timestamp for incremental sync
        status (str, optional): Filter by status (e.g., 'active', 'any' for products)
        created_at_min (str, optional): Filter by creation date
        created_at_max (str, optional): Filter by creation date

    Example config:
        {
            "type": "shopify",
            "shopify_conn_id": "shopify_default",
            "resource": "products",
            "limit": 250,
            "fields": ["id", "title", "vendor", "product_type"],
            "status": "active"
        }

    Connection Setup in Airflow UI:
        Connection Type: HTTP
        Connection ID: shopify_default
        Host: your-shop.myshopify.com
        Login: your_api_key (client_id)
        Password: shpat_your_access_token or your_api_secret (client_secret)
        Extra: {
            "api_version": "2024-01"
        }
    
    See docs/SHOPIFY_OAUTH_SETUP.md for detailed setup instructions.
    """

    # Supported resources and their API endpoints
    SUPPORTED_RESOURCES = {
        'products': 'products.json',
        'orders': 'orders.json',
        'customers': 'customers.json',
        'inventory_items': 'inventory_items.json',
        'collections': 'collections.json',
        'custom_collections': 'custom_collections.json',
        'smart_collections': 'smart_collections.json',
    }

    def __init__(self, config: Dict[str, Any]):
        """Initialize Shopify source with configuration."""
        super().__init__(config)
        self.validate_config()

        # Get connection ID from config
        self.shopify_conn_id = config.get('shopify_conn_id', 'shopify_default')
        
        # Initialize ShopifyHook - handles all authentication
        self.hook = ShopifyHook(shopify_conn_id=self.shopify_conn_id)
        
        # Resource to extract
        self.resource = config['resource'].lower()
        
        # Optional parameters with defaults
        self.limit = min(config.get('limit', 250), 250)  # Max 250 per Shopify API
        self.fields = config.get('fields')
        
        # Filter parameters
        self.updated_at_min = config.get('updated_at_min')
        self.created_at_min = config.get('created_at_min')
        self.created_at_max = config.get('created_at_max')
        self.status = config.get('status')

    def validate_config(self) -> None:
        """
        Validate required configuration parameters.

        Raises:
            ValueError: If required parameters are missing or invalid
        """
        required_fields = ['resource']
        missing_fields = [
            field for field in required_fields if not self.config.get(field)
        ]

        if missing_fields:
            raise ValueError(
                f"Missing required configuration fields: {', '.join(missing_fields)}"
            )

        resource = self.config.get('resource', '').lower()
        if resource not in self.SUPPORTED_RESOURCES:
            raise ValueError(
                f"Unsupported resource '{resource}'. "
                f"Supported resources: {', '.join(self.SUPPORTED_RESOURCES.keys())}"
            )

    def _build_params(self, page_info: Optional[str] = None) -> Dict[str, Any]:
        """
        Build query parameters for API request.

        Args:
            page_info: Pagination cursor from previous request

        Returns:
            Dict with query parameters
        """
        params = {'limit': self.limit}

        # Use page_info for cursor-based pagination if available
        if page_info:
            params['page_info'] = page_info
        else:
            # Only add filters on first request (not with page_info)
            if self.fields:
                params['fields'] = ','.join(self.fields)
            if self.updated_at_min:
                params['updated_at_min'] = self.updated_at_min
            if self.created_at_min:
                params['created_at_min'] = self.created_at_min
            if self.created_at_max:
                params['created_at_max'] = self.created_at_max
            if self.status:
                params['status'] = self.status

        return params

    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract data from Shopify resource with automatic pagination.

        Returns:
            List[Dict[str, Any]]: List of dictionaries where each dictionary
                                  represents a Shopify resource (product, order, customer).

        Raises:
            Exception: If Shopify API call fails or authentication is invalid

        Example:
            For products:
                [
                    {
                        'id': 123456789,
                        'title': 'Sample Product',
                        'vendor': 'My Store',
                        'product_type': 'Electronics',
                        'created_at': '2024-01-01T00:00:00Z',
                        'updated_at': '2024-01-15T00:00:00Z',
                        'status': 'active',
                        ...
                    },
                    ...
                ]
        """
        endpoint = self.SUPPORTED_RESOURCES[self.resource]
        
        all_data = []
        page_count = 0

        print(f"Extracting {self.resource} from Shopify using connection: {self.shopify_conn_id}")

        # Use hook's pagination helper
        for item in self.hook.paginate(
            endpoint=endpoint,
            params=self._build_params(),
            limit=self.limit
        ):
            all_data.append(item)
            
            # Log progress every 100 items
            if len(all_data) % 100 == 0:
                print(f"  Retrieved {len(all_data)} {self.resource}...")

        print(f"Extraction complete: {len(all_data)} {self.resource} retrieved")
        return all_data

    def flatten_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Optional helper method to flatten nested Shopify data structures.

        Shopify API often returns nested objects (e.g., product variants, order line_items).
        This method can be called post-extraction if you need flattened data.

        Args:
            data: Original nested data from extract()

        Returns:
            Flattened list of dictionaries
        """
        # This is a placeholder for custom flattening logic
        # You can extend this based on your needs
        return data
