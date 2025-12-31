"""Shopify hook for OAuth-based authentication with Shopify Admin API."""

import logging
import time
from typing import Any, Dict, Optional

import requests
from airflow.providers.http.hooks.http import HttpHook
from requests import Response, Session


class ShopifyHook(HttpHook):
    """
    Hook for interacting with Shopify Admin API using OAuth authorization code grant.

    This hook handles:
    - OAuth token exchange using authorization code
    - Authenticated API requests to Shopify
    - Rate limiting and retry logic
    - Connection testing

    Connection Configuration in Airflow UI:
        Connection Type: HTTP
        Connection ID: shopify_default (or custom)
        Host: your-shop.myshopify.com
        Login: your_client_id (API key from Partners Dashboard)
        Password: your_client_secret (API secret key, starts with shpss_)
        Extra: {
            "authorization_code": "code_from_oauth_flow",
            "api_version": "2024-01"
        }

    Example usage:
        hook = ShopifyHook(shopify_conn_id='shopify_default')
        response = hook.run(endpoint='products.json', params={'limit': 250})
        products = response.json()['products']

    Setup Instructions:
        1. Create a Public App in Shopify Partners Dashboard:
           - Go to https://partners.shopify.com/
           - Apps > Create app > Public app
           - Configure API scopes (read_products, read_orders, etc.)
           - Set redirect URL for OAuth callback

        2. Get OAuth credentials:
           - In Partners Dashboard, go to your app
           - Copy "Client ID" and "Client secret" (starts with shpss_)

        3. Complete OAuth flow to get authorization code:
           - Visit: https://{shop}.myshopify.com/admin/oauth/authorize?
                    client_id={client_id}&scope={scopes}&redirect_uri={redirect_uri}
           - Authorize the app
           - Copy the 'code' parameter from redirect URL

        4. Create Airflow Connection:
           - Admin > Connections > Add
           - Fill in Host, Login (client_id), Password (client_secret)
           - Add authorization_code and api_version in Extra field

    Note: Authorization codes are short-lived. Once exchanged for an access token,
          that token can be reused for the lifetime of the DAG run.
    """

    conn_name_attr = "shopify_conn_id"
    default_conn_name = "shopify_default"
    conn_type = "http"
    hook_name = "Shopify"

    def __init__(
        self,
        shopify_conn_id: str = "shopify_default",
        api_version: Optional[str] = None,
    ):
        """
        Initialize Shopify hook.

        Args:
            shopify_conn_id: Airflow connection ID for Shopify
            api_version: Shopify API version (overrides connection extra)
        """
        # Initialize parent HttpHook with GET method as default
        super().__init__(method="GET", http_conn_id=shopify_conn_id)

        # Get connection details
        self.connection = self.get_connection(shopify_conn_id)
        extra = self.connection.extra_dejson

        # Extract configuration
        self.shop_url = self.connection.host
        if not self.shop_url:
            raise ValueError(
                f"Shopify connection '{shopify_conn_id}' must have 'host' set "
                "to your shop URL (e.g., 'your-shop.myshopify.com')"
            )

        # API credentials for OAuth
        self.client_id = self.connection.login
        self.client_secret = self.connection.password

        if not self.client_id or not self.client_secret:
            raise ValueError(
                f"Shopify connection '{shopify_conn_id}' must have 'login' (API key) "
                "and 'password' (API secret key) configured for OAuth"
            )

        # API version - prefer parameter, then connection extra, then default
        self.api_version = api_version or extra.get("api_version", "2024-01")

        # Build base URL for API calls
        self.base_url = f"https://{self.shop_url}/admin/api/{self.api_version}"

        # OAuth token (generated on-demand)
        self._access_token: Optional[str] = None

        self.log.info(
            f"Initialized ShopifyHook for shop: {self.shop_url}, "
            f"API version: {self.api_version}"
        )

    def _generate_access_token(self) -> str:
        """
        Generate OAuth access token using authorization code exchange.

        This implements the OAuth 2.0 authorization code grant flow for Shopify public apps.
        The authorization code must be obtained by completing the OAuth flow in a browser first.

        OAuth Flow:
        1. User authorizes app in browser (manual step)
        2. Shopify redirects with authorization code
        3. Store code in connection extra['authorization_code']
        4. This method exchanges code for access token

        Connection Configuration:
            Login: client_id (API key from Partners Dashboard)
            Password: client_secret (API secret key, starts with shpss_)
            Extra: {"authorization_code": "...", "api_version": "2024-01"}

        Returns:
            Access token string

        Raises:
            ValueError: If authorization code is missing
            Exception: If token exchange fails
        """
        extra = self.connection.extra_dejson
        
        # Check if authorization_code is in connection extra
        authorization_code = extra.get("authorization_code")
        if not authorization_code:
            raise ValueError(
                "No authorization code found in connection extra. "
                "You must complete the OAuth flow first to obtain an authorization code.\n\n"
                "Steps:\n"
                "1. Visit: https://{shop}.myshopify.com/admin/oauth/authorize?"
                "client_id={client_id}&scope={scopes}&redirect_uri={redirect_uri}\n"
                "2. Authorize the app\n"
                "3. Copy the 'code' parameter from redirect URL\n"
                "4. Add to connection extra: {'authorization_code': 'your_code'}"
            )

        self.log.info("Exchanging authorization code for access token")

        # Exchange authorization code for access token
        token_url = f"https://{self.shop_url}/admin/oauth/access_token"
        
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code": authorization_code,
        }

        try:
            response = requests.post(
                token_url,
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30,
            )
            response.raise_for_status()
            
            token_data = response.json()
            access_token = token_data.get("access_token")
            
            if not access_token:
                raise ValueError(f"No access_token in response: {token_data}")
            
            self.log.info("Successfully obtained access token via OAuth")
            
            # Log token metadata (if present)
            if "scope" in token_data:
                self.log.info(f"Granted scopes: {token_data['scope']}")
            if "expires_in" in token_data:
                self.log.info(f"Token expires in: {token_data['expires_in']} seconds")
            
            return access_token

        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to exchange authorization code for access token: {str(e)}"
            self.log.error(error_msg)
            
            # Try to extract more details from response
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_details = e.response.json()
                    self.log.error(f"Shopify error response: {error_details}")
                except Exception:
                    self.log.error(f"Response text: {e.response.text}")
            
            raise Exception(error_msg) from e

    def get_access_token(self) -> str:
        """
        Get or generate OAuth access token.

        Tokens are generated fresh for each DAG run as specified.

        Returns:
            Valid access token
        """
        # Generate fresh token for this DAG run
        self._access_token = self._generate_access_token()
        return self._access_token

    def get_conn(
        self,
        headers: Optional[Dict[str, Any]] = None,
        extra_options: Optional[Dict[str, Any]] = None,
    ) -> Session:
        """
        Get authenticated requests session for Shopify API.

        Args:
            headers: Additional headers to include
            extra_options: Additional options (not used, for compatibility)

        Returns:
            Configured requests.Session with authentication
        """
        # Get base session from parent
        session = super().get_conn(headers=headers, extra_options=extra_options)

        # Get access token
        access_token = self.get_access_token()

        # Add Shopify authentication headers
        session.headers.update(
            {
                "X-Shopify-Access-Token": access_token,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

        return session

    def run(  # type: ignore[override]
        self,
        endpoint: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET",
        json_data: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
        **kwargs,
    ) -> Response:
        """
        Make authenticated request to Shopify API with retry logic.

        Args:
            endpoint: API endpoint (e.g., 'products.json', 'orders.json')
            params: Query parameters for the request
            method: HTTP method (GET, POST, PUT, DELETE)
            json_data: JSON data for POST/PUT requests
            max_retries: Maximum number of retries for rate limiting
            **kwargs: Additional arguments passed to parent run()

        Returns:
            requests.Response object

        Raises:
            Exception: If request fails after retries

        Example:
            response = hook.run(
                endpoint='products.json',
                params={'limit': 250, 'status': 'active'}
            )
            products = response.json()['products']
        """
        if not endpoint:
            raise ValueError("endpoint parameter is required")

        self.method = method.upper()
        retry_delay = 2  # seconds

        for attempt in range(max_retries):
            try:
                # Build full URL
                url = f"{self.base_url}/{endpoint.lstrip('/')}"

                # Get authenticated session
                session = self.get_conn()

                # Prepare request
                if self.method == "GET":
                    response = session.get(url, params=params, **kwargs)
                elif self.method == "POST":
                    response = session.post(
                        url, params=params, json=json_data, **kwargs
                    )
                elif self.method == "PUT":
                    response = session.put(url, params=params, json=json_data, **kwargs)
                elif self.method == "DELETE":
                    response = session.delete(url, params=params, **kwargs)
                else:
                    raise ValueError(f"Unsupported HTTP method: {self.method}")

                # Handle rate limiting (429 Too Many Requests)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", retry_delay))
                    self.log.warning(
                        f"Rate limited (429). Waiting {retry_after} seconds "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(retry_after)
                    continue

                # Raise exception for other HTTP errors
                response.raise_for_status()

                # Respect Shopify rate limits proactively (2 requests/second)
                time.sleep(0.5)

                self.log.debug(
                    f"Successfully called {self.method} {endpoint} "
                    f"(status: {response.status_code})"
                )

                return response

            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    self.log.error(
                        f"Request failed after {max_retries} attempts: {str(e)}"
                    )
                    raise Exception(
                        f"Failed to call Shopify API {endpoint} after "
                        f"{max_retries} attempts: {str(e)}"
                    )

                self.log.warning(
                    f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}"
                )
                time.sleep(retry_delay * (attempt + 1))  # Exponential backoff

        raise Exception(f"Unexpected error calling {endpoint}")

    def get_shop_info(self) -> Dict[str, Any]:
        """
        Get shop information from Shopify.

        Useful for testing connection and getting shop metadata.

        Returns:
            Dictionary containing shop information

        Example:
            shop_info = hook.get_shop_info()
            shop_name = shop_info['shop']['name']
            shop_domain = shop_info['shop']['domain']
        """
        response = self.run(endpoint="shop.json")
        return response.json()

    def test_connection(self) -> tuple[bool, str]:  # type: ignore[override]
        """
        Test the Shopify connection.

        This method is called by Airflow UI when testing connections.

        Returns:
            Tuple of (success: bool, message: str)

        Example:
            success, message = hook.test_connection()
            if success:
                print(f"✓ {message}")
            else:
                print(f"✗ {message}")
        """
        try:
            # Try to get shop info
            shop_info = self.get_shop_info()
            shop_data = shop_info.get("shop", {})
            shop_name = shop_data.get("name", "Unknown")
            shop_domain = shop_data.get("domain", self.shop_url)

            return True, (
                f"Successfully connected to Shopify shop: {shop_name} ({shop_domain})"
            )

        except Exception as e:
            error_msg = str(e)
            self.log.error(f"Connection test failed: {error_msg}")

            # Provide helpful error messages
            if "401" in error_msg or "Unauthorized" in error_msg:
                return False, (
                    "Authentication failed. Please check your API credentials "
                    "(client_id and client_secret/access_token)"
                )
            elif "403" in error_msg or "Forbidden" in error_msg:
                return False, (
                    "Access forbidden. Please check your API scopes in Shopify Admin"
                )
            elif "404" in error_msg:
                return False, (
                    f"Shop not found. Please check the host: {self.shop_url}"
                )
            else:
                return False, f"Connection failed: {error_msg}"

    def paginate(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        limit: int = 250,
    ):
        """
        Helper method to paginate through Shopify API results.

        Args:
            endpoint: API endpoint (e.g., 'products.json')
            params: Additional query parameters
            limit: Results per page (max 250 for Shopify)

        Yields:
            Individual items from paginated results

        Example:
            for product in hook.paginate('products.json', limit=250):
                print(product['title'])
        """
        params = params or {}
        params["limit"] = min(limit, 250)  # Shopify max is 250

        page_info = None
        page_count = 0

        while True:
            page_count += 1

            # Add page_info for cursor-based pagination
            if page_info:
                params["page_info"] = page_info

            self.log.info(f"Fetching page {page_count} from {endpoint}")

            # Make request
            response = self.run(endpoint=endpoint, params=params)
            data = response.json()

            # Extract resource name from endpoint (e.g., 'products' from 'products.json')
            resource_name = endpoint.replace(".json", "").split("/")[-1]
            items = data.get(resource_name, [])

            if not items:
                self.log.info(f"No more items found on page {page_count}")
                break

            # Yield items
            for item in items:
                yield item

            # Check for next page using Link header
            page_info = self._extract_page_info(response)
            if not page_info:
                self.log.info("No more pages to fetch")
                break

    def _extract_page_info(self, response: Response) -> Optional[str]:
        """
        Extract page_info cursor from Link header for pagination.

        Args:
            response: HTTP response object

        Returns:
            page_info string if next page exists, None otherwise
        """
        link_header = response.headers.get("Link", "")
        if not link_header:
            return None

        # Parse Link header: <url>; rel="next"
        import re

        for link in link_header.split(","):
            if 'rel="next"' in link or "rel='next'" in link:
                # Extract page_info parameter from URL
                match = re.search(r"page_info=([^&>]+)", link)
                if match:
                    return match.group(1)

        return None
