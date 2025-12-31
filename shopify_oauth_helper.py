#!/usr/bin/env python3
"""
Shopify OAuth Helper Script

This script helps you complete the OAuth flow and capture the authorization code
that you'll need to configure your Airflow Shopify connection.

Usage:
    python shopify_oauth_helper.py

The script will:
1. Ask for your app credentials and shop URL
2. Generate the authorization URL
3. Start a local server to capture the OAuth callback
4. Display the authorization code for you to copy into Airflow
"""

import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, urlencode
import webbrowser


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """HTTP request handler for OAuth callback."""
    
    authorization_code = None
    
    def do_GET(self):
        """Handle GET request from Shopify OAuth redirect."""
        query = urlparse(self.path).query
        params = parse_qs(query)
        
        code = params.get('code', [None])[0]
        error = params.get('error', [None])[0]
        shop = params.get('shop', [None])[0]
        
        if error:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(f"<h1>OAuth Error</h1><p>{error}</p>".encode())
            print(f"\n❌ OAuth error: {error}\n")
            return
        
        if code:
            OAuthCallbackHandler.authorization_code = code
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            
            html = f"""
            <html>
                <head><title>Shopify OAuth Success</title></head>
                <body style="font-family: Arial, sans-serif; padding: 50px;">
                    <h1 style="color: green;">✓ Authorization Successful!</h1>
                    <p>Authorization code has been captured.</p>
                    <p><strong>Shop:</strong> {shop}</p>
                    <p>You can close this window and return to your terminal.</p>
                </body>
            </html>
            """
            self.wfile.write(html.encode())
            
            print(f"\n{'=' * 80}")
            print("✓ AUTHORIZATION SUCCESSFUL")
            print('=' * 80)
            print(f"Shop: {shop}")
            print(f"\nAuthorization Code:")
            print(f"  {code}")
            print('\n' + '=' * 80)
            print("\nNext Steps:")
            print("1. Copy the authorization code above")
            print("2. Go to Airflow UI > Admin > Connections")
            print("3. Edit 'shopify_default' connection")
            print('4. In Extra field, add: {"authorization_code": "YOUR_CODE", "api_version": "2024-01"}')
            print("5. Save and test your DAG")
            print('=' * 80 + '\n')
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"<h1>Error</h1><p>No authorization code received</p>")
            print("\n❌ No authorization code received\n")
    
    def log_message(self, format, *args):
        """Suppress default logging."""
        pass


def main():
    """Main function to run OAuth flow."""
    print("\n" + "=" * 80)
    print("SHOPIFY OAUTH AUTHORIZATION CODE HELPER")
    print("=" * 80 + "\n")
    
    # Get app credentials
    print("Enter your Shopify app credentials:")
    print("(Find these in Partners Dashboard > Your App > API credentials)\n")
    
    shop = input("Shop URL (e.g., my-store.myshopify.com): ").strip()
    client_id = input("Client ID: ").strip()
    
    # Get scopes
    print("\nEnter API scopes (comma-separated):")
    print("Example: read_products,read_orders,read_customers")
    scopes = input("Scopes: ").strip()
    
    if not shop or not client_id or not scopes:
        print("\n❌ Error: All fields are required\n")
        sys.exit(1)
    
    # Ensure shop URL is in correct format
    if shop.startswith('http'):
        shop = urlparse(shop).netloc
    if not shop.endswith('.myshopify.com'):
        print(f"\n❌ Error: Shop URL should end with .myshopify.com (got: {shop})\n")
        sys.exit(1)
    
    # Callback URL (local server)
    redirect_uri = "http://localhost:8000/auth/callback"
    state = "dataforge_oauth_123"  # Random state for security
    
    # Build authorization URL
    auth_params = {
        'client_id': client_id,
        'scope': scopes,
        'redirect_uri': redirect_uri,
        'state': state,
    }
    
    auth_url = f"https://{shop}/admin/oauth/authorize?{urlencode(auth_params)}"
    
    print("\n" + "=" * 80)
    print("AUTHORIZATION URL")
    print("=" * 80)
    print(f"\n{auth_url}\n")
    print("=" * 80 + "\n")
    
    # Start local server
    print("Starting local OAuth callback server on http://localhost:8000 ...")
    server = HTTPServer(('localhost', 8000), OAuthCallbackHandler)
    
    print("\n" + "=" * 80)
    print("READY TO AUTHORIZE")
    print("=" * 80)
    print("\nOpening authorization URL in your browser...")
    print("Please authorize the app in the browser window that opens.")
    print("\nWaiting for OAuth callback...")
    print("(Press Ctrl+C to cancel)")
    print("=" * 80 + "\n")
    
    # Open browser
    try:
        webbrowser.open(auth_url)
    except Exception as e:
        print(f"⚠ Could not open browser: {e}")
        print(f"Please manually open this URL in your browser:\n{auth_url}\n")
    
    # Wait for callback
    try:
        server.handle_request()
        
        if OAuthCallbackHandler.authorization_code:
            return 0
        else:
            print("\n❌ Failed to capture authorization code\n")
            return 1
            
    except KeyboardInterrupt:
        print("\n\n❌ Cancelled by user\n")
        return 1
    finally:
        server.server_close()


if __name__ == "__main__":
    sys.exit(main())
