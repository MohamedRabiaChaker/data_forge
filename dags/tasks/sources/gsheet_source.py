"""Google Sheets source implementation."""

from typing import Any, Dict, List

from airflow.providers.google.suite.hooks.sheets import GSheetsHook

try:
    from .base_source import BaseSource
except ImportError:
    from base_source import BaseSource


class GoogleSheetsSource(BaseSource):
    """
    Extract data from Google Sheets using Airflow's Google provider.

    Configuration:
        gcp_conn_id (str): Airflow connection ID for Google Cloud (default: 'google_cloud_default')
        spreadsheet_id (str): The ID of the Google Spreadsheet
        range_name (str): The A1 notation of the range to retrieve (e.g., 'Sheet1!A1:Z1000')

    Example config:
        {
            "type": "gsheet",
            "gcp_conn_id": "google_cloud_default",
            "spreadsheet_id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
            "range_name": "Sheet1!A1:Z1000"
        }
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize Google Sheets source with configuration."""
        super().__init__(config)
        self.validate_config()

        # Get configuration with defaults
        self.gcp_conn_id = config.get("gcp_conn_id", "google_cloud_default")
        self.spreadsheet_id = config["spreadsheet_id"]
        self.range_name = config["range_name"]

    def validate_config(self) -> None:
        """
        Validate required configuration parameters.

        Raises:
            ValueError: If required parameters are missing
        """
        required_fields = ["spreadsheet_id", "range_name"]
        missing_fields = [
            field for field in required_fields if not self.config.get(field)
        ]

        if missing_fields:
            raise ValueError(
                f"Missing required configuration fields: {', '.join(missing_fields)}"
            )

    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract data from Google Sheets.

        Returns:
            List[Dict[str, Any]]: List of dictionaries where keys are column headers
                                  from the first row and values are cell values.

        Raises:
            Exception: If Google Sheets API call fails or data format is invalid

        Example:
            Given a sheet with:
                Row 1: ['Name', 'Email', 'Age']
                Row 2: ['John Doe', 'john@example.com', '30']
                Row 3: ['Jane Smith', 'jane@example.com', '25']

            Returns:
                [
                    {'Name': 'John Doe', 'Email': 'john@example.com', 'Age': '30'},
                    {'Name': 'Jane Smith', 'Email': 'jane@example.com', 'Age': '25'}
                ]
        """
        # Initialize Google Sheets hook
        hook = GSheetsHook(gcp_conn_id=self.gcp_conn_id)

        # Get values from the sheet
        values = hook.get_values(
            spreadsheet_id=self.spreadsheet_id, range_=self.range_name
        )

        # Handle empty sheet
        if not values or len(values) == 0:
            return []

        # Extract headers from first row
        headers = values[0]

        # Handle sheet with only headers
        if len(values) == 1:
            return []

        # Convert rows to list of dictionaries
        result = []
        for row in values[1:]:  # Skip header row
            # Handle rows with fewer columns than headers
            # Pad with empty strings if needed
            padded_row = row + [""] * (len(headers) - len(row))

            # Create dictionary for this row
            row_dict = {headers[i]: padded_row[i] for i in range(len(headers))}
            result.append(row_dict)

        return result
