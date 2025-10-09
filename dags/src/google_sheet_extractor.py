import logging 

import gspread
from airflow.hooks.base import BaseHook
from airflow.decorators import task

logger = logging.getLogger("google_sheet_logger")
def generate_google_sheet_extractor(**kwargs):
    @task()
    def load_google_sheet():
        spreadsheet_name = kwargs["spreadsheet_name"]
        spreadsheet_folder = kwargs.get("spreadsheet_folder", None)
        worksheet_name = kwargs["worksheet_name"]
        service_account_credentials = BaseHook.get_connection("gsheet")
        gc = gspread.service_account_from_dict(service_account_credentials.extra_dejson)
        spreadsheet = gc.open(spreadsheet_name, spreadsheet_folder)
        worksheet = spreadsheet.worksheet(worksheet_name)
        all_records = worksheet.get_all_records()
        logger.info("successfully loaded %s records", len(all_records))
        logger.info("sample data %s", all_records[0])

    return load_google_sheet
