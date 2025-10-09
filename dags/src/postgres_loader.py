import logging 

from airflow.decorators import task 
from airflow.hooks.base import BaseHook


postgres_connection = BaseHook.get_connection("prod_postgres")

def generate_postgres_load_task():
    @task
    def load_into_postgres():
        # FIXME: detect column types 
        # create or replace landing table 
        # truncate landing table 
        # insert date into staging 
        # run load validation and fail the load if one of the hard rules is not accounted for
        # historize and insert into warehouse
        pass
    
