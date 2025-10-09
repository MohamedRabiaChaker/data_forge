import os
import json
import logging

from src.helpers import consolidate_configuration
from src.google_sheet_extractor import generate_google_sheet_extractor

from airflow import settings
from airflow.decorators import dag
logger = logging.getLogger("generator")

current_directory = os.getcwd()
config_files = [file for file in os.listdir(settings.DAGS_FOLDER) if ".json" in file]
pipelines_config = []
for file in config_files:
    logger.info("Processing file : %s", file)
    with open(os.path.join(settings.DAGS_FOLDER, file), "r") as f:
        config = json.load(f)
        source_config = [consolidate_configuration(config["instances"][key],config["defaults"]) for key in config["instances"]]
        pipelines_config = pipelines_config + source_config


def generate_pipeline_dag(etl_config):
    @dag()
    def etl_pipeline():
        extract_config = etl_config["extract"]
        transform_config = etl_config.get("transform", None)
        load = etl_config["load"]

        extract_task = globals()[f"generate_{extract_config['type']}_extractor"](**extract_config["parameters"])
        # if transform_config is not None: 
        #     transform_task = globals()[f"generate_{transform_config['type']}_extractor"](**extract_config)
        # load_task = globals()[f"generate_{load_config['type']}_extractor"](**extract_config)

        extract_task()
    return etl_pipeline()

for pipeline in pipelines_config: 
    generate_pipeline_dag(pipeline)
