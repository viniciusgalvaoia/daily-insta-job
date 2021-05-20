"""
Main module of the daily-insta-job project.

"""
import configparser
import logging
import os
from datetime import datetime
from typing import Dict

import pandas as pd
from ig_helpers import (
    format_folder_path,
    get_day_account_metrics,
    get_stories_id,
    get_stories_metrics,
    write_to_s3,
)
from rich.logging import RichHandler
from rich.traceback import install

FORMAT = "[%(levelname)s] [%(message)s]"
logging.basicConfig(
    level=logging.INFO,
    format=FORMAT,
    datefmt="[%Y-%m-%d %H:%M:%S]",
    handlers=[RichHandler(show_level=False, show_path=False)],
)
install()
logger = logging.getLogger("main")


def process_values_field(values_field_list):
    if len(values_field_list) == 1:
        value = values_field_list[0]["value"]
    else:
        value = 0
        for value_field in values_field_list:
            value = value + value_field["value"]
    return value


def process_daily_account_metrics(daily_metrics: Dict[str, str]) -> pd.DataFrame:
    daily_metrics_list = []
    for daily_metric in daily_metrics["data"]:
        daily_metric_dict = {
            key: value
            for key, value in daily_metric.items()
            if key in ["name", "description", "values"]
        }
        daily_metric_dict["values"] = process_values_field(daily_metric_dict["values"])
        daily_metrics_list.append(daily_metric_dict)
    return pd.DataFrame(daily_metrics_list)


def process_stories_metrics(stories_metrics_dict):
    return True


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.realpath(".."), "setup.cfg"))

    logger.info("STARTING JOB")
    ig_user_id = config["GRAPH_API"]["IG_USER_ID"]
    base = config["GRAPH_API"]["BASE"]
    username = config["GRAPH_API"]["USERNAME"]
    access_token = config["GRAPH_API"]["ACCESS_TOKEN"]
    data_lake_bucket_name = config["S3"]["DATALAKE_BUCKET_NAME"]
    user_node = f"/{ig_user_id}"

    logger.info(f"GETTING DATA FROM INSTA: @{username}")
    date_str = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"REQUESTING DAILY METRICS ...")
    daily_account_metrics = get_day_account_metrics(base, user_node, access_token)
    daily_account_metrics_df = process_daily_account_metrics(daily_account_metrics)
    daily_account_metrics_bucket_name = f"{data_lake_bucket_name}/DailyAccountMetrics"
    write_to_s3(
        daily_account_metrics_df,
        format_folder_path(daily_account_metrics_bucket_name, date_str, logger),
        config,
        logger,
    )

    logger.info(f"REQUESTING STORIES METRICS ...")
    stories_ids = get_stories_id(base, user_node, access_token)
    stories_id_list = [storie_id for storie_id in stories_ids["data"]]
    stories_metrics = get_stories_metrics(base, stories_id_list, access_token)
    stores_metrics_df = process_stories_metrics(stories_metrics)
    stories_metrics_bucket_name = f"{data_lake_bucket_name}/StoriesAccountMetrics"
    write_to_s3(
        stores_metrics_df,
        format_folder_path(stories_metrics_bucket_name, date_str, logger),
        config,
        logger,
    )

    logger.info("END OF JOB")
