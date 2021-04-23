"""
Main module of the daily-insta-job project.

"""

import logging

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


if __name__ == "__main__":
    logger.info("STARTING JOB")
    ig_user_id = 17841411237972805
    base = "https://graph.facebook.com/v9.0"
    user_node = f"/{ig_user_id}"
    access_token = "EAAGKcMwM3hYBAHB0UcEVAtFXKsKGNubDi7bUpNGvrhCoG8fElwZBELjjggaWAWqq3TzkYUG4rxSe0520u81Lp5XZAHAQ6uR1SBnEBhm85wAd8vaS8gg0580q2NxjN1VbRcm6aFcA1zZAvvAR6VHlamoebSOd3X3HY4v8zyLZBXmyt2ud9WjG0mOftnpZBS5gZD"
    username = "vinicius.py"
    logger.info(f"GETTING DATA FROM INSTA USERNAME: {username}")
    data_lake_bucket_name = "instagram-vags-datalake-dev"
    config = {}
    date_str = "2021-01-01"

    logger.info(f"REQUESTING DAILY METRICS ...")
    day_account_metrics = get_day_account_metrics(base, user_node, access_token)
    logger.info(day_account_metrics)
    day_account_metrics_df = day_account_metrics
    day_account_metrics_bucket_name = f"{data_lake_bucket_name}/DayAccountMetrics"
    write_to_s3(
        day_account_metrics_df,
        format_folder_path(day_account_metrics_bucket_name, date_str, logger),
        config,
        logger,
    )

    logger.info(f"REQUESTING STORIES METRICS ...")
    stories_ids = get_stories_id(base, user_node, access_token)
    logger.info(stories_ids)
    stories_id_list = [storie_id for storie_id in stories_ids["data"]]
    stories_metrics = get_stories_metrics(base, stories_id_list, access_token)
    logger.info(stories_metrics)

    logger.info("END OF JOB")
