from src.helpers.crawling_helpers import fetch_cycling_files
from src.helpers.mongo_helpers import (
    upsert_data,
    fetch_data,
    check_collection_exists,
    insert_data,
)
from src.config.constants import CyclingDataAPI

import re
from io import StringIO
from typing import List
from datetime import datetime
import requests
import csv
import json
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


def parse_date(date_str):
    date_str = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", date_str)
    return datetime.strptime(date_str, "%b %d %Y, %I:%M:%S %p")


def store_cycling_files(
    db_name: str = "metadata",
    collection_name: str = "files_defination",
    url: str = CyclingDataAPI.URL,
    driver_path: str = CyclingDataAPI.DRIVER_PATH,
):
    try:
        logger.info("Fetching cycling files...")
        results = fetch_cycling_files(url, driver_path)

        if check_collection_exists(db_name, collection_name):
            # Lấy danh sách các file hiện có
            existing_files = fetch_data(db_name, collection_name, {})
            existing_file_names = {file["name"] for file in existing_files}

            # Phân loại file cần cập nhật và file mới
            updated_files = [
                {
                    **result,
                    "create_timestamp": existing_file["create_timestamp"],
                    "update_timestamp": result.pop("last_modified"),
                }
                for existing_file, result in zip(existing_files, results)
                if existing_file["name"] == result["name"]
                and parse_date(existing_file["update_timestamp"]) < parse_date(result["last_modified"])
            ]

            new_files = [
                {
                    **result,
                    "create_timestamp": result["last_modified"],
                    "update_timestamp": result["last_modified"],
                }
                for result in results
                if result["name"] not in existing_file_names
            ]

            # Thực hiện cập nhật và thêm mới
            if updated_files:
                logger.info(
                    f"Updating {len(updated_files)} files in collection '{collection_name}'."
                )
                upsert_data(db_name, collection_name, updated_files)

            if new_files:
                logger.info(
                    f"Inserting {len(new_files)} new files into collection '{collection_name}'."
                )
                insert_data(db_name, collection_name, new_files)

        else:
            # Nếu collection chưa tồn tại, chèn tất cả các file
            for result in results:
                result["create_timestamp"] = result.pop("last_modified")
                result["update_timestamp"] = result["create_timestamp"]
            insert_data(db_name, collection_name, results)
            logger.info(
                f"Inserted {len(results)} new files into collection '{collection_name}'."
            )
    except Exception as e:
        logger.error(f"Error storing cycling files: {e}")


def fetch_files_data_by_place(place: str):
    logger.info(f"Fetching files data for place: {place}")

    info_files = fetch_data("metadata", "files", {})
    target_files = [
        file["name"]
        for file in info_files
        if place.lower() in file["name"].lower()
        and parse_date(file["update_timestamp"]) > parse_date(file["create_timestamp"])
    ]
    updated_info_files = [
        {**file, "create_timestamp": file["update_timestamp"]}
        for file in info_files
        if place.lower() in file["name"].lower()
        and parse_date(file["update_timestamp"]) > parse_date(file["create_timestamp"])
    ]

    upsert_data("metadata", "files_defination", updated_info_files)

    data = []
    for file_name in target_files:
        download_url = f"{CyclingDataAPI.ROOT_URL}/{file_name}"
        logger.info(f"Downloading file: {file_name}")

        try:
            response = requests.get(download_url)
            response.raise_for_status()
            csv_reader = csv.DictReader(StringIO(response.text))
            data.extend(list(csv_reader))
            logger.info(f"Successfully downloaded and processed file: {file_name}")
        except requests.RequestException as e:
            logger.error(f"Error downloading file {file_name}: {e}")

    logger.info(f"Fetched data for {len(target_files)} files.")
    return data
