from src.helpers.mongo_helpers import (
    upsert_data,
    fetch_data,
    check_collection_exists,
    insert_data,
)

import json
from typing import List
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


def read_data_quality_rules():
    with open("dq_rules.json", "r") as f:
        rules = json.load(f)
    return rules


def store_data_quality_rules(
    rules: List[dict], db_name: str = "metadata", collection_name: str = "dq_rules"
):
    try:
        if check_collection_exists(db_name, collection_name):
            existing_rules = fetch_data(db_name, collection_name, {})
            existing_rule_keys = {rule["rule_key"] for rule in existing_rules}

            updated_rules = [
                rule
                for existing_rule, rule in zip(existing_rules, rules)
                if existing_rule["rule_key"] == rule["rule_key"]
                and datetime.strptime(
                    existing_rule["update_timestamp"], "%Y-%m-%d %H:%M:%S"
                )
                < datetime.strptime(rule["update_timestamp"], "%Y-%m-%d %H:%M:%S")
            ]
            new_rules = [
                rule for rule in rules if rule["rule_key"] not in existing_rule_keys
            ]

            if updated_rules:
                logger.info(
                    f"Updating {len(updated_rules)} rules in collection '{collection_name}'."
                )
                upsert_data(db_name, collection_name, updated_rules)
            if new_rules:
                logger.info(
                    f"Inserting {len(new_rules)} new rules into collection '{collection_name}'."
                )
                insert_data(db_name, collection_name, new_rules)
        else:
            insert_data(db_name, collection_name, rules)
            logger.info(
                f"Inserted {len(rules)} new rules into collection '{collection_name}'."
            )
    except Exception as e:
        logger.error(f"Error storing data quality rules: {e}")
