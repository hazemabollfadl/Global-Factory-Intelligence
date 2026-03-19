"""
Loads structured JSON maintenance logs into a Google BigQuery table.
Handles automatic Dataset and Table creation with strict schema enforcement.
"""

import json
import logging
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)

# Constants
PROCESSED_FILE_PATH = Path("Data/Processed/gfi_structured_logs.json")
DATASET_NAME = "gfi_maintenance"
TABLE_NAME = "structured_logs"
REGION = "europe-west3"


def load_data_to_bigquery() -> None:
    """Reads local JSON data and appends it to BigQuery."""

    # Initialize the BigQuery client
    try:
        client = bigquery.Client()
        project_id = client.project
        logger.info("Successfully authenticated to GCP Project: %s", project_id)
    except Exception as e:
        logger.error(
            "Failed to initialize BigQuery client. Check your credentials.")
        raise SystemExit(1) from e

    # 1. Define Dataset and Table references
    dataset_id = f"{project_id}.{DATASET_NAME}"
    table_id = f"{dataset_id}.{TABLE_NAME}"

    # 2. Create Dataset if it doesn't exist
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = REGION
    try:
        client.get_dataset(dataset_id)
        logger.info("Dataset %s already exists.", dataset_id)
    except NotFound:
        dataset = client.create_dataset(dataset, timeout=30)
        logger.info("Created new dataset %s in region %s.", dataset_id, REGION)

    # 3. Define the strict BigQuery Schema (Matches our Pydantic model)
    schema = [
        bigquery.SchemaField("ticket_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("original_language", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("translated_log", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("equipment_issue", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("urgency_level", "STRING", mode="REQUIRED"),
    ]

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    # 4. Read the processed JSON file
    if not PROCESSED_FILE_PATH.exists():
        logger.error(
            "File not found: %s. Please run the Extraction script first.", PROCESSED_FILE_PATH)
        raise SystemExit(1)

    with open(PROCESSED_FILE_PATH, "r", encoding="utf-8") as f:
        try:
            records = json.load(f)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse JSON file: %s", e)
            raise SystemExit(1) from e

    if not records:
        logger.warning("No records found in the JSON file to load.")
        return

    # 5. Execute the Load Job
    logger.info("Initiating BigQuery load job for %d records...", len(records))

    try:
        # load_table_from_json natively accepts a list of Python dictionaries
        load_job = client.load_table_from_json(
            records,
            table_id,
            job_config=job_config
        )

        # Wait for the job to complete
        load_job.result()

        # 6. Verification
        destination_table = client.get_table(table_id)
        logger.info(
            "Success! Appended %d rows. Table %s now contains %d total rows.",
            len(records),
            TABLE_NAME,
            destination_table.num_rows
        )

    except Exception as e:
        logger.error("BigQuery load job failed: %s", e)
        if hasattr(load_job, 'errors') and load_job.errors:
            for error in load_job.errors:
                logger.error(error)
        raise SystemExit(1) from e


def main() -> None:
    load_dotenv(override=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Suppress noisy HTTP request logs from Google libraries
    logging.getLogger("google.auth").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    load_data_to_bigquery()


if __name__ == "__main__":
    main()
