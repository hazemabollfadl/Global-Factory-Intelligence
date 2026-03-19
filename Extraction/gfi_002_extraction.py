"""
Asynchronous, batch-optimized pipeline for extracting structured JSON
from maintenance logs using the Gemini API.
"""

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from google import genai
from google.genai import types
from pydantic import BaseModel, Field
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

# Constants
MODEL_NAME = "gemini-2.5-flash"
RAW_FILE_PATH = Path("Data/Raw/gfi_maintenance_logs.json")
PROCESSED_FILE_PATH = Path("Data/Processed/gfi_structured_logs.json")
BATCH_SIZE = 5  # 20 logs / 5 = 4 requests (Under the 5 RPM Free Tier Limit)

SYSTEM_PROMPT = """
You are an expert industrial AI extraction system.
You will receive a JSON array containing multiple factory maintenance logs.
For EACH log in the array:
1. Identify the original language.
2. Translate the entire log into clear, professional English.
3. Summarize the core equipment issue in exactly 3 to 5 words.
4. Assess the urgency (High, Medium, Low).
You must return a JSON array that strictly adheres to the requested schema.
"""

# --- Data Models ---


class MaintenanceExtraction(BaseModel):
    """Schema for a single log."""
    ticket_id: str = Field(
        description="The original ticket ID passed from the input.")
    original_language: str = Field(
        description="The language of the raw log (e.g., English, German, Arabic).")
    translated_log: str = Field(
        description="The full English translation of the maintenance event.")
    equipment_issue: str = Field(
        description="A concise 3-5 word summary of the mechanical problem.")
    urgency_level: str = Field(
        description="Restricted to: 'Low', 'Medium', or 'High'.")


class ExtractionBatchResponse(BaseModel):
    """Schema for a batched response (Required for Semantic Batching)."""
    results: list[MaintenanceExtraction]


# --- Core Logic ---

def _create_client() -> genai.Client:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key or not api_key.strip():
        raise ValueError(
            "GEMINI_API_KEY environment variable is not set or empty")
    return genai.Client(api_key=api_key)


def chunk_data(data: list, chunk_size: int) -> list[list]:
    """Yield successive chunks from a list."""
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]


@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type((ConnectionError, TimeoutError, Exception)),
    before_sleep=lambda rs: logger.warning(
        'Retry attempt no: %s, Due to: %s',
        rs.attempt_number,
        rs.outcome.exception() if rs.outcome else 'Unknown'
    )
)
async def process_batch_async(client: genai.Client, batch: list[dict[str, Any]], batch_id: int) -> list[dict[str, Any]]:
    """
    Sends a chunk of logs to Gemini asynchronously.
    Note: We use client.aio for asynchronous requests in the new SDK.
    """
    logger.info(
        "Firing API request for Batch %d (Contains %d logs)...", batch_id, len(batch))

    # Pass the entire batch as a JSON string in the prompt
    prompt = f"Process the following array of logs:\n{json.dumps(batch, ensure_ascii=False)}"

    response = await client.aio.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            response_mime_type="application/json",
            response_schema=ExtractionBatchResponse,
            temperature=0.1
        ),
    )

    if not response.text:
        logger.error("Model returned empty response for Batch %d", batch_id)
        return []

    try:
        data = json.loads(response.text)
        return data.get("results", [])
    except json.JSONDecodeError:
        logger.exception(
            "Invalid JSON response from API for Batch %d", batch_id)
        raise

# --- Main Execution ---


async def run_pipeline():
    load_dotenv(override=True)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    for name in ("google_genai", "google_genai.models", "httpx"):
        logging.getLogger(name).setLevel(logging.WARNING)

    client = _create_client()

    if not RAW_FILE_PATH.exists():
        logger.error("Input file not found at %s.", RAW_FILE_PATH)
        return

    with open(RAW_FILE_PATH, "r", encoding="utf-8") as f:
        maintenance_logs = json.load(f)

    # 1. Chunk the data
    batches = chunk_data(maintenance_logs, BATCH_SIZE)
    logger.info("Split %d logs into %d batches.",
                len(maintenance_logs), len(batches))

    batch_results = []
    logger.info(
        "Processing batches sequentially to respect Free Tier burst limits...")
    for i, batch in enumerate(batches):
        result = await process_batch_async(client, batch, i + 1)
        if result:
            batch_results.append(result)

        # Add a 2-second stagger between requests (except after the last one)
        if i < len(batches) - 1:
            await asyncio.sleep(2)

    # 4. Flatten the results (list of lists -> single list)
    final_structured_results = [
        log for batch in batch_results for log in batch]

    # 5. Save output
    if final_structured_results:
        PROCESSED_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
        PROCESSED_FILE_PATH.write_text(
            json.dumps(final_structured_results, ensure_ascii=False, indent=4),
            encoding="utf-8"
        )
        logger.info("Success! Saved %d structured logs to %s", len(
            final_structured_results), PROCESSED_FILE_PATH)
    else:
        logger.error("No results to save.")

if __name__ == "__main__":
    asyncio.run(run_pipeline())
