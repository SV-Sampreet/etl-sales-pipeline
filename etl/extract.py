import pandas as pd
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    'order_id',
    'customer_id',
    'order_date',
    'product',
    'category',
    'quantity',
    'unit_price',
    'region'
]


class ExtractionError(Exception):
    """Custom exception for extraction errors."""
    pass


def extract_csv(file_path: str) -> pd.DataFrame:
    """
    Extract data from a CSV file.

    Args:
        file_path: Path to the CSV file

    Returns:
        DataFrame containing the extracted data

    Raises:
        ExtractionError: If file is missing, malformed, or missing required columns
    """
    path = Path(file_path)

    if not path.exists():
        raise ExtractionError(f"File not found: {file_path}")

    if not path.is_file():
        raise ExtractionError(f"Path is not a file: {file_path}")

    if path.suffix.lower() != '.csv':
        raise ExtractionError(f"File is not a CSV: {file_path}")

    try:
        df = pd.read_csv(file_path)
    except pd.errors.EmptyDataError:
        raise ExtractionError(f"CSV file is empty: {file_path}")
    except pd.errors.ParserError as e:
        raise ExtractionError(f"CSV parsing error: {e}")
    except Exception as e:
        raise ExtractionError(f"Unexpected error reading CSV: {e}")

    if df.empty:
        raise ExtractionError(f"CSV file contains no data: {file_path}")

    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        raise ExtractionError(
            f"Missing required columns: {missing_columns}. "
            f"Found columns: {list(df.columns)}"
        )

    logger.info(f"Successfully extracted {len(df)} rows from {file_path}")
    logger.info(f"Columns: {list(df.columns)}")

    return df


def extract_raw_data(input_path: str) -> pd.DataFrame:
    """
    Main extraction function with logging.

    Args:
        input_path: Path to the raw data file

    Returns:
        DataFrame containing raw data
    """
    logger.info(f"Starting extraction from: {input_path}")

    try:
        df = extract_csv(input_path)
        logger.info(f"Extraction completed successfully. Total rows: {len(df)}")
        return df
    except ExtractionError as e:
        logger.error(f"Extraction failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected extraction error: {e}")
        raise ExtractionError(f"Unexpected error during extraction: {e}")
