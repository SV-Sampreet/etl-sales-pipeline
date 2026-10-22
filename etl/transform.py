import pandas as pd
import logging
from datetime import datetime
from typing import Tuple, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class TransformationError(Exception):
    """Custom exception for transformation errors."""
    pass


class DataQualityValidator:
    """Validates data quality rules."""

    @staticmethod
    def is_valid_date(date_str: str) -> bool:
        """Check if date string is valid and parsable."""
        if pd.isna(date_str) or date_str == '':
            return False
        try:
            pd.to_datetime(date_str, format='%Y-%m-%d', errors='raise')
            return True
        except (ValueError, TypeError):
            return False

    @staticmethod
    def is_valid_quantity(quantity: Any) -> bool:
        """Check if quantity is a positive integer."""
        if pd.isna(quantity):
            return False
        try:
            qty = float(quantity)
            return qty > 0 and qty == int(qty)
        except (ValueError, TypeError):
            return False

    @staticmethod
    def is_valid_price(price: Any) -> bool:
        """Check if price is a non-negative number."""
        if pd.isna(price):
            return False
        try:
            return float(price) >= 0
        except (ValueError, TypeError):
            return False

    @staticmethod
    def has_required_fields(row: pd.Series) -> bool:
        """Check if all required fields are present and non-empty."""
        required = ['order_id', 'customer_id', 'order_date', 'product',
                    'category', 'quantity', 'unit_price', 'region']
        for field in required:
            if pd.isna(row[field]) or str(row[field]).strip() == '':
                return False
        return True


def normalize_text(value: Any) -> str:
    """Normalize text fields: strip whitespace, title case."""
    if pd.isna(value):
        return ''
    return str(value).strip().title()


def transform_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Transform and validate sales data.

    Args:
        df: Raw input DataFrame

    Returns:
        Tuple of (clean_df, rejected_df, metrics)

    Raises:
        TransformationError: If transformation fails
    """
    logger.info(f"Starting transformation. Input rows: {len(df)}")

    metrics = {
        'input_count': len(df),
        'accepted_count': 0,
        'rejected_count': 0,
        'duplicate_count': 0,
        'rejection_reasons': {}
    }

    if df.empty:
        logger.warning("Empty DataFrame provided")
        return pd.DataFrame(), pd.DataFrame(), metrics

    # Work on a copy to avoid modifying original
    df_transformed = df.copy()

    # Normalize text fields
    text_columns = ['product', 'category', 'region']
    for col in text_columns:
        df_transformed[col] = df_transformed[col].apply(normalize_text)

    # Parse dates
    df_transformed['order_date'] = pd.to_datetime(
        df_transformed['order_date'],
        format='%Y-%m-%d',
        errors='coerce'
    )

    # Convert numeric fields
    df_transformed['quantity'] = pd.to_numeric(df_transformed['quantity'], errors='coerce')
    df_transformed['unit_price'] = pd.to_numeric(df_transformed['unit_price'], errors='coerce')

    # Validate each row
    rejected_records = []
    clean_records = []

    validator = DataQualityValidator()

    for idx, row in df_transformed.iterrows():
        rejection_reasons = []

        # Check required fields
        if not validator.has_required_fields(row):
            rejection_reasons.append("Missing required field")

        # Check date validity
        if not validator.is_valid_date(row['order_date']):
            rejection_reasons.append("Invalid date")

        # Check quantity validity
        if not validator.is_valid_quantity(row['quantity']):
            rejection_reasons.append("Invalid quantity")

        # Check price validity
        if not validator.is_valid_price(row['unit_price']):
            rejection_reasons.append("Invalid price")

        if rejection_reasons:
            # Add to rejected records with original data and reasons
            rejected_row = df.iloc[idx].copy()
            rejected_row['rejection_reason'] = ', '.join(rejection_reasons)
            rejected_records.append(rejected_row)

            # Track rejection reasons
            for reason in rejection_reasons:
                metrics['rejection_reasons'][reason] = metrics['rejection_reasons'].get(reason, 0) + 1
        else:
            clean_records.append(row)

    clean_df = pd.DataFrame(clean_records)

    # Detect duplicates using order_id as business key
    if not clean_df.empty:
        duplicates = clean_df[clean_df.duplicated(subset=['order_id'], keep='first')]
        metrics['duplicate_count'] = len(duplicates)

        # Keep only first occurrence of each order_id
        clean_df = clean_df.drop_duplicates(subset=['order_id'], keep='first')

        # Add duplicates to rejected records
        for idx, row in duplicates.iterrows():
            orig_idx = df_transformed.index.get_loc(idx)
            rejected_row = df.iloc[orig_idx].copy()
            rejected_row['rejection_reason'] = 'Duplicate order_id'
            rejected_records.append(rejected_row)

    # Calculate revenue for clean records
    if not clean_df.empty:
        clean_df['revenue'] = clean_df['quantity'] * clean_df['unit_price']
    else:
        clean_df['revenue'] = []

    rejected_df = pd.DataFrame(rejected_records)

    metrics['accepted_count'] = len(clean_df)
    metrics['rejected_count'] = len(rejected_df)

    logger.info(f"Transformation completed. "
                f"Accepted: {metrics['accepted_count']}, "
                f"Rejected: {metrics['rejected_count']}, "
                f"Duplicates: {metrics['duplicate_count']}")

    for reason, count in metrics['rejection_reasons'].items():
        logger.info(f"Rejection reason '{reason}': {count}")

    return clean_df, rejected_df, metrics


def save_clean_data(df: pd.DataFrame, output_path: str) -> None:
    """
    Save clean data to CSV.

    Args:
        df: Clean DataFrame
        output_path: Path to save the file
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"Clean data saved to {output_path}")


def save_rejected_data(df: pd.DataFrame, output_path: str) -> None:
    """
    Save rejected data to CSV.

    Args:
        df: Rejected DataFrame
        output_path: Path to save the file
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"Rejected data saved to {output_path}")
