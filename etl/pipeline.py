import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

from etl.extract import extract_raw_data, ExtractionError
from etl.transform import transform_data, save_clean_data, save_rejected_data, TransformationError
from etl.load import load_data_from_file, LoadError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class PipelineResult:
    """Container for pipeline execution results."""

    def __init__(self):
        self.status = 'pending'
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.metrics: Dict[str, Any] = {}
        self.error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            'status': self.status,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': (self.end_time - self.start_time).total_seconds()
                                if self.start_time and self.end_time else None,
            'metrics': self.metrics,
            'error': self.error
        }


def run_pipeline(
    input_path: str,
    clean_output_path: str,
    rejected_output_path: str,
    db_path: str,
    schema_path: str
) -> PipelineResult:
    """
    Run the complete ETL pipeline.

    Args:
        input_path: Path to raw input CSV
        clean_output_path: Path to save clean data
        rejected_output_path: Path to save rejected data
        db_path: Path to SQLite database
        schema_path: Path to schema.sql

    Returns:
        PipelineResult with execution metrics
    """
    result = PipelineResult()
    result.start_time = datetime.now()

    logger.info("=" * 60)
    logger.info("Starting ETL Pipeline")
    logger.info("=" * 60)

    try:
        # Ensure directories exist
        Path(clean_output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(rejected_output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        Path('logs').mkdir(parents=True, exist_ok=True)

        # Stage 1: Extract
        logger.info("Stage 1: Extraction")
        try:
            df_raw = extract_raw_data(input_path)
            result.metrics['extraction_rows'] = len(df_raw)
        except ExtractionError as e:
            result.status = 'failed'
            result.error = f"Extraction failed: {e}"
            logger.error(result.error)
            return result

        # Stage 2: Transform
        logger.info("Stage 2: Transformation")
        try:
            df_clean, df_rejected, transform_metrics = transform_data(df_raw)
            result.metrics.update(transform_metrics)

            # Save transformed data
            save_clean_data(df_clean, clean_output_path)
            save_rejected_data(df_rejected, rejected_output_path)
        except TransformationError as e:
            result.status = 'failed'
            result.error = f"Transformation failed: {e}"
            logger.error(result.error)
            return result

        # Stage 3: Load
        logger.info("Stage 3: Loading")
        try:
            load_metrics = load_data_from_file(
                df_clean,
                db_path,
                schema_path
            )
            result.metrics['load'] = load_metrics
        except LoadError as e:
            result.status = 'failed'
            result.error = f"Loading failed: {e}"
            logger.error(result.error)
            return result

        # Pipeline completed successfully
        result.status = 'success'
        result.end_time = datetime.now()

        logger.info("=" * 60)
        logger.info("Pipeline completed successfully")
        logger.info("=" * 60)
        logger.info(f"Input records: {result.metrics['input_count']}")
        logger.info(f"Accepted records: {result.metrics['accepted_count']}")
        logger.info(f"Rejected records: {result.metrics['rejected_count']}")
        logger.info(f"Duplicate records: {result.metrics['duplicate_count']}")
        logger.info(f"Records inserted: {result.metrics['load']['inserted']}")
        logger.info(f"Records updated: {result.metrics['load']['updated']}")
        logger.info(f"Load errors: {result.metrics['load']['errors']}")
        logger.info(f"Duration: {result.to_dict()['duration_seconds']:.2f} seconds")

        for reason, count in result.metrics['rejection_reasons'].items():
            logger.info(f"  - {reason}: {count}")

        return result

    except Exception as e:
        result.status = 'failed'
        result.error = f"Unexpected pipeline error: {e}"
        logger.error(result.error, exc_info=True)
        result.end_time = datetime.now()
        return result


def main():
    """Main entry point for CLI execution."""
    # Default paths (can be overridden with environment variables or config)
    base_dir = Path(__file__).parent.parent

    input_path = str(base_dir / 'data' / 'raw' / 'sales_data.csv')
    clean_output_path = str(base_dir / 'data' / 'processed' / 'sales_clean.csv')
    rejected_output_path = str(base_dir / 'data' / 'rejected' / 'sales_rejected.csv')
    db_path = str(base_dir / 'data' / 'sales.db')
    schema_path = str(base_dir / 'sql' / 'schema.sql')

    result = run_pipeline(
        input_path=input_path,
        clean_output_path=clean_output_path,
        rejected_output_path=rejected_output_path,
        db_path=db_path,
        schema_path=schema_path
    )

    # Exit with appropriate code
    if result.status == 'success':
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
