import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
from etl.pipeline import run_pipeline, PipelineResult
from etl.extract import ExtractionError
from etl.transform import TransformationError
from etl.load import LoadError


@pytest.fixture
def sample_input_csv():
    """Create a sample input CSV file for integration testing."""
    data = """order_id,customer_id,order_date,product,category,quantity,unit_price,region
ORD001,CUST001,2024-01-15,laptop,Electronics,2,1200.00,North
ORD002,CUST002,2024-01-16,MOUSE,Electronics,5,25.00,South
ORD003,CUST003,2024-01-17,Keyboard,Electronics,3,75.00,East
ORD004,CUST004,2024-01-18,Monitor,Electronics,1,350.00,West
ORD005,CUST005,2024-01-19,headphones,Electronics,4,150.00,North
ORD006,CUST006,2024-01-20,Desk Chair,Furniture,2,200.00,South
ORD007,CUST007,2024-01-21,Bookshelf,Furniture,3,150.00,East"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data)
        return f.name


@pytest.fixture
def input_with_issues():
    """Create input CSV with data quality issues."""
    data = """order_id,customer_id,order_date,product,category,quantity,unit_price,region
ORD001,CUST001,2024-01-15,Laptop,Electronics,2,1200.00,North
ORD002,CUST002,2024-02-30,Mouse,Electronics,5,25.00,South
ORD003,CUST003,2024-01-17,Keyboard,Electronics,abc,75.00,East
ORD004,CUST004,2024-01-18,Monitor,Electronics,0,350.00,West
ORD005,CUST005,2024-01-19,Headphones,Electronics,4,-50.00,North"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data)
        return f.name


@pytest.fixture
def schema_file():
    """Create a temporary schema file."""
    schema = """
    CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        order_date DATE NOT NULL,
        product TEXT NOT NULL,
        category TEXT NOT NULL,
        quantity INTEGER NOT NULL CHECK (quantity > 0),
        unit_price REAL NOT NULL CHECK (unit_price >= 0),
        region TEXT NOT NULL,
        revenue REAL NOT NULL CHECK (revenue >= 0),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
    CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);
    CREATE INDEX IF NOT EXISTS idx_orders_category ON orders(category);
    CREATE INDEX IF NOT EXISTS idx_orders_region ON orders(region);
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write(schema)
        return f.name


@pytest.fixture
def temp_output_paths():
    """Create temporary output paths."""
    with tempfile.TemporaryDirectory() as tmpdir:
        clean_path = os.path.join(tmpdir, 'clean.csv')
        rejected_path = os.path.join(tmpdir, 'rejected.csv')
        db_path = os.path.join(tmpdir, 'test.db')
        yield clean_path, rejected_path, db_path


class TestRunPipeline:
    """Integration tests for the complete pipeline."""

    def test_successful_pipeline_run(self, sample_input_csv, schema_file, temp_output_paths):
        """Test successful end-to-end pipeline execution."""
        clean_path, rejected_path, db_path = temp_output_paths

        result = run_pipeline(
            input_path=sample_input_csv,
            clean_output_path=clean_path,
            rejected_output_path=rejected_path,
            db_path=db_path,
            schema_path=schema_file
        )

        assert result.status == 'success'
        assert result.error is None
        assert result.start_time is not None
        assert result.end_time is not None
        assert result.to_dict()['duration_seconds'] > 0

        # Check metrics
        assert result.metrics['input_count'] == 7
        assert result.metrics['accepted_count'] == 7  # No duplicates in this test data
        assert result.metrics['rejected_count'] == 0
        assert result.metrics['duplicate_count'] == 0

        # Check output files exist
        assert os.path.exists(clean_path)
        assert os.path.exists(rejected_path)
        assert os.path.exists(db_path)

        # Verify clean data
        clean_df = pd.read_csv(clean_path)
        assert len(clean_df) == 7
        assert 'revenue' in clean_df.columns

        # Verify rejected data (may be empty)
        try:
            rejected_df = pd.read_csv(rejected_path)
            assert len(rejected_df) == 0  # No rejections in this test data
        except pd.errors.EmptyDataError:
            # Empty file is acceptable when no records are rejected
            pass

        # Verify database
        from sqlalchemy import create_engine, text
        engine = create_engine(f'sqlite:///{db_path}')
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM orders"))
            count = result.scalar()
            assert count == 7

        os.unlink(sample_input_csv)
        os.unlink(schema_file)

    def test_pipeline_with_data_quality_issues(self, input_with_issues, schema_file, temp_output_paths):
        """Test pipeline handling of data quality issues."""
        clean_path, rejected_path, db_path = temp_output_paths

        result = run_pipeline(
            input_path=input_with_issues,
            clean_output_path=clean_path,
            rejected_output_path=rejected_path,
            db_path=db_path,
            schema_path=schema_file
        )

        assert result.status == 'success'
        assert result.metrics['input_count'] == 5
        assert result.metrics['accepted_count'] == 1  # Only ORD001 is valid
        assert result.metrics['rejected_count'] == 4

        # Verify rejected reasons
        rejected_df = pd.read_csv(rejected_path)
        assert len(rejected_df) == 4
        # Check that rejection reasons contain the expected issues (they may be combined)
        all_reasons = ' '.join(rejected_df['rejection_reason'].values)
        assert 'Invalid date' in all_reasons
        assert 'Invalid quantity' in all_reasons
        assert 'Invalid price' in all_reasons

        os.unlink(input_with_issues)
        os.unlink(schema_file)

    def test_pipeline_idempotency(self, sample_input_csv, schema_file, temp_output_paths):
        """Test that pipeline is idempotent - second run doesn't create duplicates."""
        clean_path, rejected_path, db_path = temp_output_paths

        # First run
        result1 = run_pipeline(
            input_path=sample_input_csv,
            clean_output_path=clean_path,
            rejected_output_path=rejected_path,
            db_path=db_path,
            schema_path=schema_file
        )

        assert result1.status == 'success'
        assert result1.metrics['load']['inserted'] == 7

        # Second run with same data
        result2 = run_pipeline(
            input_path=sample_input_csv,
            clean_output_path=clean_path,
            rejected_output_path=rejected_path,
            db_path=db_path,
            schema_path=schema_file
        )

        assert result2.status == 'success'
        assert result2.metrics['load']['inserted'] == 0
        assert result2.metrics['load']['updated'] == 7

        # Verify database still has only 7 records (no duplicates)
        from sqlalchemy import create_engine, text
        engine = create_engine(f'sqlite:///{db_path}')
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM orders"))
            count = result.scalar()
            assert count == 7

        os.unlink(sample_input_csv)
        os.unlink(schema_file)

    def test_pipeline_extraction_failure(self, schema_file, temp_output_paths):
        """Test pipeline handling of extraction failure."""
        clean_path, rejected_path, db_path = temp_output_paths

        result = run_pipeline(
            input_path='nonexistent.csv',
            clean_output_path=clean_path,
            rejected_output_path=rejected_path,
            db_path=db_path,
            schema_path=schema_file
        )

        assert result.status == 'failed'
        assert 'Extraction failed' in result.error

        os.unlink(schema_file)

    def test_pipeline_creates_output_directories(self, sample_input_csv, schema_file):
        """Test that pipeline creates output directories if they don't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            clean_path = os.path.join(tmpdir, 'subdir', 'clean.csv')
            rejected_path = os.path.join(tmpdir, 'subdir', 'rejected.csv')
            db_path = os.path.join(tmpdir, 'db', 'test.db')

            result = run_pipeline(
                input_path=sample_input_csv,
                clean_output_path=clean_path,
                rejected_output_path=rejected_path,
                db_path=db_path,
                schema_path=schema_file
            )

            assert result.status == 'success'
            assert os.path.exists(clean_path)
            assert os.path.exists(rejected_path)
            assert os.path.exists(db_path)

        os.unlink(sample_input_csv)
        os.unlink(schema_file)

    def test_pipeline_result_to_dict(self, sample_input_csv, schema_file, temp_output_paths):
        """Test PipelineResult.to_dict method."""
        clean_path, rejected_path, db_path = temp_output_paths

        result = run_pipeline(
            input_path=sample_input_csv,
            clean_output_path=clean_path,
            rejected_output_path=rejected_path,
            db_path=db_path,
            schema_path=schema_file
        )

        result_dict = result.to_dict()

        assert 'status' in result_dict
        assert 'start_time' in result_dict
        assert 'end_time' in result_dict
        assert 'duration_seconds' in result_dict
        assert 'metrics' in result_dict
        assert 'error' in result_dict

        assert result_dict['status'] == 'success'
        assert result_dict['error'] is None
        assert result_dict['duration_seconds'] > 0

        os.unlink(sample_input_csv)
        os.unlink(schema_file)
