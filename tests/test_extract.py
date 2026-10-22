import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
from etl.extract import extract_csv, extract_raw_data, ExtractionError, REQUIRED_COLUMNS


@pytest.fixture
def valid_csv_file():
    """Create a temporary valid CSV file for testing."""
    data = """order_id,customer_id,order_date,product,category,quantity,unit_price,region
ORD001,CUST001,2024-01-15,Laptop,Electronics,2,1200.00,North
ORD002,CUST002,2024-01-16,Mouse,Electronics,5,25.00,South"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data)
        return f.name


@pytest.fixture
def invalid_csv_missing_columns():
    """Create a CSV file with missing required columns."""
    data = """order_id,customer_id,product
ORD001,CUST001,Laptop"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data)
        return f.name


@pytest.fixture
def empty_csv_file():
    """Create an empty CSV file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("")
        return f.name


@pytest.fixture
def malformed_csv_file():
    """Create a malformed CSV file."""
    data = """order_id,customer_id,order_date,product,category,quantity,unit_price,region
ORD001,CUST001,2024-01-15,Laptop,Electronics,2,1200.00,North,extra,data"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data)
        return f.name


class TestExtractCSV:
    """Test cases for extract_csv function."""

    def test_valid_csv_extraction(self, valid_csv_file):
        """Test extraction of a valid CSV file."""
        df = extract_csv(valid_csv_file)
        assert len(df) == 2
        assert list(df.columns) == REQUIRED_COLUMNS
        assert df['order_id'].tolist() == ['ORD001', 'ORD002']
        os.unlink(valid_csv_file)

    def test_file_not_found(self):
        """Test extraction when file does not exist."""
        with pytest.raises(ExtractionError) as exc_info:
            extract_csv('nonexistent.csv')
        assert 'File not found' in str(exc_info.value)

    def test_missing_required_columns(self, invalid_csv_missing_columns):
        """Test extraction when required columns are missing."""
        with pytest.raises(ExtractionError) as exc_info:
            extract_csv(invalid_csv_missing_columns)
        assert 'Missing required columns' in str(exc_info.value)
        os.unlink(invalid_csv_missing_columns)

    def test_empty_csv(self, empty_csv_file):
        """Test extraction of an empty CSV file."""
        with pytest.raises(ExtractionError) as exc_info:
            extract_csv(empty_csv_file)
        assert 'empty' in str(exc_info.value).lower()
        os.unlink(empty_csv_file)

    def test_non_csv_file(self):
        """Test extraction of a non-CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("test")
            file_path = f.name

        with pytest.raises(ExtractionError) as exc_info:
            extract_csv(file_path)
        assert 'not a CSV' in str(exc_info.value)
        os.unlink(file_path)

    def test_directory_path(self):
        """Test extraction when path is a directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ExtractionError) as exc_info:
                extract_csv(tmpdir)
            assert 'not a file' in str(exc_info.value)


class TestExtractRawData:
    """Test cases for extract_raw_data function."""

    def test_successful_extraction(self, valid_csv_file):
        """Test successful raw data extraction."""
        df = extract_raw_data(valid_csv_file)
        assert len(df) == 2
        assert 'order_id' in df.columns
        os.unlink(valid_csv_file)

    def test_extraction_logs_row_count(self, valid_csv_file, caplog):
        """Test that extraction logs the row count."""
        import logging
        caplog.set_level(logging.INFO)
        extract_raw_data(valid_csv_file)
        assert 'Successfully extracted' in caplog.text
        assert '2 rows' in caplog.text
        os.unlink(valid_csv_file)

    def test_extraction_error_propagation(self, invalid_csv_missing_columns):
        """Test that extraction errors are properly propagated."""
        with pytest.raises(ExtractionError):
            extract_raw_data(invalid_csv_missing_columns)
        os.unlink(invalid_csv_missing_columns)
