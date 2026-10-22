import pytest
import pandas as pd
from etl.transform import (
    transform_data,
    normalize_text,
    DataQualityValidator,
    TransformationError
)


@pytest.fixture
def sample_raw_data():
    """Create sample raw data for testing."""
    data = {
        'order_id': ['ORD001', 'ORD002', 'ORD003', 'ORD004', 'ORD005'],
        'customer_id': ['CUST001', 'CUST002', 'CUST003', 'CUST004', 'CUST005'],
        'order_date': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-19'],
        'product': ['laptop', 'MOUSE', 'Keyboard', 'Monitor', 'headphones'],
        'category': ['electronics', 'ELECTRONICS', 'Electronics', 'Electronics', 'Electronics'],
        'quantity': [2, 5, 3, 1, 4],
        'unit_price': [1200.00, 25.00, 75.00, 350.00, 150.00],
        'region': ['north', 'SOUTH', 'East', 'West', 'North']
    }
    return pd.DataFrame(data)


@pytest.fixture
def data_with_issues():
    """Create data with various quality issues."""
    data = {
        'order_id': ['ORD001', 'ORD002', 'ORD003', 'ORD004', 'ORD005', 'ORD006', 'ORD007'],
        'customer_id': ['CUST001', 'CUST002', '', 'CUST004', 'CUST005', 'CUST006', 'CUST007'],
        'order_date': ['2024-01-15', '2024-02-30', '2024-01-17', '2024-01-18', '2024-01-19', '2024-01-20', '2024-01-21'],
        'product': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones', 'Desk Chair', 'Desk'],
        'category': ['Electronics', 'Electronics', 'Electronics', 'Electronics', 'Electronics', 'Furniture', 'Furniture'],
        'quantity': [2, 5, 3, 'abc', 4, 0, -1],
        'unit_price': [1200.00, 25.00, 75.00, 350.00, -50.00, 200.00, 400.00],
        'region': ['North', 'South', 'East', 'West', 'North', 'South', 'East']
    }
    return pd.DataFrame(data)


@pytest.fixture
def data_with_duplicates():
    """Create data with duplicate order IDs."""
    data = {
        'order_id': ['ORD001', 'ORD002', 'ORD001', 'ORD003'],
        'customer_id': ['CUST001', 'CUST002', 'CUST003', 'CUST004'],
        'order_date': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18'],
        'product': ['Laptop', 'Mouse', 'Keyboard', 'Monitor'],
        'category': ['Electronics', 'Electronics', 'Electronics', 'Electronics'],
        'quantity': [2, 5, 3, 1],
        'unit_price': [1200.00, 25.00, 75.00, 350.00],
        'region': ['North', 'South', 'East', 'West']
    }
    return pd.DataFrame(data)


class TestNormalizeText:
    """Test cases for normalize_text function."""

    def test_normalize_text_basic(self):
        """Test basic text normalization."""
        assert normalize_text('  laptop  ') == 'Laptop'
        assert normalize_text('MOUSE') == 'Mouse'
        assert normalize_text('keyboard') == 'Keyboard'

    def test_normalize_text_empty(self):
        """Test normalization of empty values."""
        assert normalize_text('') == ''
        assert normalize_text(None) == ''
        assert normalize_text(pd.NA) == ''


class TestDataQualityValidator:
    """Test cases for DataQualityValidator class."""

    def test_valid_date(self):
        """Test valid date detection."""
        assert DataQualityValidator.is_valid_date('2024-01-15') == True
        assert DataQualityValidator.is_valid_date('2024-02-30') == False
        assert DataQualityValidator.is_valid_date('') == False
        assert DataQualityValidator.is_valid_date(None) == False

    def test_valid_quantity(self):
        """Test valid quantity detection."""
        assert DataQualityValidator.is_valid_quantity(5) == True
        assert DataQualityValidator.is_valid_quantity(5.0) == True
        assert DataQualityValidator.is_valid_quantity(0) == False
        assert DataQualityValidator.is_valid_quantity(-1) == False
        assert DataQualityValidator.is_valid_quantity(5.5) == False
        assert DataQualityValidator.is_valid_quantity('abc') == False
        assert DataQualityValidator.is_valid_quantity(None) == False

    def test_valid_price(self):
        """Test valid price detection."""
        assert DataQualityValidator.is_valid_price(25.00) == True
        assert DataQualityValidator.is_valid_price(0) == True
        assert DataQualityValidator.is_valid_price(-50.00) == False
        assert DataQualityValidator.is_valid_price('abc') == False
        assert DataQualityValidator.is_valid_price(None) == False

    def test_has_required_fields(self):
        """Test required field validation."""
        row = pd.Series({
            'order_id': 'ORD001',
            'customer_id': 'CUST001',
            'order_date': '2024-01-15',
            'product': 'Laptop',
            'category': 'Electronics',
            'quantity': 2,
            'unit_price': 1200.00,
            'region': 'North'
        })
        assert DataQualityValidator.has_required_fields(row) == True

        row_missing = pd.Series({
            'order_id': 'ORD001',
            'customer_id': '',
            'order_date': '2024-01-15',
            'product': 'Laptop',
            'category': 'Electronics',
            'quantity': 2,
            'unit_price': 1200.00,
            'region': 'North'
        })
        assert DataQualityValidator.has_required_fields(row_missing) == False


class TestTransformData:
    """Test cases for transform_data function."""

    def test_successful_transformation(self, sample_raw_data):
        """Test successful data transformation."""
        clean_df, rejected_df, metrics = transform_data(sample_raw_data)

        assert metrics['input_count'] == 5
        assert metrics['accepted_count'] == 5
        assert metrics['rejected_count'] == 0
        assert metrics['duplicate_count'] == 0
        assert len(clean_df) == 5
        assert len(rejected_df) == 0

    def test_text_normalization(self, sample_raw_data):
        """Test that text fields are normalized."""
        clean_df, _, _ = transform_data(sample_raw_data)

        assert clean_df['product'].tolist() == ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones']
        assert clean_df['category'].tolist() == ['Electronics', 'Electronics', 'Electronics', 'Electronics', 'Electronics']
        assert clean_df['region'].tolist() == ['North', 'South', 'East', 'West', 'North']

    def test_revenue_calculation(self, sample_raw_data):
        """Test revenue calculation."""
        clean_df, _, _ = transform_data(sample_raw_data)

        # Check revenue is calculated correctly
        assert clean_df.loc[0, 'revenue'] == 2400.00  # 2 * 1200
        assert clean_df.loc[1, 'revenue'] == 125.00  # 5 * 25
        assert clean_df.loc[2, 'revenue'] == 225.00  # 3 * 75

    def test_data_quality_rejection(self, data_with_issues):
        """Test rejection of records with data quality issues."""
        clean_df, rejected_df, metrics = transform_data(data_with_issues)

        assert metrics['input_count'] == 7
        assert metrics['accepted_count'] == 1  # Only ORD006 should be accepted
        assert metrics['rejected_count'] == 6
        assert len(clean_df) == 1
        assert len(rejected_df) == 6

        # Check rejection reasons are tracked
        assert 'Missing required field' in metrics['rejection_reasons']
        assert 'Invalid date' in metrics['rejection_reasons']
        assert 'Invalid quantity' in metrics['rejection_reasons']
        assert 'Invalid price' in metrics['rejection_reasons']

    def test_duplicate_detection(self, data_with_duplicates):
        """Test duplicate detection using order_id."""
        clean_df, rejected_df, metrics = transform_data(data_with_duplicates)

        assert metrics['input_count'] == 4
        assert metrics['duplicate_count'] == 1
        assert metrics['accepted_count'] == 3
        assert metrics['rejected_count'] == 1
        assert len(clean_df) == 3
        assert len(rejected_df) == 1

        # Check that duplicate is in rejected
        assert 'Duplicate order_id' in rejected_df['rejection_reason'].values

    def test_empty_dataframe(self):
        """Test transformation of empty DataFrame."""
        df = pd.DataFrame()
        clean_df, rejected_df, metrics = transform_data(df)

        assert metrics['input_count'] == 0
        assert metrics['accepted_count'] == 0
        assert metrics['rejected_count'] == 0
        assert len(clean_df) == 0
        assert len(rejected_df) == 0

    def test_rejected_record_preservation(self, data_with_issues):
        """Test that rejected records are preserved with reasons."""
        _, rejected_df, _ = transform_data(data_with_issues)

        assert 'rejection_reason' in rejected_df.columns
        assert len(rejected_df) > 0

        # Check that original data is preserved
        assert 'order_id' in rejected_df.columns
        assert 'customer_id' in rejected_df.columns
