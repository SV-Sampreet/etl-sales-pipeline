import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
from sqlalchemy import text
from etl.load import (
    create_database_connection,
    initialize_schema,
    load_data,
    load_data_from_file,
    LoadError
)


@pytest.fixture
def sample_clean_data():
    """Create sample clean data for testing."""
    data = {
        'order_id': ['ORD001', 'ORD002', 'ORD003'],
        'customer_id': ['CUST001', 'CUST002', 'CUST003'],
        'order_date': pd.to_datetime(['2024-01-15', '2024-01-16', '2024-01-17']),
        'product': ['Laptop', 'Mouse', 'Keyboard'],
        'category': ['Electronics', 'Electronics', 'Electronics'],
        'quantity': [2, 5, 3],
        'unit_price': [1200.00, 25.00, 75.00],
        'region': ['North', 'South', 'East'],
        'revenue': [2400.00, 125.00, 225.00]
    }
    return pd.DataFrame(data)


@pytest.fixture
def temp_db_path():
    """Create a temporary database path."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        return f.name


@pytest.fixture
def temp_schema_file():
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


class TestCreateDatabaseConnection:
    """Test cases for create_database_connection function."""

    def test_create_connection(self, temp_db_path):
        """Test successful database connection creation."""
        engine = create_database_connection(temp_db_path)
        assert engine is not None
        os.unlink(temp_db_path)

    def test_connection_creates_directory(self):
        """Test that connection creates parent directory if needed."""
        db_path = os.path.join(tempfile.gettempdir(), 'test_dir', 'test.db')
        engine = create_database_connection(db_path)
        assert engine is not None
        assert os.path.exists(os.path.dirname(db_path))
        # Clean up
        if os.path.exists(db_path):
            os.unlink(db_path)
        if os.path.exists(os.path.dirname(db_path)):
            os.rmdir(os.path.dirname(db_path))


class TestInitializeSchema:
    """Test cases for initialize_schema function."""

    def test_schema_initialization(self, temp_db_path, temp_schema_file):
        """Test successful schema initialization."""
        engine = create_database_connection(temp_db_path)
        initialize_schema(engine, temp_schema_file)

        # Verify table exists
        with engine.connect() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
            tables = [row[0] for row in result]
            assert 'orders' in tables

        os.unlink(temp_db_path)
        os.unlink(temp_schema_file)

    def test_schema_creates_columns(self, temp_db_path, temp_schema_file):
        """Test that schema creates correct columns."""
        engine = create_database_connection(temp_db_path)
        initialize_schema(engine, temp_schema_file)

        with engine.connect() as conn:
            result = conn.execute(text("PRAGMA table_info(orders)"))
            columns = [row[1] for row in result]
            expected_columns = ['order_id', 'customer_id', 'order_date', 'product',
                             'category', 'quantity', 'unit_price', 'region', 'revenue', 'created_at']
            for col in expected_columns:
                assert col in columns

        os.unlink(temp_db_path)
        os.unlink(temp_schema_file)


class TestLoadData:
    """Test cases for load_data function."""

    def test_load_new_records(self, temp_db_path, temp_schema_file, sample_clean_data):
        """Test loading new records."""
        engine = create_database_connection(temp_db_path)
        initialize_schema(engine, temp_schema_file)

        metrics = load_data(sample_clean_data, engine, 'orders')

        assert metrics['total_rows'] == 3
        assert metrics['inserted'] == 3
        assert metrics['updated'] == 0
        assert metrics['errors'] == 0

        # Verify data in database
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM orders"))
            count = result.scalar()
            assert count == 3

        os.unlink(temp_db_path)
        os.unlink(temp_schema_file)

    def test_load_duplicate_records(self, temp_db_path, temp_schema_file, sample_clean_data):
        """Test loading duplicate records (should update)."""
        engine = create_database_connection(temp_db_path)
        initialize_schema(engine, temp_schema_file)

        # First load
        load_data(sample_clean_data, engine, 'orders')

        # Second load with same data
        metrics = load_data(sample_clean_data, engine, 'orders')

        assert metrics['total_rows'] == 3
        assert metrics['inserted'] == 0
        assert metrics['updated'] == 3
        assert metrics['errors'] == 0

        # Verify still only 3 records
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM orders"))
            count = result.scalar()
            assert count == 3

        os.unlink(temp_db_path)
        os.unlink(temp_schema_file)

    def test_load_empty_dataframe(self, temp_db_path, temp_schema_file):
        """Test loading empty DataFrame."""
        engine = create_database_connection(temp_db_path)
        initialize_schema(engine, temp_schema_file)

        empty_df = pd.DataFrame()
        metrics = load_data(empty_df, engine, 'orders')

        assert metrics['total_rows'] == 0
        assert metrics['inserted'] == 0
        assert metrics['updated'] == 0
        assert metrics['errors'] == 0

        os.unlink(temp_db_path)
        os.unlink(temp_schema_file)

    def test_primary_key_constraint(self, temp_db_path, temp_schema_file):
        """Test that primary key constraint prevents duplicates."""
        engine = create_database_connection(temp_db_path)
        initialize_schema(engine, temp_schema_file)

        # Create data with duplicate order_id
        data = {
            'order_id': ['ORD001', 'ORD001'],
            'customer_id': ['CUST001', 'CUST002'],
            'order_date': pd.to_datetime(['2024-01-15', '2024-01-16']),
            'product': ['Laptop', 'Mouse'],
            'category': ['Electronics', 'Electronics'],
            'quantity': [2, 5],
            'unit_price': [1200.00, 25.00],
            'region': ['North', 'South'],
            'revenue': [2400.00, 125.00]
        }
        df = pd.DataFrame(data)

        metrics = load_data(df, engine, 'orders')

        # First record inserted, second should update
        assert metrics['inserted'] == 1
        assert metrics['updated'] == 1

        # Verify only 1 record exists
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM orders"))
            count = result.scalar()
            assert count == 1

        os.unlink(temp_db_path)
        os.unlink(temp_schema_file)


class TestLoadDataFromFile:
    """Test cases for load_data_from_file function."""

    def test_complete_load_process(self, temp_db_path, temp_schema_file, sample_clean_data):
        """Test complete load process from file."""
        metrics = load_data_from_file(sample_clean_data, temp_db_path, temp_schema_file)

        assert metrics['total_rows'] == 3
        assert metrics['inserted'] == 3
        assert metrics['updated'] == 0
        assert metrics['errors'] == 0

        # Verify database exists
        assert os.path.exists(temp_db_path)

        os.unlink(temp_db_path)
        os.unlink(temp_schema_file)

    def test_load_creates_schema_automatically(self, temp_db_path, temp_schema_file, sample_clean_data):
        """Test that load creates schema automatically."""
        # Delete temp_db_path if it exists
        if os.path.exists(temp_db_path):
            os.unlink(temp_db_path)

        metrics = load_data_from_file(sample_clean_data, temp_db_path, temp_schema_file)

        assert metrics['inserted'] == 3
        assert os.path.exists(temp_db_path)

        os.unlink(temp_db_path)
        os.unlink(temp_schema_file)
