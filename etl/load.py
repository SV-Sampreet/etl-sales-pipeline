import pandas as pd
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, Any

logger = logging.getLogger(__name__)


class LoadError(Exception):
    """Custom exception for loading errors."""
    pass


def create_database_connection(db_path: str) -> 'sqlalchemy.engine.Engine':
    """
    Create a database connection using SQLAlchemy.

    Args:
        db_path: Path to SQLite database file

    Returns:
        SQLAlchemy engine
    """
    # Ensure parent directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    # Create connection string
    connection_string = f"sqlite:///{db_path}"

    logger.info(f"Creating database connection: {db_path}")
    engine = create_engine(connection_string)

    return engine


def initialize_schema(engine: 'sqlalchemy.engine.Engine', schema_path: str) -> None:
    """
    Initialize database schema from SQL file.

    Args:
        engine: SQLAlchemy engine
        schema_path: Path to schema.sql file
    """
    logger.info(f"Initializing schema from {schema_path}")

    with open(schema_path, 'r') as f:
        schema_sql = f.read()

    # Split SQL statements by semicolon and execute one at a time
    statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]

    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))

    logger.info("Schema initialized successfully")


def load_data(df: pd.DataFrame, engine: 'sqlalchemy.engine.Engine',
              table_name: str = 'orders') -> Dict[str, Any]:
    """
    Load cleaned data into database using upsert logic.

    Args:
        df: Clean DataFrame to load
        engine: SQLAlchemy engine
        table_name: Target table name

    Returns:
        Dictionary with load metrics

    Raises:
        LoadError: If loading fails
    """
    if df.empty:
        logger.warning("Empty DataFrame provided for loading")
        return {
            'total_rows': 0,
            'inserted': 0,
            'updated': 0,
            'errors': 0
        }

    logger.info(f"Loading {len(df)} rows into {table_name}")

    metrics = {
        'total_rows': len(df),
        'inserted': 0,
        'updated': 0,
        'errors': 0
    }

    try:
        with engine.begin() as conn:
            for idx, row in df.iterrows():
                try:
                    # Check if order_id exists
                    check_query = text(f"""
                        SELECT order_id FROM {table_name} WHERE order_id = :order_id
                    """)
                    result = conn.execute(check_query, {'order_id': row['order_id']})

                    if result.fetchone():
                        # Update existing record (only if revenue differs)
                        update_query = text(f"""
                            UPDATE {table_name}
                            SET customer_id = :customer_id,
                                order_date = :order_date,
                                product = :product,
                                category = :category,
                                quantity = :quantity,
                                unit_price = :unit_price,
                                region = :region,
                                revenue = :revenue
                            WHERE order_id = :order_id
                        """)
                        conn.execute(update_query, {
                            'order_id': row['order_id'],
                            'customer_id': row['customer_id'],
                            'order_date': str(row['order_date'].date()) if hasattr(row['order_date'], 'date') else str(row['order_date']),
                            'product': row['product'],
                            'category': row['category'],
                            'quantity': int(row['quantity']),
                            'unit_price': float(row['unit_price']),
                            'region': row['region'],
                            'revenue': float(row['revenue'])
                        })
                        metrics['updated'] += 1
                    else:
                        # Insert new record
                        insert_query = text(f"""
                            INSERT INTO {table_name}
                            (order_id, customer_id, order_date, product, category,
                             quantity, unit_price, region, revenue)
                            VALUES
                            (:order_id, :customer_id, :order_date, :product, :category,
                             :quantity, :unit_price, :region, :revenue)
                        """)
                        conn.execute(insert_query, {
                            'order_id': row['order_id'],
                            'customer_id': row['customer_id'],
                            'order_date': str(row['order_date'].date()) if hasattr(row['order_date'], 'date') else str(row['order_date']),
                            'product': row['product'],
                            'category': row['category'],
                            'quantity': int(row['quantity']),
                            'unit_price': float(row['unit_price']),
                            'region': row['region'],
                            'revenue': float(row['revenue'])
                        })
                        metrics['inserted'] += 1

                except SQLAlchemyError as e:
                    logger.error(f"Error loading row {idx} (order_id: {row['order_id']}): {e}")
                    metrics['errors'] += 1
                    # Continue with next row

        logger.info(f"Load completed. Inserted: {metrics['inserted']}, "
                    f"Updated: {metrics['updated']}, Errors: {metrics['errors']}")

        return metrics

    except SQLAlchemyError as e:
        logger.error(f"Database error during load: {e}")
        raise LoadError(f"Failed to load data: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during load: {e}")
        raise LoadError(f"Unexpected error during load: {e}")


def load_data_from_file(df: pd.DataFrame, db_path: str,
                        schema_path: str, table_name: str = 'orders') -> Dict[str, Any]:
    """
    Load data from DataFrame to database with schema initialization.

    Args:
        df: Clean DataFrame
        db_path: Path to SQLite database
        schema_path: Path to schema.sql
        table_name: Target table name

    Returns:
        Load metrics dictionary
    """
    logger.info("Starting data load process")

    try:
        # Create connection
        engine = create_database_connection(db_path)

        # Initialize schema
        initialize_schema(engine, schema_path)

        # Load data
        metrics = load_data(df, engine, table_name)

        return metrics

    except LoadError:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in load process: {e}")
        raise LoadError(f"Load process failed: {e}")
