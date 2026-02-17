"""Helper utilities for the Streamlit dashboard"""

import random
from contextlib import contextmanager
from datetime import datetime, time, timedelta
from typing import List, Optional, Tuple

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Database configuration
DB_CONFIG = {
    "host": "127.0.0.1",
    "database": "twr",
    "user": "twr_user",
    "password": "twr_password",
}

# Trading constants
MARKET_OPEN = time(9, 30)  # 9:30 AM ET
MARKET_CLOSE = time(16, 0)  # 4:00 PM ET


def parse_frequency(freq_string: str) -> timedelta:
    """Parse frequency string to timedelta object.

    Args:
        freq_string: Frequency as string (e.g., "1 min", "1 hour", "daily")

    Returns:
        timedelta object

    Raises:
        ValueError: If frequency string is not recognized
    """
    frequency_map = {
        "1 min": timedelta(minutes=1),
        "2 min": timedelta(minutes=2),
        "5 min": timedelta(minutes=5),
        "30 min": timedelta(minutes=30),
        "1 hour": timedelta(hours=1),
        "2 hours": timedelta(hours=2),
        "daily": timedelta(days=1),
    }

    if freq_string not in frequency_map:
        raise ValueError(f"Unknown frequency: {freq_string}")

    return frequency_map[freq_string]


def generate_trading_timestamps(
    start_dt: datetime, end_dt: datetime, frequency: timedelta
) -> List[datetime]:
    """Generate timestamps respecting US market hours (9:30 AM - 4:00 PM ET).

    Automatically skips weekends (Saturday/Sunday).

    Args:
        start_dt: Start datetime
        end_dt: End datetime
        frequency: Time between timestamps

    Returns:
        List of datetime objects during trading hours
    """
    timestamps = []
    current_dt = start_dt

    while current_dt <= end_dt:
        # Skip weekends
        while current_dt.weekday() >= 5:  # Saturday=5, Sunday=6
            current_dt += timedelta(days=1)
            current_dt = current_dt.replace(
                hour=MARKET_OPEN.hour,
                minute=MARKET_OPEN.minute,
                second=0,
                microsecond=0,
            )

        # Only include if within date range
        if current_dt <= end_dt:
            timestamps.append(current_dt)

        # Advance to next timestamp
        current_dt += frequency

        # If past market close, jump to next day's market open
        if current_dt.time() >= MARKET_CLOSE:
            current_dt += timedelta(days=1)
            current_dt = current_dt.replace(
                hour=MARKET_OPEN.hour,
                minute=MARKET_OPEN.minute,
                second=0,
                microsecond=0,
            )

    return timestamps


def generate_prices_linear_interpolation(
    start_price: float, end_price: float, timestamps: List[datetime]
) -> List[float]:
    """Generate prices using linear interpolation with random noise.

    The first and last prices will be exactly start_price and end_price.
    Middle prices will follow a linear trend with ±2% random noise.

    Args:
        start_price: Starting price (exact)
        end_price: Ending price (exact)
        timestamps: List of timestamps for prices

    Returns:
        List of prices corresponding to timestamps
    """
    n = len(timestamps)
    if n == 0:
        return []
    if n == 1:
        return [start_price]

    prices = []

    for i in range(n):
        # Calculate linear progress from 0.0 to 1.0
        progress = i / (n - 1)
        baseline_price = start_price + (end_price - start_price) * progress

        # First and last prices are exact
        if i == 0:
            price = start_price
        elif i == n - 1:
            price = end_price
        else:
            # Add random noise: ±2% of baseline price
            noise = baseline_price * random.uniform(-0.02, 0.02)
            price = baseline_price + noise

        prices.append(price)

    return prices


@contextmanager
def get_db_connection():
    """Context manager for database connections.

    Automatically commits on success and rolls back on error.

    Yields:
        psycopg2 connection object
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def insert_product_and_prices(
    product_name: str, timestamps_and_prices: List[Tuple[datetime, float]]
) -> None:
    """Insert or update product and its price history.

    Uses ON CONFLICT to handle duplicate products and timestamps.

    Args:
        product_name: Name of the product
        timestamps_and_prices: List of (timestamp, price) tuples
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Upsert product (get or create)
            cur.execute(
                """
                INSERT INTO product (name)
                VALUES (%s)
                ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
                """,
                (product_name,),
            )
            product_id = cur.fetchone()[0]

            # Prepare batch data
            price_data = [
                (product_id, timestamp, price) for timestamp, price in timestamps_and_prices
            ]

            # Batch insert prices (update on conflict)
            execute_values(
                cur,
                """
                INSERT INTO price_update (product_id, timestamp, price)
                VALUES %s
                ON CONFLICT (product_id, timestamp)
                DO UPDATE SET price = EXCLUDED.price
                """,
                price_data,
            )


def load_all_products() -> List[dict]:
    """Load all products from the database.

    Returns:
        List of dictionaries with 'id' and 'name' keys
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM product ORDER BY name")
            return [{"id": str(row[0]), "name": row[1]} for row in cur.fetchall()]


def load_all_users() -> List[dict]:
    """Load all users from the database.

    Returns:
        List of dictionaries with 'id' and 'name' keys
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT id, name FROM "user" ORDER BY name')
            return [{"id": str(row[0]), "name": row[1]} for row in cur.fetchall()]


def create_user(user_name: str) -> str:
    """Create a new user in the database.

    Args:
        user_name: Name of the user

    Returns:
        User ID as string
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('INSERT INTO "user" (name) VALUES (%s) RETURNING id', (user_name,))
            return str(cur.fetchone()[0])


def insert_cashflow(
    user_id: str,
    product_id: str,
    timestamp: datetime,
    units_delta: Optional[float] = None,
    execution_price: Optional[float] = None,
    execution_money: Optional[float] = None,
    user_money: Optional[float] = None,
    fees: Optional[float] = None,
) -> None:
    """Insert a cashflow transaction.

    At least 3 of the 5 fields (units_delta, execution_price, execution_money,
    user_money, fees) must be provided. The trigger will derive the other 2.

    Args:
        user_id: User UUID as string
        product_id: Product UUID as string
        timestamp: Transaction timestamp
        units_delta: Units bought (positive) or sold (negative)
        execution_price: Price per unit
        execution_money: Total transaction value (units_delta × execution_price)
        user_money: Money that left/entered user's bank (execution_money + fees)
        fees: Transaction fees (always >= 0)
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cashflow
                (user_id, product_id, timestamp, units_delta, execution_price,
                 execution_money, user_money, fees)
                VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s)
                """,
                (
                    user_id,
                    product_id,
                    timestamp,
                    units_delta,
                    execution_price,
                    execution_money,
                    user_money,
                    fees,
                ),
            )


def load_price_data(
    product_ids: List[str],
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """Load price data for selected products.

    Args:
        product_ids: List of product UUIDs (as strings)
        start_date: Optional start date filter
        end_date: Optional end date filter

    Returns:
        DataFrame with columns: product_name, timestamp, price
    """
    if not product_ids:
        return pd.DataFrame(columns=["product_name", "timestamp", "price"])

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Convert string UUIDs to UUID array
            # Build query with optional date filters
            query = """
                SELECT p.name as product_name, pp.timestamp, pp.price
                FROM price_update pp
                JOIN product p ON pp.product_id = p.id
                WHERE pp.product_id = ANY(%s::uuid[])
            """
            params = [product_ids]

            if start_date:
                query += " AND pp.timestamp >= %s"
                params.append(start_date)
            if end_date:
                query += " AND pp.timestamp <= %s"
                params.append(end_date)

            query += " ORDER BY pp.timestamp"

            cur.execute(query, params)

            # Convert to pandas DataFrame
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)


def load_user_product_timeline(
    user_ids: List[str],
    product_ids: Optional[List[str]] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """Load user product timeline data (15min granularity).

    Args:
        user_ids: List of user UUIDs (as strings)
        product_ids: Optional list of product UUIDs to filter by
        start_date: Optional start date filter
        end_date: Optional end date filter

    Returns:
        DataFrame with timeline data
    """
    if not user_ids:
        return pd.DataFrame()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Build query
            query = """
                SELECT
                    u.name as user_name,
                    p.name as product_name,
                    upt.timestamp,
                    upt.units_held,
                    upt.market_price,
                    upt.market_value,
                    upt.net_investment,
                    upt.deposits,
                    upt.withdrawals,
                    upt.fees
                FROM user_product_timeline_15min upt
                JOIN "user" u ON upt.user_id = u.id
                JOIN product p ON upt.product_id = p.id
                WHERE upt.user_id = ANY(%s::uuid[])
            """
            params = [user_ids]

            if product_ids:
                query += " AND upt.product_id = ANY(%s::uuid[])"
                params.append(product_ids)

            if start_date:
                query += " AND upt.timestamp >= %s"
                params.append(start_date)
            if end_date:
                query += " AND upt.timestamp <= %s"
                params.append(end_date)

            query += " ORDER BY upt.timestamp, u.name, p.name"

            cur.execute(query, params)

            # Convert to pandas DataFrame
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                return pd.DataFrame(cur.fetchall(), columns=columns)
            else:
                return pd.DataFrame()
