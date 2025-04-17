"""
rock/db.py
"""
import sqlite3
import os
from enum import StrEnum
from typing import Mapping, Any
from datetime import datetime as dt


DB_NAME = 'rock.db'
DB_PATH = os.path.expanduser(f'~/rockdb/{DB_NAME}')


class Tables(StrEnum):
    """Table names."""
    SECURITY = 'security'
    EXCHANGE = 'exchange'
    HISTORY = 'history'


def init() -> None:
    """Initialize the database."""
    if os.path.exists(DB_PATH):
        print(f'Database {DB_PATH} is already exist.')
        return

    def adapt_datetime_epoch(val):
        """Adapt datetime to Unix timestamp."""
        return int(val.timestamp())
    def convert_epoch_datetime(val):
        """Convert Unix timestamp to datetime."""
        return dt.fromtimestamp(int(val))
    sqlite3.register_adapter(dt, adapt_datetime_epoch)
    sqlite3.register_converter("timestamp", convert_epoch_datetime)

    def create_security_table():
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {Tables.SECURITY} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL UNIQUE CHECK (symbol != ''),
                name TEXT NOT NULL UNIQUE CHECK (name != ''),
                type TEXT CHECK (type IN ('stock', 'bond', 'fund')),
                exchange_id INTEGER REFERENCES {Tables.EXCHANGE}(id),
                history_updated_at TIMESTAMP NOT NULL DEFAULT 0 CHECK ( history_updated_at >= 0)
            )
        ''')
    def create_exchange_table():
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {Tables.EXCHANGE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE CHECK (name != ''),
                acronym TEXT NOT NULL UNIQUE CHECK (acronym != ''),
                type TEXT NOT NULL CHECK (type IN ('stock', 'bond', 'fund'))
            )
        ''')
    def create_history_table():
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {Tables.HISTORY} (
                security_id INTEGER NOT NULL REFERENCES {Tables.SECURITY}(id),
                datetime TIMESTAMP NOT NULL DEFAULT 0 CHECK ( datetime >= 0),
                open REAL NOT NULL,
                close REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                adj_close REAL NOT NULL,
                volume INTEGER NOT NULL CHECK (volume >= 0),
                frequency TEXT NOT NULL CHECK (frequency IN ('1m', '1d')),
                PRIMARY KEY (security_id, datetime)
            )
        ''')

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        create_security_table()
        create_exchange_table()
        create_history_table()
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def get_db_connection() -> sqlite3.Connection:
    """Get a database connection."""
    connection = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA foreign_keys = ON;')
    return connection


def insert_exchange(name: str, acronym: str, exchange_type: str) -> None:
    """Insert exchange data into the database."""
    connection = get_db_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f'''
            INSERT INTO {Tables.EXCHANGE} (name, acronym, type)
            VALUES (?, ?, ?)
        ''', (name, acronym, exchange_type))
        cursor.close()
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def insert_security(symbol: str, name: str, symbol_type: str, exchange_id: int) -> None:
    """Insert security data into the database."""
    connection = get_db_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f'''
            INSERT INTO {Tables.SECURITY} (symbol, name, type, exchange_id)
            VALUES (?, ?, ?, ?)
        ''', (symbol, name, symbol_type, exchange_id))
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def insert_securities(securities: list[tuple[str, str, str, int]]) -> None:
    """Insert multiple securities into the database."""
    connection = get_db_connection()
    try:
        cursor = connection.cursor()
        cursor.executemany(f'''
            INSERT INTO {Tables.SECURITY} (symbol, name, type, exchange_id)
            VALUES (?, ?, ?, ?)
        ''', securities)
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def insert_history(security_id: int, date: str, open_price: float, close_price: float,
                  high_price: float, low_price: float, adj_close: float, volume: int, frequency: str) -> None:
    """Insert history data into the database."""
    bulk_insert_history([
        (security_id, date, open_price, close_price, high_price, low_price, adj_close, volume, frequency)
    ])


def bulk_insert_history(history: list[tuple[int, str, float, float, float, float, float, int, str]]) -> None:
    """Insert multiple history into the database."""
    connection = get_db_connection()
    transformed_history = ((item[0], dt.fromisoformat(item[1]), *item[2:]) for item in history)
    try:
        cursor = connection.cursor()
        cursor.executemany(f'''
            INSERT INTO {Tables.HISTORY} (security_id, datetime, open, close, high, low, adj_close, volume, frequency)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', transformed_history)
        cursor.executemany(f'''
            UPDATE {Tables.SECURITY} SET history_updated_at = ?
            WHERE id = ?
        ''', ((dt.now(), item[0]) for item in history))
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def get_security(symbol: str) -> Mapping[str, Any]:
    """Get security data from the database."""
    connection = get_db_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f'''
            SELECT * FROM {Tables.SECURITY} WHERE symbol=?
        ''', (symbol,))
        return cursor.fetchone()
    finally:
        cursor.close()
        connection.close()


def has_history(security: str, end: str) -> bool:
    """
    Check if history exists for a given security ID by comparing the end date with
    the history_updated_at date in security table. We assume that if the history_updated_at
    date is greater than the end date, then history exists.
    Args:
        security (str): The security symbol.
        start (str): The start date in YYYY-MM-DD format.
        end (str): The end date in YYYY-MM-DD format.
    Returns:
        bool: True if history exists, False otherwise.
    """
    security = get_security(security)

    history_updated_at = security['history_updated_at']
    history_end = dt.fromisoformat(end)
    return history_updated_at >= history_end
