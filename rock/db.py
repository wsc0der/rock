"""
rock/db.py
"""
import sqlite3
import os
from enum import StrEnum
from pandas import DataFrame, read_sql

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
        print(f'Database {DB_NAME} is already exist.')
        return

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    connection = get_db_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {Tables.SECURITY} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL UNIQUE CHECK (symbol != ''),
                name TEXT NOT NULL UNIQUE CHECK (name != ''),
                type TEXT CHECK (type IN ('stock', 'bond', 'fund')),
                exchange_id INTEGER REFERENCES {Tables.EXCHANGE}(id)
            )
        ''')

        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {Tables.EXCHANGE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE CHECK (name != ''),
                acronym TEXT NOT NULL UNIQUE CHECK (acronym != ''),
                type TEXT NOT NULL CHECK (type IN ('stock', 'bond', 'fund'))
            )
        ''')

        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {Tables.HISTORY} (
                security_id INTEGER NOT NULL REFERENCES {Tables.SECURITY}(id),
                date TEXT NOT NULL ChECK (date != ''),
                open REAL NOT NULL,
                close REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                adj_close REAL NOT NULL,
                volume INTEGER NOT NULL CHECK (volume >= 0),
                frequency TEXT NOT NULL CHECK (frequency IN ('1m', '1d')),
                PRIMARY KEY (security_id, date)
            )
        ''')
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def get_db_connection() -> sqlite3.Connection:
    """Get a database connection."""
    connection = sqlite3.connect(DB_PATH)
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
    connection = get_db_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f'''
            INSERT INTO {Tables.HISTORY} (security_id, date, open, close, high, low, adj_close, volume, frequency)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (security_id, date, open_price, close_price, high_price, low_price, adj_close, volume, frequency))
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def insert_prices(prices: list[tuple[int, str, float, float, float, float, float, int, str]]) -> None:
    """Insert multiple prices into the database."""
    connection = get_db_connection()
    try:
        cursor = connection.cursor()
        cursor.executemany(f'''
            INSERT INTO {Tables.HISTORY} (security_id, date, open, close, high, low, adj_close, volume, frequency)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', prices)
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def get_security(symbol: str) -> DataFrame:
    """Get security data from the database."""
    connection = get_db_connection()
    try:
        query = f'''
            SELECT * FROM {Tables.SECURITY} WHERE symbol=?
        '''
        df = read_sql(query, connection, params=(symbol,))
    finally:
        connection.close()
    return df
