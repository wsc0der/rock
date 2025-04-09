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
    PRICE = 'price'

def init():
    """Initialize the database."""
    if os.path.exists(DB_PATH):
        print(f'Database {DB_NAME} already exists.')
        return

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    connection = sqlite3.connect(DB_PATH)
    connection.close()


def create_security_table():
    """Create stock info table."""
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {Tables.SECURITY} (
            id INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            type TEXT CHECK (type IN ('stock', 'bond', 'fund')),
            exchange_id INTEGER REFERENCES exchange(id)
        )
    ''')
    connection.commit()
    connection.close()


def create_exchange_table():
    """Create exchange table."""
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {Tables.EXCHANGE} (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            acronym TEXT NOT NULL,
            type TEXT NOT NULL CHECK (type IN ('stock', 'bond', 'fund'))
        )
    ''')
    connection.commit()
    connection.close()


def create_price_table():
    """Create price table."""
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {Tables.PRICE} (
            id INTEGER PRIMARY KEY,
            security_id INTEGER REFERENCES security(id),
            date TEXT NOT NULL,
            open REAL NOT NULL,
            close REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            adj_close REAL NOT NULL,
            volume INTEGER NOT NULL,
            frequency TEXT CHECK (frequency IN ('1m', '1d'))
        )
    ''')
    connection.commit()
    connection.close()


def insert_exchange(name: str, acronym: str, type_: str):
    """Insert exchange data into the database."""
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()
    cursor.execute(f'''
        INSERT INTO {Tables.EXCHANGE} (name, acronym, type)
        VALUES (?, ?, ?)
    ''', (name, acronym, type_))
    connection.commit()
    connection.close()


def insert_security(symbol: str, name: str, type_: str, exchange_id: int):
    """Insert security data into the database."""
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()
    cursor.execute(f'''
        INSERT INTO {Tables.SECURITY} (symbol, name, type, exchange_id)
        VALUES (?, ?, ?, ?)
    ''', (symbol, name, type_, exchange_id))
    connection.commit()
    connection.close()


def insert_securities(securities: list[tuple[str, str, str, int]]):
    """Insert multiple securities into the database."""
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()
    cursor.executemany(f'''
        INSERT INTO {Tables.SECURITY} (symbol, name, type, exchange_id)
        VALUES (?, ?, ?, ?)
    ''', securities)
    connection.commit()
    connection.close()


def insert_price(security_id: int, date: str, open_: float, close_: float,
                  high_: float, low_: float, adj_close: float, volume: int, frequency: str):
    """Insert price data into the database."""
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()
    cursor.execute(f'''
        INSERT INTO {Tables.PRICE} (security_id, date, open, close, high, low, adj_close, volume, frequency)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (security_id, date, open_, close_, high_, low_, adj_close, volume, frequency))
    connection.commit()
    connection.close()


def insert_prices(prices: list[tuple[int, str, float, float, float, float, float, int, str]]):
    """Insert multiple prices into the database."""
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()
    cursor.executemany(f'''
        INSERT INTO {Tables.PRICE} (security_id, date, open, close, high, low, adj_close, volume, frequency)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', prices)
    connection.commit()
    connection.close()


def get_security(symbol: str) -> DataFrame:
    """Get security data from the database."""
    connection = sqlite3.connect(DB_PATH)
    query = f'''
        SELECT * FROM {Tables.SECURITY} WHERE symbol=?
    '''
    df = read_sql(query, connection, params=(symbol,))
    connection.close()
    return df
