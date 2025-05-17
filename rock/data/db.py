"""
rock/db.py
"""
import sqlite3
import os
from collections.abc import Mapping
from typing import Any
from enum import StrEnum
from datetime import datetime as dt
from rock import logger


DB_NAME = 'rock.db'
DB_PATH = os.path.expanduser(f'~/rockdb/{DB_NAME}')


class Tables(StrEnum):
    """Table names."""
    SECURITY = 'security'
    EXCHANGE = 'exchange'
    HISTORY = 'history'


def create_db() -> None:
    """Initialize the database."""
    if db_exist():
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
                listing TIMESTAMP NOT NULL DEFAULT 0 CHECK ( listing >= 0),
                delisting TIMESTAMP DEFAULT NULL CHECK ( delisting > listing),
                exchange_id INTEGER REFERENCES {Tables.EXCHANGE}(id)
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
    connection = get_connection()
    cursor = connection.cursor()
    try:
        create_security_table()
        create_exchange_table()
        create_history_table()
        connection.commit()
        logger.info('Database %s created successfully.', DB_PATH)
    finally:
        cursor.close()
        connection.close()


def db_exist() -> bool:
    """Check if the database exists."""
    return os.path.exists(DB_PATH)


def get_connection() -> sqlite3.Connection:
    """Get a database connection."""
    connection = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA foreign_keys = ON;')
    return connection


def insert_exchange(name: str, acronym: str, exchange_type: str) -> None:
    """Insert exchange data into the database."""
    connection = get_connection()
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


def insert_security(symbol: str, name: str, symbol_type: str, listing: str,
                    delisting: str|None, exchange_id: int) -> None:
    """Insert security data into the database."""
    connection = get_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f'''
            INSERT INTO {Tables.SECURITY} (symbol, name, type, listing, delisting, exchange_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (symbol, name, symbol_type, dt.fromisoformat(listing),
              None if delisting is None else dt.fromisoformat(delisting),
              exchange_id))
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def insert_securities(securities: list[tuple[str, str, str, str, str, int]]) -> None:
    """Insert multiple securities into the database."""
    connection = get_connection()
    try:
        cursor = connection.cursor()
        cursor.executemany(f'''
            INSERT INTO {Tables.SECURITY} (symbol, name, type, listing, delisting, exchange_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', [(row[0], row[1], row[2],
               dt.fromisoformat(row[3]),
               dt.fromisoformat(row[4]) if row[4] is not None else None,
               row[5]) for row in securities])
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
    connection = get_connection()
    transformed_history = ((item[0], dt.fromisoformat(item[1]), *item[2:]) for item in history)
    try:
        cursor = connection.cursor()
        cursor.executemany(f'''
            INSERT INTO {Tables.HISTORY} (security_id, datetime, open, close, high, low, adj_close, volume, frequency)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', transformed_history)
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def update_security_delisting(symbol: str, delisting: str) -> None:
    """Update security data in the database."""
    connection = get_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f'''
            UPDATE {Tables.SECURITY} SET delisting = ?
            WHERE symbol = ?
        ''', (dt.fromisoformat(delisting), symbol))
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def get_all_securities() -> list[sqlite3.Row]:
    """Get all security data from the database."""
    connection = get_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f'''
            SELECT * FROM {Tables.SECURITY}
        ''')
        result = cursor.fetchall()
        return result
    finally:
        cursor.close()
        connection.close()


def get_security(symbols: str|list[str]) -> sqlite3.Row|list[sqlite3.Row]|None:
    """Get security data from the database."""
    return _get_data_from_table(Tables.SECURITY, 'symbol', symbols)


def get_exchange(ex: str|list[str]) -> sqlite3.Row|list[sqlite3.Row]|None:
    """Get exchange data from the database."""
    return _get_data_from_table(Tables.EXCHANGE, 'acronym', ex)


def get_history(symbols: str|list[str], start: str|None = None,
                end: str|None = None) -> Mapping[str, list[sqlite3.Row]]:
    """Get history data from the database."""
    if isinstance(symbols, str):
        symbols = [symbols]

    if not isinstance(symbols, list):
        return {}

    s = dt.fromisoformat(start) if start else None
    e = dt.fromisoformat(end) if end else None

    connection = get_connection()
    try:
        cursor = connection.cursor()
        result = {}
        for symbol in symbols:
            cursor.execute(f'''
                SELECT * FROM {Tables.SECURITY} WHERE symbol = ?
            ''', (symbol,))
            security = cursor.fetchone()
            if not security:
                result[symbol] = []
                continue

            cursor.execute(f'''
                SELECT * FROM {Tables.HISTORY} WHERE security_id = ?
                AND datetime >= ? AND datetime <= ?
            ''', (security['id'],
                  0 if s is None else s,
                  dt.max if e is None else e))
            history = cursor.fetchall()
            if not history:
                result[symbol] = []
                continue

            result[symbol] = history

        return result
    finally:
        cursor.close()
        connection.close()

def get_exchange_id(acronym: str) -> int|None:
    """Get exchange ID from the database."""
    connection = get_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f'''
            SELECT id FROM {Tables.EXCHANGE} WHERE acronym = ?
        ''', (acronym,))
        result = cursor.fetchone()
        if result:
            return result['id']
        return None
    finally:
        cursor.close()
        connection.close()


def _get_data_from_table(table: str, field: str, value_in: Any|list[Any]) -> sqlite3.Row|list[sqlite3.Row]|None:
    """Get data from the database."""
    if isinstance(value_in, str):
        value = [value_in]
    else:
        value = value_in

    if not isinstance(value, list):
        return None

    connection = get_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f'''
            SELECT * FROM {table} WHERE {field} IN ({','.join(['?'] * len(value))})
        ''', value)
        result = cursor.fetchall()
        if isinstance(value_in, list):
            return result
        if len(result) > 0:
            return result[0]
        return None
    finally:
        cursor.close()
        connection.close()
