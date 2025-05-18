"""This module provides utility functions for the project."""

import sqlite3
from datetime import datetime

def get_sql_version():
    """Return the version of the sqlite3 library."""
    return sqlite3.sqlite_version

def get_current_date():
    """Return the current date in YYYY-MM-DD format."""
    return datetime.now().strftime('%Y-%m-%d')

def get_epoch_date():
    """Return the epoch date in YYYY-MM-DD format."""
    return '1970-01-01'

def compare_dates(date1: str, date2: str) -> int:
    """Compare two dates in YYYY-MM-DD format."""
    d1 = datetime.strptime(date1, '%Y-%m-%d')
    d2 = datetime.strptime(date2, '%Y-%m-%d')
    return (d1 > d2) - (d1 < d2)

if __name__ == '__main__':
    print(sqlite3.sqlite_version)
