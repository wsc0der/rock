"""This module provides utility functions for the project."""

import sqlite3

def get_sql_version():
    """Return the version of the sqlite3 library."""
    return sqlite3.sqlite_version

if __name__ == '__main__':
    print(sqlite3.sqlite_version)
