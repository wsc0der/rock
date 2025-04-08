"""
test_db.py
"""

import unittest
import os
import sqlite3
from rock import db

class TestDatabase(unittest.TestCase):
    """Test cases for the database module."""
    def setUp(self):
        db.init()
        self.connection = sqlite3.connect(db.DB_PATH)
        return super().setUp()

    def tearDown(self):
        # Close the database connection if it's open
        try:
            self.connection.close()
        except AttributeError:
            pass

        if os.path.exists(db.DB_PATH):
            os.remove(db.DB_PATH)
            os.rmdir(os.path.dirname(db.DB_PATH))
        return super().tearDown()

    def test_db_initialization(self):
        """Test the database initialization."""

        # Check if the database file exists assuming it's already
        # created in the setup function
        self.assertTrue(os.path.exists(db.DB_PATH), "Database file should exist after initialization.")

    def test_create_security_table(self):
        """Test the creation of the stock_info table."""

        # Create the stock_info table
        db.create_security_table(self.connection)

        # Check if the table was created successfully
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{db.Tables.SECURITY}';")
        table_exists = cursor.fetchone() is not None

        self.assertTrue(table_exists, "stock_info table should exist after creation.")

    def test_create_exchange_table(self):
        """Test the creation of the exchange table."""

        # Create the exchange table
        db.create_exchange_table(self.connection)

        # Check if the table was created successfully
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{db.Tables.EXCHANGE}';")
        table_exists = cursor.fetchone() is not None

        self.assertTrue(table_exists, "exchange table should exist after creation.")

    def test_create_price_table(self):
        """Test the creation of the price table."""

        # Create the price table
        db.create_price_table(self.connection)

        # Check if the table was created successfully
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{db.Tables.PRICE}';")
        table_exists = cursor.fetchone() is not None

        self.assertTrue(table_exists, "price table should exist after creation.")

    def test_insert_exchange(self):
        """Test inserting an exchange into the database."""

        # Create the exchange table
        db.create_exchange_table(self.connection)

        # Insert an exchange
        db.insert_exchange(self.connection, 'Shanghai Stock Exchange', 'SSE', 'stock')

        # Check if the exchange was inserted successfully
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT * FROM {db.Tables.EXCHANGE} WHERE acronym='SSE';")
        exchange = cursor.fetchone()

        self.assertIsNotNone(exchange, "Exchange should be inserted into the database.")
        self.assertEqual(exchange[1], 'Shanghai Stock Exchange', "Exchange name should match.")

    def test_insert_security(self):
        """Test inserting a security into the database."""

        # Create the exchange table
        db.create_exchange_table(self.connection)

        # Create the security table
        db.create_security_table(self.connection)

        # Insert an exchange first
        db.insert_exchange(self.connection, 'Shanghai Stock Exchange', 'SSE', 'stock')

        # Insert a security
        db.insert_security(self.connection, '000001', 'Ping An Bank', 'stock', 1)

        # Check if the security was inserted successfully
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT * FROM {db.Tables.SECURITY} WHERE symbol='000001';")
        security = cursor.fetchone()

        self.assertIsNotNone(security, "Security should be inserted into the database.")
        self.assertEqual(security[1], '000001', "Security symbol should match.")
        self.assertEqual(security[2], 'Ping An Bank', "Security name should match.")
        self.assertEqual(security[3], 'stock', "Security type should match.")

    def test_insert_price(self):
        """Test inserting a price into the database."""

        # Create the exchange table
        db.create_exchange_table(self.connection)

        # Create the security table
        db.create_security_table(self.connection)

        # Create the price table
        db.create_price_table(self.connection)

        # Insert an exchange first
        db.insert_exchange(self.connection, 'Shanghai Stock Exchange', 'SSE', 'stock')

        # Insert a security first
        db.insert_security(self.connection, '000001', 'Ping An Bank', 'stock', 1)

        # Insert a price
        db.insert_price(self.connection, 1, '2025-03-01', 10.0, 11.0, 12.0, 9.0, 10.5, 1000, '1d')

        # Check if the price was inserted successfully
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT * FROM {db.Tables.PRICE} WHERE date='2025-03-01';")
        price = cursor.fetchone()

        self.assertIsNotNone(price, "Price should be inserted into the database.")
        self.assertEqual(price[2], '2025-03-01', "Price date should match.")
