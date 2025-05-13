"""Test cases for the data_service module."""

import unittest
import os
import pkgutil
import importlib
from rock import data_service, exchange
from rock.data import db


class TestDataService(unittest.TestCase):
    """Test cases for data_service module"""

    def setUp(self):
        db.create_db()
        self.connection = db.get_connection()
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

    def test_initialize(self):
        """Test the initialize function."""
        data_service.init_db()

        for module in data_service.get_exchange_modules():
            with self.subTest(module=module):
                # Check if the exchange is inserted into the database
                cursor = self.connection.cursor()
                cursor.execute(f'''
                    SELECT * FROM {db.Tables.EXCHANGE}
                    WHERE name = ? AND acronym = ? AND type = ?
                ''', module.METADATA)
                result = cursor.fetchone()
                self.assertIsNotNone(result, f"Exchange {module.METADATA.name} not found in the database.")
                cursor.close()

    def test_update_securities(self):
        """Test the update_securities function."""
        data_service.init_db()
        data_service.update_securities()

        for module in data_service.get_exchange_modules():
            with self.subTest(module=module):
                # Check if the securities are inserted into the database
                cursor = self.connection.cursor()
                cursor.execute(f'''
                    SELECT * FROM {db.Tables.SECURITY}
                    WHERE exchange_id = ?
                ''', (db.get_exchange_id(module.METADATA.acronym),))
                result = cursor.fetchall()
                self.assertGreater(len(result), 0, f"No securities found for exchange {module.METADATA.name}.")
                cursor.close()
