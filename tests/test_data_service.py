"""Test cases for the data_service module."""

import unittest
from unittest.mock import patch
import os
import pandas as pd
from rock import data_service
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

    @patch('rock.data.db.bulk_insert_history')
    @patch('rock.data.db.get_all_securities')
    @patch('rock.data.web_scraper.get_history')
    def test_update_histories(self, mock_get_history, mock_get_all_securities, mock_bulk_insert_history):
        """Test the update_histories function."""
        mock_get_history.return_value = {
            '000001': pd.DataFrame({'日期': '2025-03-01', '开盘': 1, '收盘': 2, '最高': 3, '最低': 0, '成交量': 10}, index=[0]),
            '000002': pd.DataFrame({'日期': '2025-03-01', '开盘': 1, '收盘': 2, '最高': 3, '最低': 0, '成交量': 10}, index=[0])
        }
        mock_get_all_securities.return_value = [
            {'symbol': '000001', 'exchange_id': 1, 'id': 1},
            {'symbol': '000002', 'exchange_id': 1, 'id': 2}
        ]
        data_service.update_histories()
        self.assertEqual(mock_bulk_insert_history.call_count, 2)
