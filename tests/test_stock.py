"""
Test stock.py
"""

import unittest
from unittest.mock import patch
from rock.types import Interval
from rock import stock

class TestStock(unittest.TestCase):
    """Test cases for stock.py module"""

    def test_get_history(self) -> None:
        """Test get_history function."""

    @patch('rock.data.db.get_security', return_value=[{'symbol': 'AAPL'}])
    @patch('rock.data.web_scraper.get_history', return_value={})
    def test_logs_missing_securities(self, mock_get_history, mock_get_security):
        """Test that missing securities are logged."""
        symbols = ['AAPL', 'GOOG', 'MSFT']

        with self.assertLogs('rock', level='INFO') as log:
            stock.get_history(symbols)

        mock_get_security.assert_called_once_with(symbols)
        mock_get_history.assert_called_once_with(symbols, Interval.ONE_DAY, None, None)

        # Check that the log contains the expected message
        self.assertTrue(any("Missing securities: ['GOOG', 'MSFT']" in message for message in log.output))
