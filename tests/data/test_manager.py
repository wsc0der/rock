"""
Test the Manager class.
"""
from unittest.mock import patch
from rock.data import manager
from rock.types import Interval
from .base import DBTestCaseBase

class TestManager(DBTestCaseBase):
    """
    Test the Manager class.
    """

    @patch('rock.data.local_db.get_security', return_value=[{'symbol': 'AAPL'}])
    @patch('rock.data.web_scraper.get_history', return_value=[])
    def test_logs_missing_securities(self, mock_get_history, mock_get_security):
        """Test that missing securities are logged."""
        symbols = ['AAPL', 'GOOG', 'MSFT']

        with self.assertLogs('rock', level='INFO') as log:
            manager.get_history(symbols)

        mock_get_security.assert_called_once_with(symbols)
        mock_get_history.assert_called_once_with(symbols, Interval.ONE_DAY, None, None)

        # Check that the log contains the expected message
        self.assertTrue(any("Missing securities: ['GOOG', 'MSFT']" in message for message in log.output))
