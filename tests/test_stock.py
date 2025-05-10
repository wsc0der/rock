"""
Test stock.py
"""

from unittest import TestCase

class TestStock(TestCase):
    """Test cases for stock.py module"""

    def test_get_history(self) -> None:
        """Test get_history function."""

    # @patch('rock.data.db.get_security', return_value=[{'symbol': 'AAPL'}])
    # def test_logs_missing_securities(self, mock_get_security):
    #     """Test that missing securities are logged."""
    #     symbols = ['AAPL', 'GOOG', 'MSFT']

    #     with self.assertLogs('rock', level='INFO') as log:
    #         stock.get_history(symbols)

    #     mock_get_security.assert_called_once_with(symbols)

    #     # Check that the log contains the expected message
    #     self.assertTrue(any("Missing securities: ['GOOG', 'MSFT']" in message for message in log.output))
