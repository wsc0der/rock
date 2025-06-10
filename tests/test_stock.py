"""
Test stock.py
"""

#pylint: disable=line-too-long

from unittest import TestCase
from unittest.mock import patch
from rock.common import utils
from rock import stock

class TestStock(TestCase):
    """Test cases for stock.py module"""

    @patch('rock.data.db.get_history', return_value={
        '000001': [
            {'datetime': utils.str_to_dt('2023-10-01'), 'open': 10, 'high': 12, 'low': 9, 'close': 11, 'volume': 1000, 'security_id': 1, 'frequency': 'daily'},
            {'datetime': utils.str_to_dt('2023-10-02'), 'open': 11, 'high': 13, 'low': 10, 'close': 12, 'volume': 1500, 'security_id': 1, 'frequency': 'daily'}
        ],
        '000002': [
            {'datetime': utils.str_to_dt('2023-10-01'), 'open': 20, 'high': 22, 'low': 19, 'close': 21, 'volume': 2000, 'security_id': 2, 'frequency': 'daily'},
        ]
    })
    def test_get_history(self, mock_get_history) -> None:
        """Test get_history function."""
        data = stock.get_history(['000001', '000002'], start='2023-10-01', end='2023-10-02')
        mock_get_history.assert_called_once_with(['000001', '000002'], '2023-10-01', '2023-10-02')

        self.assertIn('000001', data)
        self.assertIn('000002', data)
        df1 = data['000001']
        df2 = data['000002']
        self.assertEqual(len(df1), 2)
        self.assertEqual(len(df2), 1)


    def test_get_securities(self) -> None:
        """Test get_securities function."""
        with patch('rock.data.db.get_all_securities', return_value=[
            {'id': 1, 'symbol': '000001', 'name': 'Stock A', 'type': 'stock', 'listing': None, 'delisting': None, 'exchange_id': 1},
            {'id': 2, 'symbol': '000002', 'name': 'Stock B', 'type': 'stock', 'listing': None, 'delisting': None, 'exchange_id': 1}
        ]):
            securities = stock.get_securities()
            self.assertEqual(len(securities), 2)
            self.assertIn('000001', securities)
            self.assertIn('000002', securities)
