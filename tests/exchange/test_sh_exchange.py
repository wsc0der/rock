"""
Test cases for sh_exchange module.
"""

import unittest
from pandas import DataFrame
from rock.exchange import sh_exchange as exchange

class TestShExchange(unittest.TestCase):
    """Test cases for sh_exchange module"""

    def test_get_stock_list(self) -> None:
        """Test function."""
        valid_cases = [
            ('1', '2'),
            ('1,2', '2,3'),
            ('', ''),
            ([], []),
            ([1,2], [2]),
            ([exchange.StockType.A], [exchange.StockStatus.NORMAL]),
            ([exchange.StockType.A, exchange.StockType.KECHUANG],
             [exchange.StockStatus.NORMAL, exchange.StockStatus.ST])
        ]

        for case in valid_cases:
            with self.subTest():
                stock_list = exchange.get_stock_list(*case) # type: ignore
                self.assertTrue(
                    isinstance(stock_list, DataFrame),
                    "Stock list should be a DataFrame"
                )

        type_exceptions = [
            ([exchange.StockType.A,], tuple()),
            (tuple(), [exchange.StockStatus.DELISTED])
        ]

        for case in type_exceptions:
            with self.subTest():
                with self.assertRaises(TypeError):
                    exchange.get_stock_list(*case)  # type: ignore

        value_exceptions = [
            (["1"], ["2"]),
            (["1", 1], [2, "3"]),
            ([8, 9], [2, -1])
        ]

        for case in value_exceptions:   # type: ignore
            with self.subTest():
                with self.assertRaises(ValueError):
                    exchange.get_stock_list(*case)  # type: ignore
