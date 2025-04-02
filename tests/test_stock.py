"""
Test stock.py
"""

import unittest
from pandas import DataFrame
from rock import stock

class TestStock(unittest.TestCase):
    """Test cases for stock.py module"""

    def test_get_history(self) -> None:
        """Test get_history function."""
        valid_cases = [
            (['000001'], stock.Interval.ONE_MINUTE, '2025-03-01', '2025-04-02'),
            (['000001'], stock.Interval.ONE_DAY, '2025-03-01'),
            (['000001', '000002'], stock.Interval.ONE_MINUTE, '2025-03-01', '2025-04-02')
        ]

        for case in valid_cases:
            with self.subTest():
                history = stock.get_history(*case)  # type: ignore
                self.assertEqual(len(case[0]), len(history))
                self.assertTrue(all(
                    isinstance(df, DataFrame) for df in history
                ), "All elements in history should be DataFrames")
