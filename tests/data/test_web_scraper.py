"""
Test cases for web_scraper.py module
"""

import unittest
from pandas import DataFrame
from rock.data import web_scraper
from rock.types import Interval


class TestWebScraper(unittest.TestCase):
    """Test cases for web_scraper.py module"""
    def test_get_history(self):
        """Test get_history function."""

        valid_cases = [
            (['000001'], Interval.ONE_MINUTE, '2025-03-01', '2025-04-02'),
            (['000001'], Interval.ONE_DAY, '2025-03-01'),
            (['000001', '000002'], Interval.ONE_MINUTE, '2025-03-01', '2025-04-02')
        ]

        for case in valid_cases:
            with self.subTest():
                history = web_scraper.get_history(*case)  # type: ignore
                self.assertEqual(len(case[0]), len(history))
                self.assertTrue(all(
                    isinstance(df, DataFrame) for df in history
                ), "All elements in history should be DataFrames")

        invalid_cases = [
            (['000001', ''], Interval.ONE_MINUTE, 1234, 0),
        ]

        for case in invalid_cases:
            with self.subTest():
                history = web_scraper.get_history(*case)
                self.assertEqual(len(case[0]), len(history),
                                "Length of history should match number of symbols")
                for df in history:
                    self.assertTrue(df.empty,
                                    "DataFrame should be empty for invalid cases")
