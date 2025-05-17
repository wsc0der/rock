"""
Test cases for web_scraper.py module
"""

import unittest
from rock.data import web_scraper
from rock.common.types import Interval


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
                self.assertTrue(all(s in history.keys() for s in case[0]),
                                "All symbols should be present in the history")
                self.assertTrue(all(not history[s].empty for s in case[0]),
                                "All symbols should have non-empty history")

        invalid_cases = [
            (['000001', ''], Interval.ONE_MINUTE, 1234, 0),
        ]

        for case in invalid_cases:
            with self.subTest():
                history = web_scraper.get_history(*case)
                self.assertTrue(all(s in history.keys() and history[s].empty for s in case[0]),
                                "Invalid cases should return empty history")
