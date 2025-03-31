"""
Test cases for sh_exchange module.
"""

import unittest
from pandas import DataFrame
from requests.exceptions import HTTPError
from requests import Response
from rock import sh_exchange

class TestShExchange(unittest.TestCase):
    """Test cases for sh_exchange module"""

    def test_get_delistings(self) -> None:
        """Test function."""
        try:
            delistings = sh_exchange.get_delistings()
            self.assertTrue(isinstance(delistings, DataFrame), "Delistings should be a DataFrame")
            self.assertGreater(len(delistings), 0, "Delistings should not be empty")
        except ValueError as e:
            self.fail(f"ValueError raised: {e}")
        except HTTPError as e:
            self.fail(f"HTTPError raised: {e}")

    def test_sse_query(self) -> None:
        """Test function."""
        response = sh_exchange.sse_query((1,), 3)
        self.assertTrue(isinstance(response, Response), "Response should be a Response object")
