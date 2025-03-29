"""
Test cases for sh_exchange module.
"""

import unittest
from pandas import DataFrame
from requests.exceptions import HTTPError
from rock import sh_exchange as ex

class TestShExchange(unittest.TestCase):
    """Test cases for sh_exchange module"""

    def test_get_delistings(self):
        """Test function."""
        try:
            delistings = ex.get_delistings()
            self.assertTrue(isinstance(delistings, DataFrame), "Delistings should be a DataFrame")
            self.assertGreater(len(delistings), 0, "Delistings should not be empty")
        except ValueError as e:
            self.fail(f"ValueError raised: {e}")
        except HTTPError as e:
            self.fail(f"HTTPError raised: {e}")
