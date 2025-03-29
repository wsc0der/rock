"""
Test cases for sh_exchange module.
"""

import unittest
from rock import sh_exchange as ex

class TestShExchange(unittest.TestCase):
    """Test cases for sh_exchange module"""

    def test_get_delistings(self):
        """Test function."""
        delistings = ex.get_delistings()
        self.assertGreater(len(delistings), 0, "Delistings should not be empty")
