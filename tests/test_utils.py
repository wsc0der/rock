"""Test cases for the utils module."""

import sqlite3
import unittest
from rock import utils

class TestUtils(unittest.TestCase):
    """Test cases for the utils module"""

    def test_get_sql_version(self):
        """Test the get_sql_version function."""
        self.assertEqual(utils.get_sql_version(), sqlite3.sqlite_version)
