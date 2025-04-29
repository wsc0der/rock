"""Test the Manager class."""
import unittest
import os
from rock.data import local_db

class DBTestCaseBase(unittest.TestCase):
    """
    Base class for database test cases.
    """

    def setUp(self):
        local_db.init()
        self.connection = local_db.get_connection()
        return super().setUp()

    def tearDown(self):
        # Close the database connection if it's open
        try:
            self.connection.close()
        except AttributeError:
            pass

        if os.path.exists(local_db.DB_PATH):
            os.remove(local_db.DB_PATH)
            os.rmdir(os.path.dirname(local_db.DB_PATH))
        return super().tearDown()
