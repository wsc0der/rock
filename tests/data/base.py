"""Test the Manager class."""
import unittest
import os
from rock.data import db

class DBTestCaseBase(unittest.TestCase):
    """
    Base class for database test cases.
    """

    def setUp(self):
        db.init()
        self.connection = db.get_connection()
        return super().setUp()

    def tearDown(self):
        # Close the database connection if it's open
        try:
            self.connection.close()
        except AttributeError:
            pass

        if os.path.exists(db.DB_PATH):
            os.remove(db.DB_PATH)
            os.rmdir(os.path.dirname(db.DB_PATH))
        return super().tearDown()
