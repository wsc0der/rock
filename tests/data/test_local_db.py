"""
test_db.py
"""

from collections.abc import Sequence
from sqlite3 import IntegrityError, Row
from datetime import datetime as dt
from rock.data import local_db
from .base import DBTestCaseBase

class TestLocal(DBTestCaseBase):
    """Test cases for the database module."""

    def test_insert_exchange(self):
        """Test inserting an exchange into the database."""
        # Insert an exchange
        local_db.insert_exchange('Shanghai Stock Exchange', 'SSE', 'stock')

        # Check if the exchange was inserted successfully
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT * FROM {local_db.Tables.EXCHANGE} WHERE acronym='SSE';")
        exchange = cursor.fetchone()
        cursor.close()

        self.assertIsNotNone(exchange, "Exchange should be inserted into the database.")
        self.assertEqual(exchange[1], 'Shanghai Stock Exchange', "Exchange name should match.")

        # Test invalid cases
        invalid_cases = [
            (None, 'NNM', 'stock'),                     # name is None
            ('', 'EPT', 'stock'),                       # name is empty
            ('None Acronym Exchange', None, 'stock'),   # acronym is None
            ('Empty Acronym Exchange', '', 'stock'),    # acronym is empty
            ('Shanghai Stock Exchange', 'ABC', 'stock'),    # name is not unique
            ('Another Stock Exchange',  'SSE', 'stock'),    # acronym is not unique
            ('Fake Stock Exchange', 'FSE', 'invalid_type'), # type is invalid
            ('Fake Stock Exchange', 'FSE', ''),             # type is empty
            ('Fake Stock Exchange', 'FSE', None),           # type is None
        ]

        for case in invalid_cases:
            with self.subTest():
                with self.assertRaises(IntegrityError, msg=case):
                    local_db.insert_exchange(*case)

    def test_insert_security(self):
        """Test inserting a security into the database."""
        # Insert an exchange first
        local_db.insert_exchange('Shanghai Stock Exchange', 'SSE', 'stock')

        # Insert a security
        local_db.insert_security('000001', 'Ping An Bank', 'stock', 1)

        # Check if the security was inserted successfully
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT * FROM {local_db.Tables.SECURITY} WHERE symbol='000001';")
        security = cursor.fetchone()
        cursor.close()

        self.assertIsNotNone(security, "Security should be inserted into the database.")
        self.assertEqual(security[1], '000001', "Security symbol should match.")
        self.assertEqual(security[2], 'Ping An Bank', "Security name should match.")
        self.assertEqual(security[3], 'stock', "Security type should match.")

        # Test invalid cases
        invalid_cases = [
            (None, 'None Company', 'stock', 1),     # symbol is None
            ('', 'Empty Company', 'stock', 1),      # symbol is empty
            ('000002', None, 'stock', 1),           # name is None
            ('000003', '', 'stock', 1),             # name is empty
            ('000001', 'Another Company', 'stock', 1),  # symbol is not unique
            ('000002', 'Ping An Bank', 'stock', 1),     # name is not unique
            ('000003', 'Invalid Company', 'invalid_type', 1),    # type is invalid
            ('000004', 'Second Invalid Company', 'stock', 999)     # exchange_id is not valid
        ]

        for case in invalid_cases:
            with self.subTest():
                with self.assertRaises(IntegrityError, msg=case):
                    local_db.insert_security(*case)

    def test_insert_history(self):
        """Test inserting a history into the database."""
        # Insert an exchange first
        local_db.insert_exchange('Shanghai Stock Exchange', 'SSE', 'stock')

        # Insert a security first
        local_db.insert_security('000001', 'Ping An Bank', 'stock', 1)

        # Insert history
        valid_cases = [
            (1, '2025-03-01', 10.0, 11.0, 12.0, 9.0, 10.5, 1000, '1d'),
            (1, '2025-03-01 10:30:12', 10.0, 11.0, 12.0, 9.0, 10.5, 1000, '1d')
        ]
        for case in valid_cases:
            local_db.insert_history(*case)

        # Check if the history was inserted successfully
        cursor = self.connection.cursor()
        timestamp = dt.fromisoformat(valid_cases[0][1]).timestamp()
        cursor.execute(f"SELECT * FROM {local_db.Tables.HISTORY} WHERE datetime={timestamp};")
        history = cursor.fetchone()
        cursor.execute(f'SELECT history_updated_at FROM {local_db.Tables.SECURITY} WHERE id={valid_cases[0][0]};')
        history_updated_at = cursor.fetchone()
        cursor.close()

        self.assertIsNotNone(history, "History should be inserted into the database.")
        self.assertIsInstance(history['datetime'], dt, "History datetime should be a datetime object.")
        self.assertTrue(history_updated_at[0] <= dt.now(), msg="Security history_updated_at should be updated.")

        # Test invalid cases
        invalid_cases = [
            (IntegrityError, None, '2025-03-01', 10.0, 11.0, 12.0, 9.0, 10.5, 1000, '1d'),  # security_id is None
            (TypeError, 0, None, 10.0, 11.0, 12.0, 9.0, 10.5, 1000, '1d'),                  # date is None
            (ValueError, 1, '', 10.0, 11.0, 12.0, 9.0, 10.5, 1000, '1d'),                   # date is empty
            (ValueError, 1, 'abcd', 10.0, 11.0, 12.0, 9.0, 10.5, 1000, '1d'),               # date not in valid format
            (TypeError, 1, 999, 10.0, 11.0, 12.0, 9.0, 10.5, 1000, '1d'),                   # date is not a string
            (IntegrityError, 0, '2025-03-01', 10.0, 11.0, 12.0, 9.0, 10.5, 1000, '1d'),     # security_id is invalid
            (IntegrityError, 999, '2025-03-01', 10.0, 11.0, 12.0, 9.0, 10.5, 1000, '1d'),   # security_id is invalid
            (IntegrityError, 1, '2025-03-01', 10.0, 11.0, 12.0, 9.0, 10.5, 1000, '1d'),     # primary key is duplicated
            (IntegrityError, 1, '2025-03-02', None, 11.0, 12.0, 9.0, 10.5, 1000, '1d'),     # open_price is None
            (IntegrityError, 1, '2025-03-02', 10.0, None, 12.0, 9.0, 10.5, 1000, '1d'),     # close_price is None
            (IntegrityError, 1, '2025-03-02', 10.0, 11.0, None, 9.0, 10.5, 1000, '1d'),     # high_price is None
            (IntegrityError, 1, '2025-03-02', 10.0, 11.0, 12.0, None, 10.5, 1000, '1d'),    # low_price is None
            (IntegrityError, 1, '2025-03-02', 10.0, 11.0, 12.0, 9.0, None, 1000, '1d'),     # adj_close is None
            (IntegrityError, 1, '2025-03-02', 10.0, 11.0, 12.0, 9.0, 10.5, None, '1d'),     # volume is None
            (IntegrityError, 1, '2025-03-02', 10.0, 11.0, 12.0, 9.0, 10.5, -1000, '1d'),    # volume is negative
            (IntegrityError, 1, '2025-03-02', 10.0, 11.0, 12.0, 9.0, 10.5, 1000, None),     # frequency is None
            (IntegrityError, 1, '2025-03-02', 10.0, 11.0, 12.0, 9.0, 10.5, 1000, ''),       # frequency is empty
            (IntegrityError, 1, '2025-03-02', 10.0, 11.0, 12.0, 9.0, 10.5, 1000, 'invalid') # frequency is invalid
        ]

        for case in invalid_cases:
            with self.subTest():
                with self.assertRaises(case[0], msg=case):
                    local_db.insert_history(*case[1:])

    def test_bulk_insert_history(self):
        """Test inserting multiple history into the database."""
        # Insert an exchange first
        local_db.insert_exchange('Shanghai Stock Exchange', 'SSE', 'stock')

        # Insert a security first
        local_db.insert_security('000001', 'Ping An Bank', 'stock', 1)

        # Insert multiple history
        histories = [
            (1, '2025-03-01', 10.0, 11.0, 12.0, 9.0, 10.5, 1000, '1d'),
            (1, '2025-03-02', 11.0, 12.0, 13.0, 10.0, 11.5, 2000, '1d')
        ]
        local_db.bulk_insert_history(histories)

        # Check if the histories were inserted successfully
        cursor = self.connection.cursor()
        cursor.execute(f'''
            SELECT * FROM {local_db.Tables.HISTORY} WHERE datetime={dt.fromisoformat('2025-03-01').timestamp()};
        ''')
        history_1 = cursor.fetchone()

        cursor.execute(f'''
            SELECT * FROM {local_db.Tables.HISTORY} WHERE datetime={dt.fromisoformat('2025-03-02').timestamp()};
        ''')
        history_2 = cursor.fetchone()

        self.assertTrue(history_1 is not None and history_2 is not None,
                        "Histories should be inserted into the database.")

        cursor.execute(f'SELECT history_updated_at FROM {local_db.Tables.SECURITY} WHERE id={histories[0][0]};')
        history_updated_at = cursor.fetchone()
        self.assertTrue(history_updated_at[0] <= dt.now(), msg="Security history_updated_at should be updated.")

        # Test invalid case
        invalid_case = [
            (1, '2025-03-03', 10.0, 11.0, 12.0, 9.0, 10.5, 1000, '1d'),
            (2, '2025-03-03', 11.0, 12.0, 13.0, 10.0, 11.5, 2000, '1d'),
        ]

        with self.assertRaises(IntegrityError):
            local_db.bulk_insert_history(invalid_case)

        # Check if the database is still consistent
        cursor.execute(f"SELECT * FROM {local_db.Tables.HISTORY} WHERE datetime='2025-03-03';")
        history_3 = cursor.fetchall()
        self.assertEqual(len(history_3), 0, "No history should be inserted after an error.")

        # Test invalid datetime
        invalid_datetime = [
            (1, '', 10.0, 11.0, 12.0, 9.0, 10.5, 1000, '1d'),   # date is empty
            (1, '2025-03-03', 10.0, 11.0, 12.0, 9.0, 10.5, 1000, '1d'), # frequency is None
        ]

        with self.assertRaises(ValueError):
            local_db.bulk_insert_history(invalid_datetime)
        cursor.close()

    def test_get_security(self):
        """Test getting a security from the database."""
        # Insert an exchange first
        local_db.insert_exchange('Shanghai Stock Exchange', 'SSE', 'stock')

        # Insert a security first
        securities = [
            ('000001', 'Ping An Bank', 'stock', 1),
            ('000002', 'Another company', 'stock', 1),
            ('000003', 'Another company 2', 'stock', 1),
            ('000004', 'Another company 3', 'stock', 1)
        ]
        for s in securities:
            local_db.insert_security(*s)

        # Test single case
        test_security = securities[0]
        result = local_db.get_security(test_security[0])

        self.assertIsNotNone(result, "Security should be retrieved from the database.")
        self.assertTrue(isinstance(result, Sequence), "Security should be returned as a Sequence object.")
        self.assertTrue(result[0]['symbol' == test_security[0]]
            and result[0]['name'] == test_security[1]
            and result[0]['type'] == test_security[2], "Security data should match.")

        # Test multiple cases
        result = local_db.get_security([item[0] for item in securities])
        self.assertTrue(isinstance(result, Sequence), "Security should be returned as a Sequence object.")
        self.assertTrue(len(result) == len(securities), "Number of securities should match.")
        for r, s in zip(result, securities):
            self.assertTrue(isinstance(r, Row), "Each item should be a Row object.")
            self.assertTrue(r['symbol'] == s[0] and r['name'] == s[1] and r['type'] == s[2],
                "Security data should match.")

        # Test invalid case
        invalid_cases = [
            None,  # symbol is None
            '',    # symbol is empty
            'abcdefg'  # symbol does not exist
        ]
        results = local_db.get_security(invalid_cases)
        self.assertTrue(len(results) == 0, "Security should not be found in the database.")

        # Test partial valid cases
        partial_cases = ['000001', '000002', 'invalid symbol']
        results = local_db.get_security(partial_cases)
        self.assertTrue(len(results) == 2, "Only valid securities should be retrieved.")
