"""
rock/stock.py
This module provides a function to retrieve stock related data.
"""

from collections.abc import Sequence, Mapping
from datetime import datetime as dt
import pandas as pd
from rock.types import Interval
from rock.data import db, web_scraper
from rock import utils
from rock import logger


def get_history(symboles: Sequence[str],
                start: str | None = None,   # YYYY-MM-DD
                end: str | None = None      # YYYY-MM-DD
            ) -> Mapping[str, pd.DataFrame]:
    """
    Retrieve historical stock data for the given symbols.
    Args:
        symboles (Sequence[str]): List of stock symbols.
        interval (Interval): The interval for the data.
        start (str | None): The start date in YYYY-MM-DD format.
        end (str | None): The end date in YYYY-MM-DD format.
    Returns:
        Mapping[str, DataFrame]: A dictionary of DataFrames containing historical data for each symbol.
    """
    # Get securities from the local database
    # securities = db.get_security(symboles)

    # # Log missing securities
    # missing = [s for s in symboles if s not in {item['symbol'] for item in securities}]
    # logger.info("Missing securities: %s", missing)

    if start is None:
        start = utils.get_epoch_date()
    if end is None:
        end = utils.get_current_date()

    histories = db.get_history(symboles, start, end)

    def get_date_discrepency(desired: tuple[str, str], actual: tuple[str, str]) -> list[tuple[str, str]]:
        """
        Return a list of discrepancies between desired and actual dates.
        """
        discrepencies = []

        # Compare start dates
        start_result = utils.compare_dates(desired[0], actual[0])
        if start_result == 1:
            raise ValueError(f"Start date {desired[0]} is after actual start date {actual[0]}")
        elif start_result -1:
            discrepencies.append((desired[0], actual[0]))

        # Compare end dates
        end_result = utils.compare_dates(desired[1], actual[1])
        if end_result == 1:
            discrepencies.append((desired[1], actual[1]))
        elif end_result == -1:
            raise ValueError(f"End date {desired[1]} is before actual end date {actual[1]}")

        return discrepencies

    result = {}
    missing = []
    for s, h in histories.items():
        if len(h):
            history = pd.DataFrame(h).sort_values(by='datetime', key=dt.fromisoformat)
            security = db.get_security(s)[0]
            if not history.empty:
                discrepencies = get_date_discrepency((start, end),
                                    (history.iloc[0]['datetime'], history.iloc[-1]['datetime']))
            else:
                discrepencies = [(start, end)]

            for d in discrepencies:
                data = web_scraper.get_history(s, Interval.ONE_DAY, d[0], d[1])
                data['security_id'] = security['id']
                history = pd.concat([history, data], ignore_index=True)
                rows = [(
                    row['security_id'],
                    row['datetime'],
                    row['open'],
                    row['close'],
                    row['high'],
                    row['low'],
                    row['adj_close'],
                    row['volume'],
                    row['frequency']
                ) for row in data.to_dict(orient='records')]
                db.bulk_insert_history(rows)
            result[s] = history.sort_values(by='datetime', key=dt.fromisoformat)
        else:
            missing.append(s)

        if len(missing):
            logger.info("Missing securities: %s", missing)

    return result
