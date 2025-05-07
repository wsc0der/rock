"""
rock/stock.py
This module provides a function to retrieve stock related data.
"""

from collections.abc import Sequence, Mapping
from pandas import DataFrame
from rock.types import Interval
from rock.data import web_scraper, local_db
from rock import logger


def get_history(symboles: Sequence[str],
                interval: Interval = Interval.ONE_DAY,
                start: str | None = None,   # YYYY-MM-DD
                end: str | None = None      # YYYY-MM-DD
            ) -> Mapping[str, DataFrame]:
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
    securities = local_db.get_security(symboles)

    # Log missing securities
    missing = [s for s in symboles if s not in {item['symbol'] for item in securities}]
    logger.info("Missing securities: %s", missing)

    return web_scraper.get_history(symboles, interval, start, end)
