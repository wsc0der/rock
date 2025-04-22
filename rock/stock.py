"""
rock/stock.py
This module provides a function to retrieve stock related data.
"""

from collections.abc import Sequence
from pandas import DataFrame
from rock.types import Interval
from rock.data import web_scraper


def get_history(symboles: Sequence[str],
                interval: Interval = Interval.ONE_DAY,
                start: str | None = None,   # YYYY-MM-DD
                end: str | None = None      # YYYY-MM-DD
            ) -> Sequence[DataFrame]:
    """
    Retrieve historical stock data for the given symbols.
    Args:
        symboles (Sequence[str]): List of stock symbols.
        interval (Interval): The interval for the data.
        start (str | None): The start date in YYYY-MM-DD format.
        end (str | None): The end date in YYYY-MM-DD format.
    Returns:
        Sequence[DataFrame]: A list of DataFrames containing historical data for each symbol.
    """
    return web_scraper.get_history(
        symboles,
        interval,
        start,
        end
    )
