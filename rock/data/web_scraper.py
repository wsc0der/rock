"""
web_scraper.py
This module provides a function to retrieve stock related data.
"""
from collections.abc import Sequence, Mapping
from pandas import DataFrame
from efinance import stock
from rock.common.types import Interval


INTERVAL_KLT_MAPPING = {
    Interval.ONE_MINUTE: 1,
    Interval.FIVE_MINUTES: 5,
    Interval.FIFTEEN_MINUTES: 15,
    Interval.THIRTY_MINUTES: 30,
    Interval.ONE_HOUR: 60,
    Interval.ONE_DAY: 101,
    Interval.ONE_WEEK: 102,
    Interval.ONE_MONTH: 103
}


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
    start = str(start).replace('-', '') if start is not None else '19000101'
    end = str(end).replace('-', '') if end is not None else '20500101'

    klt = INTERVAL_KLT_MAPPING.get(interval, 101)
    return stock.get_quote_history(
        list(symboles),
        start,
        end,
        klt,
        0,
        None,
        True,
        True
    )
