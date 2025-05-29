"""
web_scraper.py
This module provides a function to retrieve stock related data.
"""
from collections.abc import Sequence, Mapping
from pandas import DataFrame
from rock.common.types import Interval
from rock.em import utils as em_utils

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
    not_adjusted = em_utils.get_quote_history(
        list(symboles),
        start,
        end,
        klt,
        0,
        True,
        True
    )

    adjusted = em_utils.get_quote_history(
        list(symboles),
        start,
        end,
        klt,
        1,
        True,
        True
    )

    result = {}
    for s in symboles:
        not_adjusted_df = not_adjusted[s].set_index('日期', drop=False)
        adjusted_df = adjusted[s].set_index('日期', drop=False)
        result[s] = DataFrame({
            'name': not_adjusted_df['股票名称'],
            'datetime': not_adjusted_df['日期'],
            'open': not_adjusted_df['开盘'],
            'high': not_adjusted_df['最高'],
            'low': not_adjusted_df['最低'],
            'close': not_adjusted_df['收盘'],
            'adj_close': adjusted_df['收盘'],
            'volume': not_adjusted_df['成交量'],
            'amount': not_adjusted_df['成交额']
        })

    return result
