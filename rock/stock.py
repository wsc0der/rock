"""
rock/stock.py
This module provides a function to retrieve stock related data.
"""

from collections.abc import Sequence, Mapping
import pandas as pd
from rock.data import db
from rock.common import utils
from rock.logger import logger


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
    if start is None:
        start = utils.get_epoch_date()
    if end is None:
        end = utils.get_current_date()

    histories = db.get_history(list(symboles), start, end)

    result = {}
    for s, h in histories.items():
        if h is None:
            logger.warning("No history found for %s", s)
            continue
        # Convert to DataFrame
        df = pd.DataFrame([dict(row) for row in h])
        # Drop columns
        df.drop(columns=['security_id', 'frequency'], inplace=True)
        # Sort by date
        df.sort_values(by='datetime', inplace=True)
        # Add to result
        result[s] = df
    return result
