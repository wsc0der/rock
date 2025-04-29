"""
rock.data.manager
"""

from typing import Sequence
import pandas as pd
from rock import types, logger
from . import local_db, web_scraper

def get_history(symboles: Sequence[str],
                interval: types.Interval = types.Interval.ONE_DAY,
                start: str | None = None,   # YYYY-MM-DD
                end: str | None = None      # YYYY-MM-DD
            ) -> Sequence[pd.DataFrame]:
    """
    Get historical data for a list of securities.
    """

    # Get securities from the local database
    securities = local_db.get_security(symboles)

    # Log missing securities
    missing = [s for s in symboles if s not in {item['symbol'] for item in securities}]
    logger.info("Missing securities: %s", missing)

    return web_scraper.get_history(symboles, interval, start, end)
