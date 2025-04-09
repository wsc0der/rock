"""
rock.types
This module defines types and enums used in the rock package.
"""
from enum import StrEnum

class Interval(StrEnum):
    """
    Enum for intervals used in stock data retrieval.
    """
    ONE_MINUTE = "1m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    THIRTY_MINUTES = "30m"
    ONE_HOUR = "1h"
    ONE_DAY = "1d"
    ONE_WEEK = "1wk"
    ONE_MONTH = "1mo"
