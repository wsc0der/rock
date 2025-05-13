"""

This modules provides functions for getting data from SSE.
"""
from io import BytesIO
from enum import IntEnum, StrEnum
from typing import Type, TypeVar, Sequence
import requests
import pandas as pd
from .common import ExchangeMeta, StockMeta


METADATA = ExchangeMeta(
    name='Shanghai Stock Exchange',
    acronym='SSE',
    type='stock'
)


class StockType(IntEnum):
    """
    Enum for stock types on the Shanghai Stock Exchange (SSE).
    """
    A = 1
    B = 2
    KECHUANG = 8


class StockStatus(IntEnum):
    """
    Enum for company status on the Shanghai Stock Exchange (SSE).
    """
    NORMAL = 2
    DELISTED = 3
    ST = 7


class ContentType(StrEnum):
    """
    Enum for raw data types returned by querying SSE endpoints.
    """
    EXCEL = "application/vnd.ms-excel"


def get_a_shares() -> Sequence[StockMeta]:
    """Get A shares from the Shanghai Stock Exchange (SSE)."""
    d = get_stock_list([StockType.A], [StockStatus.NORMAL, StockStatus.ST])
    return [
        StockMeta(
            symbol=row['原公司代码'],
            name=row['原公司简称'],
            listing=str(row['上市日期']),
            delisting=str(row['终止上市日期']) if row['终止上市日期'] != '-' else None
        )
        for _, row in d.iterrows()
    ]


def get_stock_list(types_in: str|list[StockType], status_in: str|list[StockStatus]) -> pd.DataFrame:
    """
    Get stock list from the Shanghai Stock Exchange (SSE) based on stock types and statuses.
    This function fetches stock data from the SSE using Http request and returns
    the data as a Pandas DataFrame. The function supports filtering by stock types and
    statuses using the provided parameters.
    Resources: https://www.sse.com.cn/assortment/stock/list/share/
    Args:
        types_in (str|list[StockType]): A string or list of stock types to filter the query.
        status_in (str|list[StockStatus]): A string or list of stock statuses to filter the query.
    Returns:
        pd.DataFrame: A DataFrame containing the stock data.
    Raises:
        TypeError: If types_in or status_in are not of the expected types.
        ValueError: If the input values are invalid or if the response content is not as expected.
    """

    T = TypeVar('T', bound='IntEnum')
    def process_input(ipt: str|list[T], cls: Type[T]) -> list[T]:
        try:
            if isinstance(ipt, str):
                return list(map(cls, (map(int, ipt.replace(',', ' ').split()))))
            return list(map(cls, ipt))
        except ValueError as e:
            raise ValueError(
                f"Invalid input: {ipt}. Expected a string or list of {cls.__name__} values.") from e

    if not isinstance(types_in, (str, list)):
        raise TypeError("types_in must be a string or a list of StockType")
    if not isinstance(status_in, (str, list)):
        raise TypeError("status_in must be a string or a list of StockStatus")

    types = process_input(types_in, StockType)
    status = process_input(status_in, StockStatus)

    response = _query(','.join(map(str, types)), ','.join(map(str, status)))
    response.raise_for_status()

    return _get_data(response, ContentType.EXCEL)


def _get_data(response: requests.Response, expected_type: ContentType) -> pd.DataFrame:
    """
    Parse the response content based on the expected type.
    This function checks the Content-Type of the response and raises an error if it
    does not match the expected type. If the Content-Type is correct, it parses the
    content into a Pandas DataFrame.
    Args:
        response (requests.Response): The HTTP response object.
        expected_type (ContentType): The expected Content-Type of the response.
    Returns:
        pd.DataFrame: A DataFrame containing the parsed data.
    Raises:
        ValueError: If content doesn't exist, the response's Content-Type does not
            match the expected type or the expected_type is unsupported.
    """

    # Check Content-Type
    content_type = response.headers.get("Content-Type")
    if content_type is None:
        raise ValueError("Content-Type header is missing in the response")
    if expected_type not in content_type:
        raise ValueError(f"Unexpected Content-Type: {content_type}")

    # Parse the response content based on the expected type
    result = None
    match expected_type:
        case ContentType.EXCEL:
            result = pd.read_excel(BytesIO(response.content))
        case _:
            raise ValueError(f"Unsupported Content-Type: {expected_type}")

    return result


def _query(types: str, status: str) -> requests.Response:
    """
    Fetch data from the Shanghai Stock Exchange (SSE) using a specific SQL query.
    This function constructs a URL with the provided parameters and makes an HTTP GET
    request to the SSE's official website. The function returns the raw response object.
    Args:
        types (str): A comma seperated int string representing the stock types to filter the query.
        status (str): A comma seperated int string representing the stock statuses to filter the query.
    Returns:
        requests.Response: The raw response object from the HTTP GET request.
    Raises:
        HTTPError: If the request to the SSE fails.
    """
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Host": "query.sse.com.cn",
        "Pragma": "no-cache",
        "Referer": "https://www.sse.com.cn/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
    }

    url = (f'https://query.sse.com.cn//sseQuery/commonExcelDd.do?'
           f'sqlId=COMMON_SSE_CP_GPJCTPZ_GPLB_ZZGP_L'
           f'&type=inParams&STOCK_CODE=&REG_PROVINCE='
           f'&STOCK_TYPE={types}'
           f'&COMPANY_STATUS={status}'
    )

    # Make the request
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    return response
