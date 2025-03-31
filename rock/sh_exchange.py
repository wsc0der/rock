"""
This modules provides functions for getting data from SSE.
"""
from io import BytesIO
import requests
import pandas as pd

def get_delistings() -> pd.DataFrame:
    """
    Fetch delisting data from the Shanghai Stock Exchange (SSE).

    This function retrieves delisting information from the SSE's official website
    by making an HTTP GET request to the specified endpoint. The response is expected
    to be in Excel format, which is then parsed into a Pandas DataFrame.

    https://www.sse.com.cn/assortment/stock/list/delisting/

    Returns:
        pd.DataFrame: A DataFrame containing delisting data.

    Raises:
        ValueError: If the response's Content-Type is not expected".
        requests.exceptions.RequestException:
            If the HTTP request fails (e.g., timeout, connection error).
    """

    response = sse_query((1,2), 3)
    response.raise_for_status()

    # Check Content-Type
    expect_type = "application/vnd.ms-excel"
    content_type = response.headers.get("Content-Type")
    if content_type and expect_type not in content_type:
        raise ValueError(f"Unexpected Content-Type: {content_type}")

    return pd.read_excel(BytesIO(response.content))


def sse_query(stock_type: tuple[int, ...] = (1,), company_status: int = 1) -> requests.Response:
    """
    Fetch data from the Shanghai Stock Exchange (SSE) using a specific SQL query.
    This function constructs a URL with the provided parameters and makes an HTTP GET
    request to the SSE's official website. The function returns the raw response object.
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
    stock_type_str = ','.join(map(str, stock_type))
    url = (f'https://query.sse.com.cn//sseQuery/commonExcelDd.do?'
           f'sqlId=COMMON_SSE_CP_GPJCTPZ_GPLB_ZZGP_L'
           f'&type=inParams&STOCK_CODE=&REG_PROVINCE='
           f'&STOCK_TYPE={stock_type_str}'
           f'&COMPANY_STATUS={company_status}'
    )

    # Make the request
    return requests.get(url, headers=headers, timeout=10)
