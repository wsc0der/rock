"""
This modules provides functions for getting data from SSE.
"""
from io import BytesIO
import requests
import pandas as pd

def get_delistings():
    """
    Fetch delisting data from the Shanghai Stock Exchange (SSE).

    This function retrieves delisting information from the SSE's official website
    by making an HTTP GET request to the specified endpoint. The response is expected
    to be in Excel format, which is then parsed into a Pandas DataFrame.

    Endpoint: https://www.sse.com.cn/assortment/stock/list/delisting/

    Returns:
        pd.DataFrame: A DataFrame containing delisting data.

    Raises:
        ValueError: If the response's Content-Type is not expected".
        requests.exceptions.RequestException: If the HTTP request fails (e.g., timeout, connection error).
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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/97.0.4692.71 Safari/537.36",
    }
    url ='https://query.sse.com.cn//sseQuery/commonExcelDd.do?sqlId=COMMON_SSE_CP_GPJCTPZ_GPLB_ZZGP_L&type=inParams&STOCK_CODE=&REG_PROVINCE=&STOCK_TYPE=1,2&COMPANY_STATUS=3'
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()

    # Check Content-Type
    expect_type = "application/vnd.ms-excel"
    content_type = response.headers.get("Content-Type")
    if expect_type not in content_type:
        raise ValueError(f"Unexpected Content-Type: {content_type}")

    return pd.read_excel(BytesIO(response.content))
