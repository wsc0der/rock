"""
This modules provides functions for getting data from SSE.
"""
from io import BytesIO
import requests
import pandas as pd

def get_delistings():
    """
    SSE delistings.
    https://www.sse.com.cn/assortment/stock/list/delisting/
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

    print(response.headers.get("Content-Type"))
    print(response.headers.get("Content-Disposition"))

    # Check Content-Type
    content_type = response.headers.get("Content-Type")
    print(f"Content-Type: {content_type}")

    if "application/vnd.ms-excel" in content_type:
        df = pd.read_excel(BytesIO(response.content))
        print(df.head())

    return response.content
