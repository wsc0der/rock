"""
@file util.py
This module provides utility functions for handling stock quotes and market data.
"""
import json
import re
import time
from collections import namedtuple
from collections.abc import Callable
from functools import wraps
from typing import List, TypeVar, ParamSpec, Dict
import requests
from requests.adapters import HTTPAdapter
from jsonpath import jsonpath

import pandas as pd
from retry.api import retry
import multitasking
from tqdm.auto import tqdm

from rock.em.cache import em_cache
import rock.config as config

multitasking.set_max_threads(8)
MAX_CONNECTIONS = 10

ADDRESS = f"http://{config.USERNAME}:{config.PASSWORD}@{config.PROXY}:{config.PORT}/"
proxies={
    "http": ADDRESS,
    "https": ADDRESS
}

EASTMONEY_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; Touch; rv:11.0) like Gecko",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    # 'Referer': 'http://quote.eastmoney.com/center/gridlist.html',
}


EASTMONEY_KLINE_FIELDS = {
    "f51": "日期",
    "f52": "开盘",
    "f53": "收盘",
    "f54": "最高",
    "f55": "最低",
    "f56": "成交量",
    "f57": "成交额",
    "f58": "振幅",
    "f59": "涨跌幅",
    "f60": "涨跌额",
    "f61": "换手率",
}


class CustomedSession(requests.Session):
    """
    Custom session class to set a default timeout for requests.
    """
    def request(self, *args, **kwargs):
        kwargs.setdefault("timeout", 180)  # 3min
        return super().request(*args, **kwargs)


session = CustomedSession()
adapter = HTTPAdapter(
    pool_connections=MAX_CONNECTIONS, pool_maxsize=MAX_CONNECTIONS, max_retries=5
)
session.mount("http://", adapter)
session.mount("https://", adapter)

T = TypeVar("T")
P = ParamSpec("P")

def to_numeric(func: Callable[P, T]) -> Callable[P, T]:
    """
    Convert DataFrame values to numeric types where possible.
    """
    ignore = [
        "股票代码",
        "基金代码",
        "代码",
        "市场类型",
        "市场编号",
        "债券代码",
        "行情ID",
        "正股代码",
    ]

    def convert(o: str|int|float) -> str|float|int:
        if not re.findall(r"\d", str(o)):
            return o
        try:
            if str(o).isalnum():
                o = int(o)
            else:
                o = float(o)
        except ValueError:
            pass
        return o

    @wraps(func)
    def run(*args: P.args, **kwargs: P.kwargs) -> T:
        values = func(*args, **kwargs)
        if isinstance(values, pd.DataFrame):
            for column in values.columns:
                if column not in ignore:
                    values[column] = values[column].apply(convert)
        return values

    return run


Quote = namedtuple(
    "Quote",
    [
        "code",
        "name",
        "pinyin",
        "id",
        "jys",
        "classify",
        "market_type",
        "security_typeName",
        "security_type",
        "mkt_num",
        "type_us",
        "quote_id",
        "unified_code",
        "inner_code",
    ],
)


@retry(tries=3, delay=1)
def get_quote_id(
    stock_code: str,
    use_local=True,
    suppress_error=False,
    **kwargs,
) -> str:
    """
    Get the East Money specific quote ID for a given stock code.
    """
    if len(str(stock_code).strip()) == 0:
        if suppress_error:
            return ""
        raise ValueError("stock_code cannot be empty")
    quote = search_quote(
        stock_code, use_local=use_local, **kwargs
    )
    if isinstance(quote, Quote):
        return quote.quote_id
    if quote is None:
        if not suppress_error:
            print(f'stock_code "{stock_code}" not found')
        return ""
    return ""


def search_quote(
    keyword: str,
    count: int = 1,
    use_local: bool = True,
) -> Quote|None|List[Quote]:
    """
    Get quote information by searching for a keyword.
    """

    if use_local and count == 1:
        quote = search_quote_locally(keyword)
        if quote:
            return quote
    url = "https://searchapi.eastmoney.com/api/suggest/get"
    params = (
        ("input", f"{keyword}"),
        ("type", "14"),
        ("token", "D43BF722C8E33BDC906FB84D85E326E8"),
        ("count", f"{max(count, 5)}"),
    )
    try:
        json_response = session.get(url, params=params, proxies=proxies).json()
        items = json_response["QuotationCodeTable"]["Data"]
    except json.JSONDecodeError as e:
        raise RuntimeWarning(
            "unable to parse search quote result, consider if you are blocked"
        ) from e

    if items is not None and items:
        quotes = [
            Quote(*item.values())
            for item in items
            if keyword == item["Code"]
        ]
        save_search_result(keyword, quotes[:1])
        if count == 1:
            return quotes[0] if len(quotes) >= 1 else None

        return quotes[:count]

    return None


def search_quote_locally(
    keyword: str|None = None
) -> Quote|None:
    """
    Search for a quote by keyword in the local cache.
    """
    q = em_cache.search_result.get(keyword)
    if q is None:
        return None

    last_time: float = q["last_time"]
    # Seconds for cache expiration
    max_ts = 3600 * 24 * 3
    now = time.time()
    # Cache expired, search online
    if (now - last_time) > max_ts:
        return None
    _q = q.copy()
    del _q["last_time"]
    quote = Quote(**_q)
    return quote


def save_search_result(keyword: str, quotes: List[Quote]):
    """
    Save search results to a file.
    """
    with open(em_cache.search_result_path, "w", encoding="utf-8") as f:
        for quote in quotes:
            now = time.time()
            d = dict(quote._asdict())
            d["last_time"] = now
            em_cache.search_result[keyword] = d
            break
        json.dump(em_cache.search_result.copy(), f)


def get_quote_history(
    codes: str|List[str],
    beg: str = "19000101",
    end: str = "20500101",
    klt: int = 101,
    fqt: int = 1,
    suppress_error: bool = False,
    use_id_cache: bool = True,
    **kwargs,
) -> pd.DataFrame|Dict[str, pd.DataFrame]:
    """
    Get the K-line data for a stock or multiple stocks.
    """

    df = None
    if isinstance(codes, str):
        df = get_quote_history_single(
            codes,
            beg=beg,
            end=end,
            klt=klt,
            fqt=fqt,
            suppress_error=suppress_error,
            use_id_cache=use_id_cache,
            **kwargs,
    )
    elif hasattr(codes, "__iter__"):
        codes = list(codes)
        df = get_quote_history_multi(
            codes,
            beg=beg,
            end=end,
            klt=klt,
            fqt=fqt,
            suppress_error=suppress_error,
            use_id_cache=use_id_cache,
            **kwargs,
    )
    else:
        raise TypeError("codes must be a string or an iterable of strings")

    if isinstance(df, pd.DataFrame):
        df.rename(columns={"代码": "股票代码", "名称": "股票名称"}, inplace=True)
    elif isinstance(df, dict):
        for _, item in df.items():
            item.rename(
                columns={"代码": "股票代码", "名称": "股票名称"}, inplace=True
            )
    return df


@to_numeric
def get_quote_history_single(
    code: str,
    beg: str = "19000101",
    end: str = "20500101",
    klt: int = 101,
    fqt: int = 1,
    suppress_error: bool = False,
    use_id_cache: bool = True,
    **kwargs,
) -> pd.DataFrame:
    """
    Get the K-line data for a single stock.
    """

    fields = list(EASTMONEY_KLINE_FIELDS.keys())
    columns = list(EASTMONEY_KLINE_FIELDS.values())
    fields2 = ",".join(fields)

    quote_id = get_quote_id(
        stock_code=code,
        use_local=use_id_cache,
        suppress_error=suppress_error,
        **kwargs,
    )

    params = (
        ("fields1", "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13"),
        ("fields2", fields2),
        ("beg", beg),
        ("end", end),
        ("rtntype", "6"),
        ("secid", quote_id),
        ("klt", f"{klt}"),
        ("fqt", f"{fqt}"),
    )

    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"

    json_response = session.get(
        url, headers=EASTMONEY_REQUEST_HEADERS, params=params, verify=True, proxies=proxies
    ).json()
    klines = jsonpath(json_response, "$..klines[:]")
    if not klines:
        columns.insert(0, "代码")
        columns.insert(0, "名称")
        return pd.DataFrame(columns=columns)

    rows = [kline.split(",") for kline in klines]
    name = json_response["data"]["name"]
    code = quote_id.split(".")[-1]
    df = pd.DataFrame(rows, columns=columns)
    df.insert(0, "代码", code)
    df.insert(0, "名称", name)

    return df


def get_quote_history_multi(
    codes: List[str],
    beg: str = "19000101",
    end: str = "20500101",
    klt: int = 101,
    fqt: int = 1,
    tries: int = 3,
    suppress_error: bool = False,
    use_id_cache: bool = True,
    **kwargs,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch historical market data for multiple stocks.

    """

    dfs: Dict[str, pd.DataFrame] = {}
    total = len(codes)

    @multitasking.task
    @retry(tries=tries, delay=1)
    def start(code: str):
        _df = get_quote_history_single(
            code,
            beg=beg,
            end=end,
            klt=klt,
            fqt=fqt,
            suppress_error=suppress_error,
            use_id_cache=use_id_cache,
            **kwargs,
        )
        dfs[code] = _df
        pbar.update(1)
        pbar.set_description_str(f"Processing => {code}")

    pbar = tqdm(total=total)
    for code in codes:
        if len(multitasking.get_active_tasks()) > MAX_CONNECTIONS:
            time.sleep(3)
        start(code)

    multitasking.wait_for_tasks()
    pbar.close()

    return dfs
