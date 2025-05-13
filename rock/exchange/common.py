"""Exchange common module."""
from collections import namedtuple

ExchangeMeta = namedtuple('ExchangeMeta', ['name', 'acronym', 'type'])
StockMeta = namedtuple('StockMeta', ['symbol', 'name', 'listing', 'delisting'])
