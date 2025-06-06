"""Service code for data."""

import pkgutil
import importlib
from enum import StrEnum
from typing import Generator
from types import ModuleType
from sqlite3 import Row, Error
from rock.data import db, web_scraper
from rock import exchange
from rock.logger import logger
from rock.common import utils


class DBKeys(StrEnum):
    """Keys for database metadata."""
    HISTORY_UPDATED_AT = 'history_updated_at'


def init_db() -> None:
    """Initialize the database."""
    for module in get_exchange_modules():
        if not hasattr(module, 'METADATA'):
            logger.warning("Module %s does not have METADATA.", module.__name__)
            continue

        db.insert_exchange(*module.METADATA)

    logger.info("Database initialized successfully.")


def update_securities() -> None:
    """Update the securities in the database."""
    logger.info("Updating securities...")

    for module in get_exchange_modules():
        if not hasattr(module, 'get_stock_list'):
            logger.warning("Module %s does not have get_stock_list function.", module.__name__)
            continue

        if hasattr(module, 'get_a_shares'):
            try:
                stock_list = module.get_a_shares()
                exchange_id = db.get_exchange_id(module.METADATA.acronym)
                insert_list = []
                for stock in stock_list:
                    security: Row|None = db.get_security(str(stock.symbol))  # type: ignore
                    if not security:
                        insert_list.append((stock.symbol, stock.name, 'stock',
                                           stock.listing,
                                           stock.delisting,
                                           exchange_id))
                    elif security['delisting'] is None and stock.delisting is not None:
                        db.update_security_delisting(stock.symbol, stock.delisting)
                    else:
                        # security already exists and is not delisted
                        continue
                db.insert_securities(insert_list)
            except Error as e:  # pylint: disable=W0718
                logger.error("Error updating securities from %s: %s", module.__name__, e)
                continue

    logger.info("Securities updated.")


def update_histories(inc: bool = False) -> None:
    """Update the historical data in the database."""
    logger.info("Updating historical data...")
    securities = db.get_all_securities()
    history_updated_at = db.get_meta(DBKeys.HISTORY_UPDATED_AT)
    histories = web_scraper.get_history(
        [security['symbol'] for security in securities],
        start = history_updated_at if inc else None
    )
    for security in securities:
        history = histories[security['symbol']]
        if not history.empty:
            db.bulk_insert_history([(
                int(security['id']),
                str(row.datetime),
                float(row.open),    # type: ignore
                float(row.close),    # type: ignore
                float(row.high),    # type: ignore
                float(row.low),    # type: ignore
                float(row.adj_close),    # type: ignore
                int(row.volume),    # type: ignore
                int(row.amount),    # type: ignore
                web_scraper.Interval.ONE_DAY,
            ) for row in history.itertuples(index=False)])
        else:
            logger.warning("No history data for %s", security['symbol'])
    db.insert_meta(DBKeys.HISTORY_UPDATED_AT, utils.get_current_date())
    logger.info("Historical data updated.")


def get_exchange_modules() -> Generator[ModuleType, None,None]:
    """Get all exchange modules."""
    for module_info in pkgutil.iter_modules(exchange.__path__, exchange.__name__ + '.'):
        if module_info.name.split('.')[-1].startswith('exchange'):
            module = importlib.import_module(module_info.name)
            yield module


def run() -> None:
    """Run the data service."""
    if not db.db_exist():
        db.create_db()
        init_db()

    update_securities()
    update_histories(True)


if __name__ == "__main__":
    run()
