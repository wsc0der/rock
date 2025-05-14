"""Service code for data."""

import pkgutil
import importlib
from typing import Generator
from types import ModuleType
from rock.data import db
from rock import exchange
from rock import logger


def init_db() -> None:
    """Initialize the database."""
    for module in get_exchange_modules():
        if not hasattr(module, 'METADATA'):
            logger.warning("Module %s does not have METADATA.", module.__name__)
            continue

        db.insert_exchange(*module.METADATA)

    logger.info("Database initialized successfully.")


def update_securities() -> bool:
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
                    security = db.get_security(stock.symbol)
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
            except Exception as e:  # pylint: disable=W0718
                logger.error("Error updating securities from %s: %s", module.__name__, e)
                logger.error("stock_list: %s", stock)
                continue

    logger.info("Securities updated.")


def update_histories() -> bool:
    """Update the historical data in the database."""


def get_exchange_modules() -> Generator[ModuleType, None, None]:
    """Get all exchange modules."""
    for module_info in pkgutil.iter_modules(exchange.__path__, exchange.__name__ + '.'):
        if module_info.name.split('.')[-1].startswith('exchange'):
            module = importlib.import_module(module_info.name)
            yield module


if __name__ == "__main__":
    if not db.db_exist():
        db.create_db()
        init_db()

    update_securities()
    update_histories()
