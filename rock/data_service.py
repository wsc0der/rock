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


def update_histories() -> bool:
    """Update the historical data in the database."""


def get_exchange_modules() -> Generator[ModuleType, None, None]:
    """Get all exchange modules."""
    for module_info in pkgutil.iter_modules(exchange.__path__, exchange.__name__ + '.'):
        if module_info.name.startswith('exchange'):
            module = importlib.import_module(module_info.name)
            yield module


if __name__ == "__main__":
    if not db.db_exist():
        db.create_db()
        init_db()

    update_securities()
    update_histories()
