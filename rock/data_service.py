"""Service code for data."""

import pkgutil
import importlib
from rock.data import db
from rock import exchange
from rock import logger


def init_db() -> None:
    """Initialize the database."""
    for module_info in pkgutil.iter_modules(exchange.__path__, exchange.__name__ + '.'):
        if module_info.name.startswith('exchange'):
            module = importlib.import_module(module_info.name)
            if hasattr(module, 'METADATA'):
                db.insert_exchange(*module.METADATA)
            else:
                logger.warning("Module %s does not have METADATA.", module_info.name)
    logger.info("Database initialized successfully.")


def update_securities() -> bool:
    """Update the securities in the database."""
    logger.info("Updating securities...")


def update_histories() -> bool:
    """Update the historical data in the database."""


if __name__ == "__main__":
    if not db.db_exist():
        db.create_db()
        init_db()

    update_securities()
    update_histories()
