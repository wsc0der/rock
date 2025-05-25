"""
This module configures the logging system for the Rock application.
It sets the logging level, format, and handlers to output logs to the console.
"""

import logging

# Configure the logging system
logging.basicConfig(
    level=logging.DEBUG,  # Set the minimum log level
    format='[%(asctime)s][%(name)s][%(levelname)s][%(filename)s] %(message)s',  # Log format
    handlers=[
        logging.StreamHandler()  # Log to console
    ]
)
logging.getLogger('urllib3').setLevel(logging.INFO)

logger = logging.getLogger('rock')
