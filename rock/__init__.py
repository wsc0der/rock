"""
rock
"""

import logging
# Configure the logging system
logging.basicConfig(
    level=logging.DEBUG,  # Set the minimum log level
    format='[%(asctime)s][%(name)s][%(levelname)s][%(filename)s] %(message)s',  # Log format
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler('app.log')  # Log to a file
    ]
)
logging.getLogger('urllib3').setLevel(logging.INFO)

logger = logging.getLogger('rock')
