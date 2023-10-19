import logging
import datetime

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a file handler
log_file = f"Case_Analysis_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.log"
file_handler = logging.FileHandler(log_file)

# Create a log formatter
log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(log_format)

# Add the file handler to the logger
logger.addHandler(file_handler)

def get_logger():
    return logger
