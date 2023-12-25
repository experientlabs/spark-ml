import datetime
import io
import logging
from logging.config import fileConfig
from logging.handlers import RotatingFileHandler


def setup_logger(config_file='log_config.ini'):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"app_{timestamp}.log"
    logging.config.fileConfig(config_file, defaults={'log_filename': log_filename})
    file_handler = logging.getLogger('fileHandler')
    file_handler.baseFilename = log_filename
    return logging.getLogger(__name__)


if __name__ == '__main__':
    logger = setup_logger()
    logger.info("This is an information message.")
    logger.error("This is an error message.")
    logger.debug("debug message")
