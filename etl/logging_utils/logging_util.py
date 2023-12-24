import logging
import logging.config


def setup_logger(config_file='log_config.ini'):
    logging.config.fileConfig(config_file)
    return logging.getLogger(__name__)


# if __name__ == '__main__':
#     logger = setup_logger()
#     logger.info("This is an information message.")
#     logger.error("This is an error message.")
