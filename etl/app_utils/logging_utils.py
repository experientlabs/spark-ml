"""
This file contains the logger app_utils functions
"""
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler


FORMATTER = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../logs/logs.log')


def get_console_handler():
    """
    This function returns the handler for the logger
    :return: returns stream handler for logger
    """
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler():
    """
    This function returns the file handler object which rotates the log file at midnight of each day
    :return: file handler with rotating log file configuration
    """
    file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight')
    file_handler.setFormatter(FORMATTER)
    return file_handler


def get_logger(logger_name):
    """
    This function returns the logger object with the handler and log rotation config_utils defined.
    :param logger_name: Name or file for which the logger is created.
    :return: logger object with the handler and log rotation config_utils defined.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler())
    logger.propagate = False
    return logger
