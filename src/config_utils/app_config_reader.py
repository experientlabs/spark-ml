"""
This file contains the functions to read configurations from file and return their values

"""

import configparser
import os

config = configparser.ConfigParser()
#ini_path = os.path.join(os.getcwd(),'app_config.ini')
# get the path to config.ini
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app_config.ini')
config.read(config_path)
#config.read(r'../../app_config.ini')


def read_config(section, config_data):
    """
    This function provides the config_utils value for the given input
    :param section:
    :param config_data:
    :return:
    """
    return config.get(section, config_data)
