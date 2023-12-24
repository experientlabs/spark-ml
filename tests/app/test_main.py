"""
Test the main funtion file.

This file contains testing of partition_folder_path function only.
Main function is not tested

"""
import datetime

from etl.app.main import partition_folder_path
from etl.config_utils.app_config_reader import read_config


def test_partition_folder_path():
    """
    Test the file path read from the config
    :return: None
    """
    path = read_config("Paths", "output_file_path")
    actual = partition_folder_path(path)
    expected = '../../resources/output/' + str(datetime.datetime.today().year) + '/' + str(
        datetime.datetime.today().month) + '/' + str(datetime.datetime.today().day)
    assert actual == expected
