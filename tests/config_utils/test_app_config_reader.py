"""
Test the config reader function
"""

from src.config_utils.app_config_reader import read_config


def test_read_config():
    """
    Test the read config function
    :return: None
    """
    section = "Paths"
    config_data = "output_file_path"

    actual = read_config(section, config_data)
    expected = '../../resources/output/'
    assert actual == expected
