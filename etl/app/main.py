"""
This is the driver function for the application.

This file contains the logic to process the following steps:
    1. Create and retrieve Spark session
    2. Read the input Json File
    3. Calculate the BMI value for each record
    4. Categorize the records with the BMI Category and Health risk with the BMI value
    5. Count the number of records with BMI Category as "Overweight"
    6. Write the processed dataframe in parquet format to the output folder with date partition

"""
import datetime
import sys

from etl.app.dataframe_processor import calculate_bmi, get_bmi_category, get_record_count
from etl.app_utils.logging_utils import get_logger
from etl.app_utils.spark_utils import get_spark_session, read_input_json_data, write_csv_output
from etl.config_utils.app_config_reader import read_config

logger = get_logger("main")


def partition_folder_path(file_path):
    """
    This function provides the output path to which the data frame will be created
    :param file_path: file path from config_utils for the value 'output_file_path'
    :return: output file path with date partition
    """
    file_path = file_path + str(datetime.datetime.today().year) + '/' + str(
        datetime.datetime.today().month) + '/' + str(datetime.datetime.today().day)
    logger.info("file_path: " + file_path)
    return file_path


if __name__ == '__main__':
    """
        Main driver function to run the spark application
    """

    try:
        
        input_df = read_input_json_data(get_spark_session('BMI Calculator Challenge'), read_config("Paths", "input_file_path"))
        logger.info("dataframe from input json files created")

        bmi_df = calculate_bmi(input_df)
        logger.info("BMI Calculated")
        
        bmi_category_df = get_bmi_category(bmi_df)
        logger.info("BMI Category and Health risk derived")

        people_count = get_record_count(bmi_category_df)
        logger.info("Total number of people with overweight category: {}".format(people_count))

        write_csv_output(partition_folder_path(read_config("Paths", "output_file_path")), bmi_category_df)
        logger.info("Data processing completed")

    except Exception as message:
        logger.info("Failed to process the input file")
        logger.exception("Error in main driver function " + str(message))
        sys.exit(400)
