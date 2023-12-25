"""
This file contains the app_utils functions required for spark.
Functions:
    i. get_spark_session: This function creates and returns a spark session.
   ii. read_input_json_files: This function reads the input JSON files from a file path and returns a dataframe.
  iii. write_csv_output: This function writes the dataframe into a csv file in the desired location.

"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType


def get_spark_session(app_name, logger):
    """
    Create and returns spark session
    :param logger: logger object
    :param app_name: this is the input app_name which will be used to create the spark session
    :return: spark session with the input app_name
    """
    try:
        spark_session = SparkSession.builder.appName(app_name).getOrCreate()
        return spark_session
    except Exception as error_message:
        logger.info(" Failed to create sparkSession")
        logger.exception("Error in getting sparkSession" + str(error_message))
        sys.exit(400)


def read_text_data_to_spark_df(spark, file_path, logger):
    """
    Read text data from a CSV file into a Spark DataFrame.
    Parameters:
    - file_path: str, path to the CSV file.
    Returns:
    - df: pyspark.sql.DataFrame, Spark DataFrame containing the text data.
    """
    # Define schema (adjust data types as needed)
    schema = "text STRING, label INT"
    # Read CSV into Spark DataFrame
    logger.info(" Reading csv file")
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df


def write_csv_output(file_path, df, logger):
    """
    This function writes the dataframe to the output location in csv format
    :param logger: logger object
    :param file_path: the output folder path
    :param df: dataframe which is to be written to the desired location in csv
    :return: None
    """
    df.write.mode("overwrite").csv(file_path, header='true')
    logger.info("Data written to the output location: %s", file_path)
