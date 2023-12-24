from pyspark.sql import SparkSession

"""
This file contains common functions which are used in testing functions
"""


def get_spark_session():
    """
    Function to return the spark session
    :return: spark session
    """
    return SparkSession.builder.appName("Preprocess Unit Test").getOrCreate()


def get_spark_df(spark, data, schema):
    """
    This function returns a spark data frame
    :param spark: Spark session
    :param data: data fields
    :param schema: schema of the data fields
    :return: spark data frame
    """
    df = spark.createDataFrame(data=data, schema=schema)
    return df


def compare_data_frames(df1, df2):
    """
    Function for comparing two DataFrame data. If data is equal returns True.
    :param df1: actual DataFrame
    :param df2: expected DataFrame
    :return: Boolean result if dataframes are equal
    """
    data1 = df1.collect()
    data2 = df2.collect()
    return set(data1) == set(data2)