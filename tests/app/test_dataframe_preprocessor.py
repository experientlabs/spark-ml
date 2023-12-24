"""
This file contains the testing of 'dateframe_processor'

"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from etl.app.dataframe_processor import calculate_bmi, get_bmi_category, get_record_count
from etl.app_utils.spark_utils import get_spark_session
from tests.app_utils.test_utils import get_spark_df, compare_data_frames


def get_input_dataframe():
    """
    Get the input dataframe
    :return: data and the schema for the dataframe
    """
    data = [("Male", 171, 76), ("Female", 151, 96)]
    schema = StructType([StructField("Gender", StringType(), True),
                         StructField("HeightCm", IntegerType(), True),
                         StructField("WeightKg", IntegerType(), True)])

    return data, schema


def get_bmi_dataframe():
    """
    Get the dataframe with bmi info
    :return: data and schema for the dataframe
    """
    data = [("Male", 171, 76, 1.71, 25.99), ("Female", 151, 96, 1.51, 42.1)]
    schema = StructType([StructField("Gender", StringType(), True),
                         StructField("HeightCm", IntegerType(), True),
                         StructField("WeightKg", IntegerType(), True),
                         StructField("HeightM", DoubleType(), True),
                         StructField("BMI", DoubleType(), True)])

    return data, schema


def get_bmi_category_df():
    """
    Get the dataframe for the bmi category
    :return: data and schema for the data frame
    """
    data = [("Male", 171, 76, 1.71, 25.99, "Overweight", "Enhanced risk"),
            ("Female", 151, 96, 1.51, 42.1, "Very severely obese", "Very high risk")]
    schema = StructType([StructField("Gender", StringType(), True),
                         StructField("HeightCm", IntegerType(), True),
                         StructField("WeightKg", IntegerType(), True),
                         StructField("HeightM", DoubleType(), True),
                         StructField("BMI", DoubleType(), True),
                         StructField("BMI Category", StringType(), True),
                         StructField("Health risk", StringType(), True)])

    return data, schema


def test_calculate_bmi():
    """
    Test Calculate BMI
    :return: None
    """
    spark = get_spark_session("test_calculate_bmi")
    data, schema = get_input_dataframe()
    input_df = get_spark_df(spark, data, schema)

    transformed_df = calculate_bmi(input_df)
    transformed_df.show()
    transformed_df.printSchema()

    data, schema = get_bmi_dataframe()
    expected_df = get_spark_df(spark, data, schema)
    assert (compare_data_frames(transformed_df, expected_df))


def test_get_bmi_category():
    """
    Test the BMI category from data
    :return: None
    """
    spark = get_spark_session("test_get_bmi_category")
    data, schema = get_bmi_dataframe()
    input_df = get_spark_df(spark, data, schema)

    transformed_df = get_bmi_category(input_df)
    transformed_df.show()
    transformed_df.printSchema()

    data, schema = get_bmi_category_df()
    expected_df = get_spark_df(spark, data, schema)
    assert (compare_data_frames(transformed_df, expected_df))


def test_get_record_count():
    """
    Test the record count with 'overweight' category
    :return: None
    """
    spark = get_spark_session("test_get_record_count")
    data, schema = get_bmi_category_df()
    input_df = get_spark_df(spark, data, schema)

    actual = get_record_count(input_df)
    expected = 1
    assert actual == expected
