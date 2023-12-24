"""
This file contains the functions to calculate the BMI for each record,
categorize BMI and health risk based on the BMI value from the dataframe.

"""


import pyspark.sql.functions as F

from etl.app_utils.logging_utils import get_logger

logger = get_logger("dataframe_processor")


def calculate_bmi(input_df):
    """
    This function calculates height in meters and BMI value for the input dataframe.
    :param input_df: raw input dataframe for which the BMI will be calculated.
    :return: dataframe with height in meters,BMI value added to the input dataframe
    """
    return input_df.withColumn('HeightM', input_df.HeightCm / 100).withColumn('BMI', F.round(
        input_df.WeightKg / ((input_df.HeightCm / 100) * (input_df.HeightCm / 100)), 2))


def get_bmi_category(df):
    """
    This function adds the BMI category and Health risk based on the BMI value
    :param df: input dataframe with BMI values
    :return: Dataframe with  BMI category and Health risk derived from their respective BMI values
    """
    return df.withColumn('BMI Category', F.when(df.BMI <= 18.4, "Underweight")
                         .when((df.BMI >= 18.5) & (df.BMI <= 24.9), "Normal weight")
                         .when((df.BMI >= 25) & (df.BMI <= 29.9), "Overweight")
                         .when((df.BMI >= 30) & (df.BMI <= 34.9), "Moderately obese")
                         .when((df.BMI >= 35) & (df.BMI <= 39.9), "Severely obese")
                         .when((df.BMI >= 40), "Very severely obese")
                         .otherwise('Undefined'))\
        .withColumn('Health risk', F.when(df.BMI <= 18.4, "Malnutrition risk")
                    .when((df.BMI >= 18.5) & (df.BMI <= 24.9), "Low risk")
                    .when((df.BMI >= 25) & (df.BMI <= 29.9), "Enhanced risk")
                    .when((df.BMI >= 30) & (df.BMI <= 34.9), "Medium risk")
                    .when((df.BMI >= 35) & (df.BMI <= 39.9), "High risk")
                    .when((df.BMI >= 40), "Very high risk")
                    .otherwise('Undefined'))


def get_record_count(processed_df):
    """
    This function returns the count of people who are in 'Overweight' category
    :param processed_df: dataframe with BMI value and their respective BMI categories
    :return: Count of records of people with BMI category as 'Overweight'
    """
    return processed_df.filter(F.col("BMI Category") == 'Overweight').count()