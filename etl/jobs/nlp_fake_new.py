import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.ml.feature import (
    RegexTokenizer, StopWordsRemover, CountVectorizer, IDF,
    StringIndexer, VectorAssembler
)
from pyspark.ml import Pipeline
from pyspark.ml.pipeline import PipelineModel

from etl.app_utils.spark_utils import read_text_data_to_spark_df, get_spark_session
from etl.logging_utils.logging_util import setup_logger


def tokenize_text(input_col, output_col, pattern='\\W', to_lowercase=True):
    return RegexTokenizer(inputCol=input_col, outputCol=output_col, pattern=pattern, toLowercase=to_lowercase)


def remove_stop_words(input_col, output_col):
    return StopWordsRemover(inputCol=input_col, outputCol=output_col)


def compute_tf(input_col, output_col):
    return CountVectorizer(inputCol=input_col, outputCol=output_col)


def compute_tfidf(input_col, output_col):
    return IDF(inputCol=input_col, outputCol=output_col)


def index_string(input_col, output_col):
    return StringIndexer(inputCol=input_col, outputCol=output_col)


def assemble_features(input_cols, output_col):
    return VectorAssembler(inputCols=input_cols, outputCol=output_col)


def build_feature_pipeline():
    # Define stages for the pipeline
    text_tokenizer = tokenize_text('text', 'text_words')
    text_sw_remover = remove_stop_words('text_words', 'text_sw_removed')
    text_count_vectorizer = compute_tf('text_sw_removed', 'tf_text')
    text_tfidf = compute_tfidf('tf_text', 'tf_idf_text')

    subject_str_indexer = index_string('subject', 'subject_idx')

    vec_assembler = assemble_features(['tf_idf_title', 'tf_idf_text', 'subject_idx'], 'features')

    # Create a pipeline
    pipeline = Pipeline(stages=[
        text_tokenizer, text_sw_remover, text_count_vectorizer, text_tfidf,
        subject_str_indexer, vec_assembler
    ])

    return pipeline


if __name__ == "__main__":
    # Example usage
    feature_pipeline = build_feature_pipeline()

    # Fit the pipeline on your DataFrame
    fitted_pipeline = feature_pipeline.fit(your_data_frame)

    # Transform your DataFrame
    transformed_data = fitted_pipeline.transform(your_data_frame)

    # If you want to save the fitted pipeline for later use
    fitted_pipeline.save("your_model_path")

    # If you want to load the saved pipeline
    loaded_pipeline = PipelineModel.load("your_model_path")


# if __name__ == "__main__":
#     logger = setup_logger()
#     path = '/home/sanjeet/Desktop/git_pod_el/spark-ml/resources/input/train.csv'
#     spark = get_spark_session("nlp_app", logger)
#     df = read_text_data_to_spark_df(spark, path, logger)
#     df.groupBy('label').count().show()
#     df.show(10)
