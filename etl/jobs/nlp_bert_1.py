import json
import pandas as pd
import numpy as np

import sparknlp
import pyspark.sql.functions as F

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from sparknlp.annotator import *
from sparknlp.base import *
from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql.types import StringType, IntegerType


def start_spark():
    spark = sparknlp.start()
    print("Spark NLP Version :", sparknlp.version())
    return spark


def select_model():
    model_name = 'classifierdl_use_fakenews'
    return model_name


documentAssembler = DocumentAssembler()\
    .setInputCol("text")\
    .setOutputCol("document")

use = UniversalSentenceEncoder.pretrained(lang="en") \
 .setInputCols(["document"])\
 .setOutputCol("sentence_embeddings")

document_classifier = ClassifierDLModel.pretrained(model_name)\
  .setInputCols(['sentence_embeddings'])\
  .setOutputCol("class_")

nlpPipeline = Pipeline(
    stages=[
        documentAssembler,
        use,
        document_classifier])



sc = SparkContext(master= 'local', appName= 'Fake and real news')
ss = SparkSession(sc)

# Funtion for conver Pandas Dataframe to Spark Dataframe
from pyspark.sql.types import StringType, StructField, StructType
def read_data(path):
  schema= StructType(
      [StructField('title', StringType(), True),
      StructField('text', StringType(), True),
      StructField('subject', StringType(), True),
      StructField('date', StringType(), True)])
  pd_df= pd.read_csv(path)
  sp_df= ss.createDataFrame(pd_df, schema = schema)
  return sp_df