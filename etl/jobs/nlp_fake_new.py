import pyspark.sql.functions as F
import sparknlp
from pyspark.sql.types import StringType
from sparknlp.annotator import *
from sparknlp.base import *


def start_spark_nlp():
    # Start Spark Session
    spark = sparknlp.start()
    print("Spark NLP Version :", sparknlp.version())
    return spark


def create_spark_nlp_pipeline(model_name='classifierdl_use_fakenews'):
    documentAssembler = DocumentAssembler() \
        .setInputCol("text") \
        .setOutputCol("document")

    use = UniversalSentenceEncoder.pretrained(lang="en") \
        .setInputCols(["document"]) \
        .setOutputCol("sentence_embeddings")

    document_classifier = ClassifierDLModel.pretrained(model_name) \
        .setInputCols(['sentence_embeddings']) \
        .setOutputCol("class_")

    nlp_pipeline = Pipeline(
        stages=[
            documentAssembler,
            use,
            document_classifier
        ])

    return nlp_pipeline


def run_spark_nlp_pipeline(spark, pipeline, text_list):
    df = spark.createDataFrame(text_list, StringType()).toDF("text")
    result = pipeline.fit(df).transform(df)
    return result


def visualize_results(result):
    result.select(F.explode(F.arrays_zip(result.class_.result,
                                         result.document.result)).alias("cols")) \
        .select(F.expr("cols['0']").alias("class"),
                F.expr("cols['1']").alias("document")).show(truncate=False)


def main():
    # Step 1: Start Spark NLP
    spark = start_spark_nlp()

    # Step 2: Create Spark NLP Pipeline
    nlp_pipeline = create_spark_nlp_pipeline()

    # Step 3: Run Spark NLP Pipeline
    text_list = ["Your sample text 1", "Your sample text 2", "Your sample text 3"]
    result = run_spark_nlp_pipeline(spark, nlp_pipeline, text_list)

    # Step 4: Visualize Results
    visualize_results(result)


if __name__ == "__main__":
    main()
