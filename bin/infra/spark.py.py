from pyspark.sql import SparkSession, SQLContext



def get_spark(app_name):
    spark_session = build_spark_session(app_name)
    spark_context = spark_session.sparkContext
    return spark_session, spark_context


def build_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()









