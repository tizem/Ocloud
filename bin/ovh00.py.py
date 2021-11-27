from infra import spark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main(data, header, idSolution, app_name):

    # init spark session
    spark_session = spark.get_spark(app_name)
    # create dataframe from data and header
    df = spark_session.createDataFrame(data, header)

    match idSolution:
        case 1:
            return solution1(df)
        case 2:
            return solution2(df)
        case 3:
            return solution2(df)
        case _:
            return []
def solution1(df):
    """ in this solution we use rdd to achive the goal"""
    """ create rdd from dataframe"""
    r_dd = df.rdd
    rslt = r_dd.map(lambda l: (l, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda l: (l[0][1])) \
        .map(lambda l: (l[0][0], (l[0][1], l[1]))) \
        .reduceByKey(lambda x1, x2: max(x1, x2, key=lambda x: x[-1])) \
        .map(lambda l: (l[0], l[1][0])) \
        .collect()
    return  rslt

def solution2(df):
    """ in this solution we add another column to meet the goal"""
    df1 = df.groupBy('Key', 'Value')\
            .agg(F.count('*') \
            .alias("cnt"))\
            .withColumn('mx', F.max("cnt")
                              .over(Window.partitionBy("Key")
                              .orderBy(F.asc("Value"))))\
            .filter("cnt = mx")\
            .groupBy('Key').agg(F.first('Value').alias("Value"))

    return df1.collect()

def solution3(df):
    """ in this solution we use join to meet the gool"""
    df2 = df.withColumn("index", F.lit(1))\
            .groupBy('Key', 'Value')\
            .agg(F.sum('index').alias('flag'))
    df3 = df2.groupBy('Key')\
             .agg(F.max('flag').alias('flag'))\
             .select(F.col("Key").alias('Key1'), 'flag')
    df_f = df2.join(df3, (df2.Key == df3.Key1) & (df2.flag == df3.flag), how='inner')\
        .groupBy('Key')\
        .agg( F.min('Value').alias('Value'))
    return df_f.collect()

if __name__ == '__main__':
     data = [['A' ,'Blue'],['A','Red'],['A','Blue'],['A','Blue'],['B','Purple'],['B','Green'],['B','Purple'],['B','Green'],['C','Black']]
     header = ['Key','Value']
     main(data, header, 1, 'app00')
