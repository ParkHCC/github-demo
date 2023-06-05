import pyspark

from pyspark.sql import SparkSession

conf = pyspark.SparkConf()
conf.set('spark.driver.host', '127.0.0.1')
conf.set('spark.hadoop.fs.s3a.access.key', 'AKIAUWCJLKGCBNGWS4E3')    #AWS Access Key
conf.set('spark.hadoop.fs.s3a.secret.key', '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')   #AWS Secret Key
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.2')   #Apache Hadoop 버전에 맞는 Library 설치

spark = SparkSession.builder \
    .config(conf=conf) \
        .appName('S3 Bucket Reader Program') \
            .getOrCreate()

input_path = 's3a://' + 'phc-datalake-bucket' + '/input' #Bucket name
print('Green Taxi Data')
green_df = spark.read.parquet(f'{input_path}/green_tripdata_2022-10.parquet')
green_df.show(truncate=False)

print('------------------------------')
print('Yellow Taxi Data')
yellow_df = spark.read.parquet(f'{input_path}/yellow_tripdata_2022-10.parquet')
yellow_df.show(truncate=False)