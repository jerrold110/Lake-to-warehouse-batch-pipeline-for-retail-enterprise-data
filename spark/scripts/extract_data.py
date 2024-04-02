import csv
import boto3
from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("Extract from S3")\
    .getOrCreate()


s3 = boto3.client('s3',
                  aws_access_key_id=access_key_id,
                  aws_secret_access_key=secret_access_key)
                  
from pyspark import SparkConf

conf = SparkConf()
conf.set('spark.hadoop.fs.s3a.access.key',"")
conf.set('spark.hadoop.fs.s3a.secret.key', "")
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')

spark = SparkSession.builder.config(conf=conf).getOrCreate()

s3_path = "s3a://dvd-rental-data/folder1/address.dat"
df = spark.read.csv(s3_path, sep="\t", schema='address_id int, address string, address2 string, district string, city_id int, postal_code int, phone string, last_update timestamp')

df.printSchema()
df.show()

