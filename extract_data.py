import csv
import boto3
from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("Extract from S3")\
    .getOrCreate()

with open('Jerrold_accessKeys.csv') as file:
    reader = csv.reader(file, delimiter=',')
    next(reader)
    access_key_id, secret_access_key = next(reader)


s3 = boto3.client('s3',
                  aws_access_key_id=access_key_id,
                  aws_secret_access_key=secret_access_key)

print('-----------------')
# Set the S3 path
s3_path = "s3a://dvd-rental-data/folder1/address.dat"

# Set Hadoop configuration for S3 access
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key_id)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_access_key)

# Read data into DataFrame
df = spark.read.csv(s3_path)#,
                    #sep='\t',
                    #schema='address_id int, address string, address2 string, district string, city_id int, postal_code int, phone string, last_update timestamp')

df.printSchema()
df.show()
