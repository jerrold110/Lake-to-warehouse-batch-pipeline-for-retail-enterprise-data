import csv
import boto3
import pyspark

with open('Jerrold_accessKeys.csv') as file:
    reader = csv.reader(file, delimiter=',')
    next(reader)
    access_key_id, secret_access_key = next(reader)

bucket = 'dvd-rental-data'

s3 = boto3.client('s3',
                  aws_access_key_id=access_key_id,
                  aws_secret_access_key=secret_access_key)

s3.download_file('dvd-rental-data', 'folder1/address.dat', 'input/address.dat')

