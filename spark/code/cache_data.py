"""
The purpose of this file is to validate the data in the transactional_data folder to ensure that the transactional data is clean before 
being transformed and loaded in the DWH.
"""
import argparse
from pyspark.sql import SparkSession

def cache_data(file, batch_date):
	spark = SparkSession.builder.appName(f"Cache {file}").getOrCreate()
	
	s3_path = f"s3a://dvd-rental-data/transaction_data/{batch_date}/{file}"

	try:
		df = spark.read.options(header='true', inferSchema='true').csv(s3_path)
	except FileNotFoundError as e:
		print("Error during file read from AWS: ", e)
	except InterruptedError as e:
		print("Error during file read from AWS: ", e)
		# Other actions such as logging

	# Cache the data as the name of the file without extension
	name = file.rstrip('.csv')
	globals()[name] = df
	globals()[name].cache()
	globals()[name].count()
	payment = df; payment.cache(); payment.count()
	spark.stop()

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--file")
	parser.add_argument("--batch_date") # string in the form yyyy-mm-dd
	args = parser.parse_args()
	print(args.file, args.batch_date)

	cache_data(args.file, args.batch_date)
	print('Cache successful')

