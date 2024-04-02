"""
The purpose of this file is to validate the data in the transactional_data folder to ensure that the transactional data is clean before 
being transformed and loaded in the DWH.
"""
import argparse
from pyspark.sql import SparkSession

def validate_data(file, batch_date):
	spark = SparkSession.builder.appName(f"Validate {file}").getOrCreate()
	
	s3_path = f"s3a://dvd-rental-data/transactional_data/{file}"

	try:
		df = spark.read.csv(s3_path, sep="\t", )
	except FileNotFoundError as e:
		print("Error during file read from AWS: ", e)
	except InterruptedError as e:
		print("Error during file read from AWS: ", e)
		# Other actions such as logging

	df 

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--file")
	parser.add_argument("--batch_date")
	args = parser.parser_args()
    
	validate_data(args.file, args.batch_date)

