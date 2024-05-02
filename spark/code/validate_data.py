"""
The purpose of this file is to validate the data in the transactional_data folder to save a copy of the cleaned data before 
being transformed and loaded in the DWH, and to log anomalies we find

./bin/spark-submit ./pyspark_scripts/validate_data.py --file rental.csv --batch_date 2007-04-30
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count

def validate_data(file, batch_date):
	spark = SparkSession.builder\
		.appName(f"Validate {file}")\
		.master('spark://spark-master:7077')\
		.getOrCreate()
	
	s3_path = f"s3a://dvd-rental-data/transaction_data/{batch_date}/{file}"

	try:
		df = spark.read.options(header='true', inferSchema='true').csv(s3_path)
	except FileNotFoundError as e:
		print("Error during file read from AWS: ", e)
	except InterruptedError as e:
		print("Error during file read from AWS: ", e)
		# Other actions such as logging

	# Clean the data and report any errors in the data through logs (airflow or python)
	missing_count_by_col_df = df.select([count(
		when(col(c).contains('None') | col(c).contains('NULL') | (col(c) == '' ) | col(c).isNull() | isnan(c), c 
	   )).alias(c) for c in df.columns])
	# Some logging for the number of missing values in each column
	for i, c in enumerate(missing_count_by_col_df.columns):
		print(c, missing_count_by_col_df.collect()[0][i])

	df.na.drop("any").show()

	# Write it back to the same folder
	new_name = file.rstrip('.csv')
	df.write\
		.format('csv')\
		.options(mode='overwrite', header=True)\
		.save(f"s3a://dvd-rental-data/transaction_data/{batch_date}/{new_name}_cleaned")
	
	spark.stop()

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--file")
	parser.add_argument("--batch_date") # string in the form yyyy-mm-dd
	args = parser.parse_args()
    
	validate_data(args.file, args.batch_date)

