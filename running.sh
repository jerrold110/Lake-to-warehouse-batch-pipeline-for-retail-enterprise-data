docker exec spark_master ./bin/spark-submit ./pyspark_scripts/validate_data.py --file payment.csv --batch_date 2007-04-30
docker exec spark_master ./bin/spark-submit ./pyspark_scripts/validate_data.py --file rental.csv --batch_date 2007-04-30


docker exec spark_master ./bin/spark-submit ./pyspark_scripts/transform_store.py --batch_date 2007-04-30
