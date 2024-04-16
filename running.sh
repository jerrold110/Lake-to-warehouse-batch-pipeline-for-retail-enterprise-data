docker exec spark_master ./bin/spark-submit ./pyspark_scripts/validate_data.py --file payment.csv --batch_date 2007-04-30
docker exec spark_master ./bin/spark-submit ./pyspark_scripts/validate_data.py --file rental.csv --batch_date 2007-04-30

docker exec spark_master ./bin/spark-submit ./pyspark_scripts/etl_film.py --batch_date 2007-04-30
docker exec spark_master ./bin/spark-submit ./pyspark_scripts/etl_storecustomer.py --batch_date 2007-04-30
docker exec spark_master ./bin/spark-submit ./pyspark_scripts/etl_date.py --batch_date 2007-04-30
docker exec spark_master ./bin/spark-submit ./pyspark_scripts/etl_Fsale.py --batch_date 2007-04-30

