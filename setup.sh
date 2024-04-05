#!/bin/bash
docker compose up &
sleep 2
docker exec postgres_c psql -U root -f psql_scripts/db_setup.sql
docker exec postgres_c psql -U abc -d dvd_database -f psql_scripts/create_tables.sql
# Make sure to use the original files
docker exec postgres_c cp -f /tmp/pg_hba.conf /var/lib/postgresql/data/pg_hba.conf
docker restart postgres_c
#docker exec -it postgres_c psql -U abc -d dvd_database -h 127.0.0.1 -W
#docker exec -it postgres_c /bin/bash
#docker exec -it postgres_c cat /var/lib/postgresql/data/pg_hba.conf
# docker exec -it postgres_c psql -U abc -d dvd_database
# docker exec -it postgres_c psql -U root -d dvd_database

docker exec spark_master cp -f /opt/bitnami/spark/db_connectors/postgresql-42.7.3.jar /opt/bitnami/spark/jars/postgresql-42.7.3.jar

echo 'Set up done'

