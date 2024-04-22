#!/bin/bash
# Sometimes 'docker compose up' takes a long time. Better to run docker compose up first, then these commands
docker exec postgres_c psql -U root -f psql_scripts/db_setup.sql
docker exec postgres_c psql -U root -d dvd_database -f psql_scripts/create_tables.sql

# Configure the db and then restart to enforce changes
#docker exec postgres_c cp -f /tmp/pg_hba.conf /var/lib/postgresql/data/pg_hba.conf # Make sure to use the original files
docker restart postgres_c
# docker exec -it postgres_c psql -U abc -d dvd_database -W
# docker exec -it postgres_c bash
# docker exec -it postgres_c cat /var/lib/postgresql/data/pg_hba.conf
# docker exec -it postgres_c psql -U abc -d dvd_database
# docker exec -it postgres_c psql -U root -d dvd_database
# docker exec spark_master cp -f /opt/bitnami/spark/db_connectors/postgresql-42.7.3.jar /opt/bitnami/spark/jars/postgresql-42.7.3.jar

echo '*********Set up done***********'

