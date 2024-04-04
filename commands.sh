#!/bin/bash
docker exec postgres_c psql -U root -d postgres -f psql_scripts/db_setup.sql
docker exec postgres_c psql -U abc -w abc -d dvd_database -f psql_scripts/create_tables.sql
docker exec postgres_c cp -f /tmp/pg_hba.conf /var/lib/postgresql/data/pg_hba.conf

#docker exec -it postgres_c psql -U abc -d dvd_database -h 127.0.0.1 -W
#docker exec -it postgres_c psql /bin/bash