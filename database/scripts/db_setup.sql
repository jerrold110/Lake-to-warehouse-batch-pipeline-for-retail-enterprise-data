/*
We run this file after connecting with the superuser postgres.
This file creates a new user and database
*/
CREATE DATABASE dvd_database;
CREATE USER abc WITH PASSWORD 'abc';
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO abc;
--ALTER DATABASE dvd_database OWNER to abc;
\c dvd_database root

