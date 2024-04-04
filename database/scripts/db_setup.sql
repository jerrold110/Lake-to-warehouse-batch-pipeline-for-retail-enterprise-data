/*
We run this file after connecting with the superuser postgres.
This file creates a new user and database
*/
CREATE DATABASE dvd_database;
CREATE USER abc WITH PASSWORD 'abc';
GRANT ALL PRIVILEGES  ON DATABASE dvd_database TO abc;
--CREATE SCHEMA dvd_schema;
--GRANT CREATE, INSERT, UPDATE, DELETE, SELECT ON dvd_schema TO abc;

