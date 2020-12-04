# pysnow
Multithreading Python connector to send large data files to Snowflake
---
This was created to be used with Single Sign-On (SSO) login, 
for large (250MB+) .zip, .csv, or .txt files to be sent to a Snowflake table.

Has been tested and successful with 44 million row compressed csv file 
(2.1GB compressed, 41.5GB uncompressed) and smaller.