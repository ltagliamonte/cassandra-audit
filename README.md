# Cassandra Audit

Prior to Cassandra 4 there is no auditing feature included to the open-source version.
This is a proof of concept designed for archiving query logs in Elasticsearch.
Following the same principle it is possible to store the logs in any other datastore (Cassandra,Mysql,Postgress) (PRs are welcome). 

## Cassandra Version
This code has been tested on Cassandra 3.0.16, the QueryHandler interface has changed in newer Cassandra versions.
Update the pom dependency and adapt the code if you need it for newer Casssandra releases.

## Deploy this Audit plugin to Cassandra
* Put the built jar (`mvn clean install`) file to Cassandra `/lib` folder.
* Start Cassandra with the following Java option:

`-Dcassandra.custom_query_handler_class=com.ltagliamonte.cassandra.audit.AuditQueryHandler`

## Variables
Using Environment Variables it is possible to customise the following parameters:
- CASSANDRA_AUDIT_INDEX_NAME	(default: cassandra_audit)
- CASSANDRA_AUDIT_ES_ADDRESS	(default: 127.0.0.1)
- CASSANDRA_AUDIT_ES_PORT	(default: 443)
- CASSANDRA_AUDIT_ES_SCHEMA	(default: https)
- CASSANDRA_AUDIT_DAILY_INDEX	(default: false)
