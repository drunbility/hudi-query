A simple data lake query service that can query Apache Hudi and collect results in Excel format.

The query service pulls messages periodically from Apache RocketMQ, which indicate the query SQL. The service then sends the SQL to the Kyuubi server for processing. 

The Kyuubi server can be connected via a JDBC client to execute SQL queries and retrieve results. Alternatively, the Kyuubi server can be connected through a restful API to submit a raw Spark on YARN application for executing the query on Hudi and saving the results on HDFS.


The query service blocks during execution and must monitor the specific SQL query process until it successfully ends. Once the query is complete, the service retrieves the resulting Excel file locally, makes any necessary preparations, and sends it to the business side.