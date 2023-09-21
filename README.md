A simple data lake query service that can query Apache Hudi and collect results in Excel format.

The query service pulls messages periodically from Apache RocketMQ, which indicate the query SQL. The service then sends the SQL to the Kyuubi server for processing. 

It connects to the Kyuubi server using a JDBC client to directly execute SQL and retrieve results.

It is possible to connect to Kyuubi server via RESTful API to submit a raw Spark application on YARN, which will execute a query on Hudi and save the resulting data on HDFS.


The query service blocks during execution and must monitor the specific SQL query process until it successfully ends. Once the query is complete, the service retrieves the resulting Excel file locally, makes any necessary preparations, and sends it to the business side.