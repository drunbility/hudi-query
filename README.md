A simple data lake query service that can query Apache Hudi and collect results in Excel format.

The query service pulls messages periodically from Apache RocketMQ, which indicate the query SQL. The service then sends the SQL to the Kyuubi server for processing. 

The query service can connect to the Kyuubi server via JDBC client to execute SQL directly and obtain the result. Alternatively, it can also connect to the Kyuubi server via a RESTful API to submit a raw Spark on YARN application, execute the query on Hudi, and save the result on HDFS.


The query service blocks during execution and must monitor the specific SQL query process until it successfully ends. Once the query is complete, the service retrieves the resulting Excel file locally, makes any necessary preparations, and sends it to the business side.