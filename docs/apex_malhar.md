Apache Apex Malhar
================================================================================

Apache Apex Malhar is an open source operator and codec library that can be used with the Apache Apex platform to build real-time streaming applications.  As part of enabling enterprises extract value quickly, Malhar operators help get data in, analyze it in real-time and get data out of Hadoop in real-time with no paradigm limitations.  In addition to the operators, the library contains a number of demos applications, demonstrating operator features and capabilities.

![MalharDiagram](images/MalharOperatorOverview.png)

# Capabilities common across Malhar operators

For most streaming platforms, connectors are afterthoughts and often end up being simple ‘bolt-ons’ to the platform. As a result they often cause performance issues or data loss when put through failure scenarios and scalability requirements. Malhar operators do not face these issues as they were designed to be integral parts of apex*.md RTS. Hence, they have following core streaming runtime capabilities

1.  **Fault tolerance** – Apache Apex Malhar operators where applicable have fault tolerance built in. They use the checkpoint capability provided by the framework to ensure that there is no data loss under ANY failure scenario.
2.  **Processing guarantees** – Malhar operators where applicable provide out of the box support for ALL three processing guarantees – exactly once, at-least once & at-most once WITHOUT requiring the user to write any additional code.  Some operators like MQTT operator deal with source systems that cant track processed data and hence need the operators to keep track of the data. Malhar has support for a generic operator that uses alternate storage like HDFS to facilitate this. Finally for databases that support transactions or support any sort of atomic batch operations Malhar operators can do exactly once down to the tuple level.
3.  **Dynamic updates** – Based on changing business conditions you often have to tweak several parameters used by the operators in your streaming application without incurring any application downtime. You can also change properties of a Malhar operator at runtime without having to bring down the application.
4.  **Ease of extensibility** – Malhar operators are based on templates that are easy to extend.
5.  **Partitioning support** – In streaming applications the input data stream often needs to be partitioned based on the contents of the stream. Also for operators that ingest data from external systems partitioning needs to be done based on the capabilities of the external system. E.g. With the Kafka or Flume operator, the operator can automatically scale up or down based on the changes in the number of Kafka partitions or Flume channels

# Operator Library Overview

## Input/output connectors

Below is a summary of the various sub categories of input and output operators. Input operators also have a corresponding output operator

*   **File Systems** – Most streaming analytics use cases we have seen require the data to be stored in HDFS or perhaps S3 if the application is running in AWS. Also, customers often need to re-run their streaming analytical applications against historical data or consume data from upstream processes that are perhaps writing to some NFS share. Hence, it’s not just enough to be able to save data to various file systems. You also have to be able to read data from them. RTS supports input & output operators for HDFS, S3, NFS & Local Files
*   **Flume** – NOTE: Flume operator is not yet part of Malhar

Many customers have existing Flume deployments that are being used to aggregate log data from variety of sources. However Flume does not allow analytics on the log data on the fly. The Flume input/output operator enables RTS to consume data from flume and analyze it in real-time before being persisted.

*   **Relational databases** – Most stream processing use cases require some reference data lookups to enrich, tag or filter streaming data. There is also a need to save results of the streaming analytical computation to a database so an operational dashboard can see them. RTS supports a JDBC operator so you can read/write data from any JDBC compliant RDBMS like Oracle, MySQL etc.
*   **NoSQL databases** –NoSQL key-value pair databases like Cassandra & HBase are becoming a common part of streaming analytics application architectures to lookup reference data or store results. Malhar has operators for HBase, Cassandra, Accumulo (common with govt. & healthcare companies) MongoDB & CouchDB.
*   **Messaging systems** – JMS brokers have been the workhorses of messaging infrastructure in most enterprises. Also Kafka is fast coming up in almost every customer we talk to. Malhar has operators to read/write to Kafka, any JMS implementation, ZeroMQ & RabbitMQ.
*   **Notification systems** – Almost every streaming analytics application has some notification requirements that are tied to a business condition being triggered. Malhar supports sending notifications via SMTP & SNMP. It also has an alert escalation mechanism built in so users don’t get spammed by notifications (a common drawback in most streaming platforms)
*   **In-memory Databases & Caching platforms** - Some streaming use cases need instantaneous access to shared state across the application. Caching platforms and in-memory databases serve this purpose really well. To support these use cases, Malhar has operators for memcached & Redis
*   **Protocols** - Streaming use cases driven by machine-to-machine communication have one thing in common – there is no standard dominant protocol being used for communication. Malhar currently has support for MQTT. It is one of the more commonly, adopted protocols we see in the IoT space. Malhar also provides connectors that can directly talk to HTTP, RSS, Socket, WebSocket & FTP sources



## Compute

One of the most important promises of a streaming analytics platform like Apache Apex is the ability to do analytics in real-time. However delivering on the promise becomes really difficult when the platform does not provide out of the box operators to support variety of common compute functions as the user then has to worry about making these scalable, fault tolerant etc. Malhar takes this responsibility away from the application developer by providing a huge variety of out of the box computational operators. The application developer can thus focus on the analysis.

Below is just a snapshot of the compute operators available in Malhar

*   Statistics & Math - Provide various mathematical and statistical computations over application defined time windows.
*   Filtering & pattern matching
*   Machine learning & Algorithms
*   Real-time model scoring is a very common use case for stream processing platforms. &nbsp;Malhar allows users to invoke their R models from streaming applications
*   Sorting, Maps, Frequency, TopN, BottomN, Random Generator etc.


## Query & Script invocation

Many streaming use cases are legacy implementations that need to be ported over. This often requires re-use some of the existing investments and code that perhaps would be really hard to re-write. With this in mind, Malhar supports invoking external scripts and queries as part of the streaming application using operators for invoking SQL query, Shell script, Ruby, Jython, and JavaScript etc.

## Parsers

There are many industry vertical specific data formats that a streaming application developer might need to parse. Often there are existing parsers available for these that can be directly plugged into an Apache Apex application. For example in the Telco space, a Java based CDR parser can be directly plugged into Apache Apex operator. To further simplify development experience, Malhar also provides some operators for parsing common formats like XML (DOM & SAX), JSON (flat map converter), Apache log files, syslog, etc.

## Stream manipulation

Streaming data aka ‘stream’ is raw data that inevitably needs processing to clean, filter, tag, summarize etc. The goal of Malhar is to enable the application developer to focus on ‘WHAT’ needs to be done to the stream to get it in the right format and not worry about the ‘HOW’. Hence, Malhar has several operators to perform the common stream manipulation actions like – DeDupe, GroupBy, Join, Distinct/Unique, Limit, OrderBy, Split, Sample, Inner join, Outer join, Select, Update etc.

## Social Media

Malhar includes an operator to connect to the popular Twitter stream fire hose.
