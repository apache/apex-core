Apache Apex
================================================================================

Apex is a Hadoop YARN native big data processing platform, enabling real time stream as well as batch processing for your big data.  Apex provides the following benefits:

* High scalability and performance
* Fault tolerance and state management
* Hadoop-native YARN & HDFS implementation
* Event processing guarantees
* Separation of functional and operational concerns
* Simple API supports generic Java code

Platform has been demonstated to scale linearly across Hadoop clusters under extreme loads of billions of events per second.  Hardware and process failures are quickly recovered with HDFS-backed checkpointing and automatic operator recovery, preserving application state and resuming execution in seconds.  Functional and operational specifications are separated.  Apex provides a simple API, which enables users to write generic, reusable code.  The code is dropped in as-is and platform automatically handles the various operational concerns, such as state management, fault tolerance, scalability, security, metrics, etc.  This frees users to focus on functional development, and lets platform provide operability support.

The core Apex platform is supplemented by Malhar, a library of connector and logic functions, enabling rapid application development.  These operators and modules provide access to HDFS, S3, NFS, FTP, and other file systems; Kafka, ActiveMQ, RabbitMQ, JMS, and other message systems; MySql, Cassandra, MongoDB, Redis, HBase, CouchDB, generic JDBC, and other database connectors.  In addition to the operators, the library contains a number of demos applications, demonstrating operator features and capabilities.  To see the full list of available operators and related documentation, visit [Apex Malhar on Github](https://github.com/apache/apex-malhar)

For additional information visit [Apache Apex](http://apex.apache.org/).

[![](favicon.ico)](http://apex.apache.org/)
