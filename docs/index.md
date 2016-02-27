Apache Apex (Incubating)
================================================================================

Apex is a Hadoop YARN native big data processing platform, enabling real time stream as well as batch processing for your big data.  Apex provides the following benefits:

* High scalability and performance
* Fault tolerance and state management
* Hadoop-native YARN & HDFS implementation
* Event processing guarantees
* Separation of functional and operational concerns
* Simple API supports generic Java code

Platform has been demonstated to scale linearly across Hadoop clusters under extreme loads of billions of events per second.  Hardware and process failures are quickly recovered with HDFS-backed checkpointing and automatic operator recovery, preserving application state and resuming execution in seconds.  Functional and operational specifications are separated.  Apex provides a simple API, which enables users to write generic, reusable code.  The code is dropped in as-is and platform automatically handles the various operational concerns, such as state management, fault tolerance, scalability, security, metrics, etc.  This frees users to focus on functional development, and lets platform provide operability support.

The core Apex platform is supplemented by Malhar, a library of connector and logic functions, enabling rapid application development.  These operators and modules provide access to HDFS, S3, NFS, FTP, and other file systems; Kafka, ActiveMQ, RabbitMQ, JMS, and other message systems; MySql, Cassandra, MongoDB, Redis, HBase, CouchDB, generic JDBC, and other database connectors. The Malhar library also includes a host of other common business logic patterns that help users to significantly reduce the time it takes to go into production.  Ease of integration with all other big data technologies is one of the primary missions of Malhar.


For additional information visit [Apache Apex (incubating)](http://apex.incubator.apache.org/).

[![](favicon.ico)](http://apex.incubator.apache.org/)
