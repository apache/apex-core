/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 * 
 * com.malhartech.stream package contains all code related to various implementations of Stream interface. A stream is a logical unit of a dag that defines the connection between
 * a node and list of listener nodes. Stream has the following properties in Malhar's streaming platform
 * - One writer node
 * - Any number of listener nodes
 * - Properties defined by the context as per the dag specification
 * 
 * The streams included in com.malhartech.stream include
 * 
 * AbstractActiveMQInputStream: Provides implementation to read from ActiveMQ. Users need to provide getObject implementation. (See example in InputActiveMQStreamTest)
 * ActiveMQOutputStream: TBD
 * AbstractHDFSInputStream: Provides implementation of reading from HDFS. Users need to probide getRecord implementation. (See example of HDFSInputStream in com.malhartech.example.wordcount)
 * BufferServerInputStream: 
 * BufferServerOutputStream:
 * ConsoleOutputStream:
 * HDFSOutputStream:
 * InlineStream:
 * KafkaInputStream:
 * KafkaOutputStream:
 * SocketInputStream:
 * SocketOutputStream:
 * 
 * 
 * A tuple emitted by the writer node is automatically routed to the consumer nodes by the streaming platform
 * 
 */

package com.malhartech.stream;

