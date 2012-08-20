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
 * 
 * AbstractHDFSInputStream: Provides implementation of reading from HDFS. Users need to probide getRecord implementation. (See example of HDFSInputStream in com.malhartech.example.wordcount)
 *  HDFSOutputStream:
 * 
 * BufferServerInputStream: extends SocketInputStream, takes data from buffer server into the node. Every logical stream will have at least two such objects (BufferServerInputStream
 *  and BufferServerOutputStream). If the logical stream gets partitioned into physical streams then each of these physical streams will have these objects. Inlined version of
 *  a logical stream does not go through the buffer server and hence would not have BufferServerStream objects
 * BufferServerOutputStream: extends SocketOutputStream and in conjunction with BufferServerInputStream forms a complete stream in a node->buffer server->node path
 * 
 * ConsoleOutputStream:
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

