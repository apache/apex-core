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
 * AbstractActiveMQInputStream: Provides implementation to read from ActiveMQ by  extending AbstractInputAdapter. Users need to provide getObject implementation. (See example in InputActiveMQStreamTest)
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
 * ConsoleOutputStream: Extends Stream class. Writes directly to stdout. The data would show up in the stdout of Hadoop container in which the node runs. This
 *  is a very good way to debug. Care must be taken to avoid connecting ConsoleOutputStream to an output of a node with high throughput
 
 * InlineStream: Streams data between two nodes in inline mode. This instance of stream object does not have connection to BufferServer and cannot be persisted.
 * 
 * KafkaInputStream: Extends AbstractInputAdapter, and provides implementation for a node to read from Kafka.
 * KafkaOutputStream: Implements Stream, and Sink classes to provide implementation for a node to write to Kafka.
 * 
 * SocketInputStream: Implements Stream and provides basic stream connection for a node to read from a socket. Users can use this class if they want to directly connect to a outside socket
 * SocketOutputStream: Implements Stream and provides basic stream connection for a node to write to a socket. Most likely users would not use it to write to a socket by themselves.
 *   Would be used in adapters and via BufferServerOutputStream
 * 
 * 
 * 
 */

package com.malhartech.stream;

