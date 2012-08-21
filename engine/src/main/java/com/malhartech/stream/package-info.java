
/*
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */

/**
 * 
 * com.malhartech.stream package contains all code related to various implementations of Stream interface.<br>
 * <br>
 * A stream is a logical unit of a dag that defines the connection between
 * a node and list of listener nodes. Stream has the following properties in Malhar's streaming platform<br>
 * - One writer node<br>
 * - Any number of listener nodes<br>
 * - Properties defined by the context as per the dag specification<br>
 * <br>
 * <br>
 * The streams included in com.malhartech.stream include<br>
 * <br>
 * <br><b>ActiveMQ Interface Streams</b><br>
 * <b>AbstractActiveMQInputStream</b>: Provides implementation to read from ActiveMQ by  extending AbstractInputAdapter. Users need to provide getObject implementation. (See example in InputActiveMQStreamTest)<br>
 * <b>ActiveMQOutputStream</b>: TBD<br>
 * <br>
 * <b>HDFS Streams</b><br>
 * <b>AbstractHDFSInputStream</b>: Provides implementation of reading from HDFS. Users need to probide getRecord implementation. (See example of HDFSInputStream in com.malhartech.example.wordcount)<br>
 * <b>HDFSOutputStream</b>: Provides implementation for writing to to HDFS<br>
 * 
 * <br><b>Buffer Server Streams</b><br>
 * <b>BufferServerInputStream</b>: extends SocketInputStream, takes data from buffer server into the node. Every logical stream will have at least two such objects (BufferServerInputStream
 *  and BufferServerOutputStream). If the logical stream gets partitioned into physical streams then each of these physical streams will have these objects. Inlined version of
 *  a logical stream does not go through the buffer server and hence would not have BufferServerStream objects<br>
 * <b>BufferServerOutputStream</b>: extends SocketOutputStream and in conjunction with BufferServerInputStream forms a complete stream in a node->buffer server->node path<br>
 * 
 * <br><b>Console Streams</b><br>
 * <b>ConsoleOutputStream</b>: Extends Stream class. Writes directly to stdout. The data would show up in the stdout of Hadoop container in which the node runs. This
 *  is a very good way to debug. Care must be taken to avoid connecting ConsoleOutputStream to an output of a node with high throughput<br>
 * 
 * <br><b>Inline Stream (Within a Hadoop Container)</b><br>
 * <b>InlineStream</b>: Streams data between two nodes in inline mode. This instance of stream object does not have connection to BufferServer and cannot be persisted.<br>
 * 
 * <br><b>Kafka Interface Streams</b><br>
 * <b>KafkaInputStream</b>: Extends AbstractInputAdapter, and provides implementation for a node to read from Kafka.<br>
 * <b>KafkaOutputStream</b>: Implements Stream, and Sink classes to provide implementation for a node to write to Kafka.<br>
 * 
 * <br><b>Socket Interface Streams</b><br>
 * <b>SocketInputStream</b>: Implements Stream and provides basic stream connection for a node to read from a socket. Users can use this class if they want to directly connect to a outside socket<br>
 * <b>SocketOutputStream</b>: Implements Stream and provides basic stream connection for a node to write to a socket. Most likely users would not use it to write to a socket by themselves.
 *   Would be used in adapters and via BufferServerOutputStream<br>
 * 
 * 
 * 
 */

package com.malhartech.stream;

