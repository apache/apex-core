/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 *
 * <b>com.datatorrent.stream</b> package contains all code related to various implementations of Stream interface<p>
 * <br>
 * A stream is a logical unit of a dag that defines the connection between
 * a node and list of listener operators. Stream has the following properties in Malhar's streaming platform<br>
 * - One writer node<br>
 * - Any number of listener operators<br>
 * - Context as defined by the properties specified in the dag<br>
 * A stream definition in the dag is a logical definition. Multiple logical listerner operators means that the emitted tuple
 * would reach each of them. Partitioning is done when a single
 * logical listener node partitions into multilpe physical operators. This may happen due to initial user
 * specification, or dynamic run time constraint enforcement. In such a scenerio the logical stream gets partitioned
 * into physical streams. Each physical stream would retain the
 * characteristics of the logical node (one writer, multiple readers, and context).<br>
 * <br>
 * The streams included in com.datatorrent.stream include<br>
 * <br>
 * <br><b>Buffer Server Streams</b><br>
 * <b>{@link com.datatorrent.stream.BufferServerInputStream}</b>: extends {@link com.datatorrent.stream.SocketInputStream},
 * takes data from buffer server into the node. Every logical stream will have at least two such
 * objects ({@link com.datatorrent.stream.BufferServerInputStream}
 *  and {@link com.datatorrent.stream.BufferServerOutputStream}). If the logical stream gets partitioned
 * into physical streams then each of these physical streams will have these objects. Inlined version of
 *  a logical stream does not go through the buffer server and hence would not have
 * {@link com.datatorrent.stream.BufferServerInputStream} and {@link com.datatorrent.stream.BufferServerOutputStream} objects<br>
 * <b>{@link com.datatorrent.stream.BufferServerOutputStream}</b>: extends {@link com.datatorrent.stream.SocketOutputStream}
 * and in conjunction with {@link com.datatorrent.stream.BufferServerInputStream} forms a complete stream
 * in a node->buffer server->node path<br>
 * <br><b>Inline Stream (Within a Hadoop Container)</b><br>
 * <b>{@link com.datatorrent.stream.InlineStream}</b>: Streams data between two operators in inline mode. This implementation of
 * {@link com.datatorrent.engine.Stream} and {{@link com.datatorrent.api.Sink}
 * interface does not have connection to BufferServer and cannot be persisted.<br>
 *
 * <b>{@link com.datatorrent.stream.MuxStream}</b>: <br>
 * <b>{@link com.datatorrent.stream.PartitionAwareSink}</b>: <br>
 *
 * <br><b>Socket Interface Streams</b><br>
 * <b>{@link com.datatorrent.stream.SocketInputStream}</b>: Implements {@link com.datatorrent.engine.Stream} interface and provides
 * basic stream connection for a node to read from a socket. Users can use this class if they want to directly connect to
 * a outside socket<br>
 * <b>{@link com.datatorrent.stream.SocketOutputStream}</b>: Implements {@link com.datatorrent.engine.Stream} interface and provides
 * basic stream connection for a node to write to a socket. Most likely users would not use it to write to a socket by themselves.
 *   Would be used in adapters and via {@link com.datatorrent.stream.BufferServerOutputStream}<br>
 * <br>
 *
 */

package com.datatorrent.stram.stream;

