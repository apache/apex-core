/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 *
*/

/**
 * <b>com.malhartech.engine</b> package deals with data operators, tuple processing, serialization, streams, window boundaries etc.<p>
 * <br>
 * All code needed that is executed by Stram to run the dag in Hadoop. Once you have designed your DAG, and designed what each node would do, you would need to write your code by deriving your classes from
 * dag package.<br>
 * <br>
 * <br>
 * The streaming platform would take care of the following for you<br>(TBD, add "How it will")<br>
 * - Standard partitioning (round robin, sticky key). You can add  your own partitioning.<br>
 * - End of window statistics collection in terms of number of tuples, bandwidth, I/O etc<br>
 * - Ensuring the the emitted tuple reaches the downstream operators<br>
 * - Queueing tuples and retaining them till all future downstream operators have consumed it<br>
 * <br>
 * <br>
 * There are pre-defined library operators that you can use: see ...<br>
 * Examples are in the following packages<br>
 * <b>com.malhartech.example.wordcount</b><br>
 * <b>com.malhattech.example.twitter</b><br>
 * <b>com.malhartech.example.ads</b><br>
 * <br>
 * com.malhartech.engine package contains the following classes<br>
 * <b>{@link com.malhartech.engine.AbstractInputNode}</b>: <b>TBD</b>:The input adapter class<br>
 * <b>{@link com.malhartech.engine.AbstractNode}</b>: The base class for node implementation. Emits and consumes tuples<br>
 * <b>{@link com.malhartech.engine.Blackhole}</b>: To send tuples to no where<br>
 * <b>{@link com.malhartech.api.Component}</b>: <b>TBD</b><br>
 * <b>{@link com.malhartech.api.Context}</b>: The base interface for context for all of the streaming platform objects<br>
 * <b>{@link com.malhartech.engine.DefaultNodeSerDe}</b>: <b>TBD</b><br>
 * <b>{@link com.malhartech.engine.DefaultSerDe}</b>: Default SerDe for streams if nothing is configured<br>
 * <b>{@link com.malhartech.engine.EndStreamTuple}</b>: Defines end of streaming tuple<br>
 * <b>{@link com.malhartech.engine.EndWindowTuple}</b>: End of window tuple<br>
 * <b>{@link com.malhartech.engine.HeartbeatCounters}</b>: Data for heartbeat from node to stram<br>
 * <b>{@link com.malhartech.engine.Node}</b>: TBD<br>
 * <b>{@link com.malhartech.engine.NodeConfiguration}</b>: Extends {@link org.apache.hadoop.conf.Configuration} for operators of the dag<br>
 * <b>{@link com.malhartech.engine.NodeContext}</b>: The for context for all of the operators<br>
 * <b>{@link com.malhartech.engine.NodeSerDe}</b>: <b>TBD</b><br>
 * <b>{@link com.malhartech.engine.ResetWindowTuple}</b>: Resets window id<br>
 * <b>{@link com.malhartech.engine.SerDe}</b>: Serializing and Deserializing the data tuples and controlling the partitioning<<br>
 * <b>{@link com.malhartech.api.Sink}</b>:
 * <b>{@link com.malhartech.engine.Stream}</b>: Base interface for all streaming in the streaming platform<br>
 * <b>{@link com.malhartech.engine.StreamConfiguration}</b>: Configuration object provided per stream object<br>
 * <b>{@link com.malhartech.engine.StreamContext}</b>: Defines the destination for tuples processed<br>
 * <b>{@link com.malhartech.engine.Tuple}</b>: Basic object to be streamed<br>
 * <br>
 *
 *
 */

package com.malhartech.engine;


