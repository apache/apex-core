/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 * 
*/

/**
 * <b>com.malhartech.dag</b> package deals with data nodes, tuple processing, serialization, streams, window boundaries etc.<p>
 * <br>
 * All code needed that is executed by Stram to run the dag in Hadoop. Once you have designed your DAG, and designed what each node would do, you would need to write your code by deriving your classes from
 * dag package.<br>
 * <br>
 * <br>
 * The streaming platform would take care of the following for you<br>(TBD, add "How it will")<br>
 * - Standard partitioning (round robin, sticky key). You can add  your own partitioning.<br>
 * - End of window statistics collection in terms of number of tuples, bandwidth, I/O etc<br>
 * - Ensuring the the emitted tuple reaches the downstream nodes<br>
 * - Queueing tuples and retaining them till all future downstream nodes have consumed it<br>
 * <br>
 * <br>
 * There are pre-defined library nodes that you can use: see ...<br>
 * Examples are in the following packages<br>
 * com.malhartech.example.wordcount<br>
 * com.malhattech.example.twitter<br>
 * com.malhartech.example.ads<br>
 * <br>
 * com.malhartech.dag package contains the following classes<br>
 * <b>AbstractNode</b>: The base class for node implementation<br>
 * <b>BackupAgent</b>: Interface that defines how to write checkpoint state<br>
 * <b>BlackHole</b>: To send tuples to no where<br>
 * <b>Context</b>: The base interface for context for all of the streaming platform objects<br>
 * <b>DAGPart</b>: The common base interface for runtime objects of streams and nodes<<br>
 * <b>DefaultSerDe</b>: Default SerDe for streams if nothing is configured<br>
 * <b>EndStreamTuple</b>: Defines end of streaming tuple<br>
 * <b>EndWindowTuple</b>: End of window tuple<br>
 * <b>HeartbeatCounters</b>: Data for heartbeat from node to stram<br>
 * <b>InputAdapter</b>: InputAdapter for streams that are inbound from outside (to be changed)<br>
 * 
 * 
 */

package com.malhartech.dag;


