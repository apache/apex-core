/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 * 
*/

/** <b>dag</b> package deals with data nodes, tuple processing, serialization, streams, window boundaries etc. All code needed that is executed by Stram to
 * run the dag in Hadoop.. Once you have designed your DAG, and designed what each node would do, you would need to write your code by deriving your classes from
 * dag package.</b>
 *  
 * Steps to follow while writing your own node<br>
 * How to access config parameters...<br>
 * Window-ed data vs non window-ed data...<br>
 * Node state...<br>
 * Persistance...<br>
 * Tuples...<br>
 * Output stream has one writer...<br>
 * 
 * 
 * 
 * The streaming platform would take care of the following for you<br>(TBD, add "How it will")<br>
 * - Standard partitioning (round robin, sticky key). You can add  your own partitioning.<br>
 * - End of window statistics collection in terms of number of tuples, bandwidth, I/O etc<br>
 * - Ensuring the the emitted tuple reaches the downstream nodes<br>
 * - Queueing tuples and retaining them till all future downstream nodes have consumed it<br>
 * 
 * There are pre-defined library nodes that you can use: see ...<br>
 * Examples are in the following packages<br>
 * com.malhartech.example.wordcount<br>
 * com.malhattech.example.twitter<br>
 * com.malhartech.example.ads<br>
 * 
 * 
 * 
 */

package com.malhartech.dag;


