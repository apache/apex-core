/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */

/**
 * 
 * <b>com.malhartech.bufferserver</b> Internal messaging service that implements streams and is window complaint<p>
 * <br>
 * com.malhartech.bufferserver includes<br>
 * <b>{@link com.malhartech.bufferserver.Client}</b>: Sends a list of continent/city pairs to a LocalTimeServer to get the local times of the specified cities<br>
 * <b>{@link com.malhartech.bufferserver.ClientHandler}</b>: This class is called the last while reading the response from server<br>
 * <b>{@link com.malhartech.bufferserver.DataIntrospector}/b>: Looks at the data and provide information about it<br>
 * <b>{@link com.malhartech.bufferserver.DataList}</>: Maintains list of data and manages addition and deletion of the data<br>
 * <b>{@link com.malhartech.bufferserver.DataListIterator}</b>: <br>
 * <b>{@link com.malhartech.bufferserver.DataListner}</b>: Interface that waits for data to be added to the buffer server and then acts on it<br>
 * <b>{@link com.malhartech.bufferserver.LogicalNode}</b>: LogicalNode represents a logical node in a DAG<br>
 * <b>{@link com.malhartech.bufferserver.PhysicalNode}</b>: <br>
 * <b>{@link com.malhartech.bufferserver.ProtobufDataInspector}</b>: <br>
 * <b>{@link com.malhartech.bufferserver.Server}</b>: The buffer server application<br>
 * <b>{@link com.malhartech.bufferserver.ServerHandler}</b>: Handler to serve connections accepted by the server<br>
 * <br>
 */

package com.malhartech.bufferserver;

