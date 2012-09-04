/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 * 
 */

/** 
 * <b>com.malhartech.stram</b> package contains all code for streaming application master<p>
 * <br>
 * The application master is also called <b>STRAM</b><br>
 * (TBD - To explain all functionality)<br>
 * <br>
 * <s><b>{@link com.malhartech.stram.AdapterWrapperNode}</b>: Wrapper node to connects adapter "stream" to another source or sink stream</s><br>
 * <b>{@link com.malhartech.stram.DNodeManager}</b>: Tracks topology provisioning/allocation to containers<br>
 * <b>{@link com.malhartech.stram.LaunchContainerRunnable}</b>: Runnable to connect to the {@link org.apache.hadoop.yarn.api.ContainerManager} and launch the container that will host streaming nodes<br>
 * <b>{@link com.malhartech.stram.NodeDeployInfo}</b>: <b>TBD</b><br>
 * <b>{@link com.malhartech.stram.StramAppContext}</b>: Context interface for sharing information across components in YARN App<br>
 * <b>{@link com.malhartech.stram.StramAppMaster}</b>: Streaming Application Master<br>
 * <b>{@link com.malhartech.stram.StramChild}</b>: The main() for streaming node processes launched by {@link com.malhartech.stram.StramAppMaster}<br>
 * <b>{@link com.malhartech.stram.StramChildAgent}</b>: Representation of a child container in the master<br>
 * <b>{@link com.malhartech.stram.StramClient}</b>: Submits application to YARN<br>
 * <b>{@link com.malhartech.stram.StramConstants}</b>: Placeholder for constants to be used by Stram/Dag<br>
 * <b>{@link com.malhartech.stram.StramUtils}</b>: Utilities for shared use in Stram components<br>
 * <s><b>{@link com.malhartech.stram.StreamPConf}</b>: Definition of stream connecting 2 nodes either inline or via buffer server</s><br>
 * <b>{@link com.malhartech.stram.StreamingNodeParent}</b>: Stram side implementation of communication protocol with hadoop container<br>
 * <b>{@link com.malhartech.stram.StreamingNodeUmbilicalProtocol}</b>: Classes and code for communication protocol between streaming node child process and stram<br>
 * <b>{@link com.malhartech.stram.TopologyDeployer}</b>: Classes and code that derives the physical model from the logical dag and assigned to hadoop container. Is the initial query planner<br>
 * <b>{@link com.malhartech.stram.WindowGenerator}</b>: Runs in the hadoop container of the input adapters and generates windows<br>
 * <br>  
 * 
 */

package com.malhartech.stram;
