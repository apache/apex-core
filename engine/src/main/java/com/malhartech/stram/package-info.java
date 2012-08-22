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
 * <b>StramConstants</b>: Placeholder for constants to be used by Stram/Dag<br>
 * <b>StramUtils</b>: Utilities for shared use in Stram components<br>
 * <b>StreamPConf</b>: Definition of stream connecting 2 nodes either inline or via buffer server<br>
 * <b>StreamingNodeParent</b>: Stram side implementation of communication protocol with hadoop container<br>
 * <b>StreamingNodeUmbilicalProtocol</b>: Classes and code for communication protocol between streaming node child process and stram<br>
 * <b>TopologyGenerator</b>: Classes and code that derives the physical model from the logical dag and assigned to hadoop container. Is the initial query planner<br>
 * <b>WindowGenerator</b>: Runs in the hadoop container of the input adapters and generates windows<br>
 * <br>  
 * 
 */

package com.malhartech.stram;
