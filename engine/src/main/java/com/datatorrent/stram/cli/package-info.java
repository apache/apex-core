/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */

/**
 * 
 * <b>com.malhartech.stram.cli</b> package deals with code for command line interface for the streaming platform<p>
 * <br>
 * The cli code wraps the webservices layer (<b>com.malhartech.stream.webapp</b>) and thus accesses the dag
 * via one single point. All webservice calls for streaming data go through to the stram.<br>
 * <br>
 * The com.malhartech.stram.cli package consists of<br>
 * <b>{@link com.malhartech.stram.cli.StramAppLauncher}</b>: Launch a streaming application packaged as jar file<br>
 * <b>{@link com.malhartech.stram.cli.StramCli}</b>: Provides command line interface for a streaming application on hadoop (yarn)<br>
 * <b>{@link com.malhartech.stram.cli.StramClientUtils}</b> (Yarn Client Helper, Resource Mgr Client Helper): Collection of utility classes for command line interface package
 * <br>
 * 
 */
package com.malhartech.stram.cli;


