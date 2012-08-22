/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */

/**
 * 
 * <b>com.malhartech.stram.cli</b> package deals with code for command line interface for the streaming platform<p>
 * <br>
 * The cli code wraps the webservices layer (<b>com.malhartech.stream.webapp</b>) and thus all access to the start of the current
 * dag is via one single point. All webservice calls for streaming data go through the stram.<br>
 * <br>
 * The CLI package consists of<br>
 * <b>StramAppLauncher</b>: Launch a streaming application packaged as jar file<br>
 * <b>StramCli</b>: Provides command line interface for a streaming application on hadoop (yarn)<br>
 * <b>StramClientUtils</b> (Yarn Client Helper, Resource Mgr Client Helper): Collection of utility classes for command line interface package
 * <br>
 * 
 */
package com.malhartech.stram.cli;


