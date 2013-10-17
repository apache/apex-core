/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */

/**
 *
 * <b>com.datatorrent.stram.cli</b> package deals with code for command line interface for the streaming platform<p>
 * <br>
 * The cli code wraps the webservices layer (<b>com.datatorrent.stream.webapp</b>) and thus accesses the dag
 * via one single point. All webservice calls for streaming data go through to the stram.<br>
 * <br>
 * The com.datatorrent.stram.cli package consists of<br>
 * <b>{@link com.datatorrent.stram.cli.DTCli}</b>: Provides command line interface for a streaming application on hadoop (yarn)<br>
 * <br>
 *
 */
package com.datatorrent.stram.cli;


