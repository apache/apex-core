/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Server;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;

/**
 * Configuration object provided per stream object.
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class StreamConfiguration extends Configuration
{
  public static final String SERVER_ADDRESS = "bufferserver";
  public static final String START_WINDOW_MILLIS = "startWindowsMillis";
  public static final String WINDOW_SIZE_MILLIS = "windowSizeMillis";

  /**
   * Stream class name, required for input/output adapter
   */
  public static final String STREAM_CLASSNAME = "streamClassName";
  
  public InetSocketAddress getBufferServerAddress()
  {
    return this.getSocketAddr(SERVER_ADDRESS, "localhost", Server.DEFAULT_PORT);
  }

  public long getWindowSizeMillis() {
    return this.getLong(WINDOW_SIZE_MILLIS, 0);
  }
  
  public long getStartWindowMillis() {
    return this.getLong(START_WINDOW_MILLIS, 0);
  }
  
}
