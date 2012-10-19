/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Server;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * Configuration object provided per stream object<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class StreamConfiguration extends Configuration
{
  public static final String SERVER_ADDRESS = "bufferserver";

  public StreamConfiguration()
  {
  }

  public InetSocketAddress getBufferServerAddress()
  {
    return this.getSocketAddr(SERVER_ADDRESS, "localhost", Server.DEFAULT_PORT);
  }
}
