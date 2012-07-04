/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Server;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class StreamConfiguration extends Configuration
{
  public InetSocketAddress getSourceSocketAddress()
  {
    return this.getSocketAddr("source", "localhost", Server.DEFAULT_PORT);
  }

  SocketAddress getSinkSocketAddress()
  {
    return this.getSocketAddr("sink", "localhost", Server.DEFAULT_PORT);
  }
}
