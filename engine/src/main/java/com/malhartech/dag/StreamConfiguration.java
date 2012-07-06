/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;

import com.malhartech.bufferserver.Server;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class StreamConfiguration extends Configuration
{
  public static final String SERVER_ADDRESS = "bufferserver";

  public InetSocketAddress getBufferServerAddress()
  {
    return this.getSocketAddr(SERVER_ADDRESS, "localhost", Server.DEFAULT_PORT);
  }

}
