/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.connectlet;

import com.googlecode.connectlet.Connector;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Bootstrap
{
  public static final ConcurrentHashMap<String, Connector> connectors = new ConcurrentHashMap<String, Connector>(2);

  public static void shutdown()
  {
  }

}
