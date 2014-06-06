/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package org.slf4j.impl;

import java.util.Map;

import junit.framework.Assert;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import com.datatorrent.stram.StramChild;
import com.datatorrent.stram.StreamingAppMaster;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.client.DTConfiguration;

public class DTLoggerFactoryTest
{

  @Test
  public void test()
  {
    System.setProperty(DTLoggerFactory.DT_LOGGERS_LEVEL, "com.datatorrent.stram.client.*:INFO,com.datatorrent.stram.api.*:DEBUG");
    DTLoggerFactory.getInstance().initialize();

    LoggerFactory.getLogger(DTConfiguration.class);
    LoggerFactory.getLogger(StramEvent.class);
    LoggerFactory.getLogger(StreamingAppMaster.class);

    org.apache.log4j.Logger dtConfigLogger = LogManager.getLogger(DTConfiguration.class);
    Assert.assertEquals(dtConfigLogger.getLevel(), Level.INFO);

    org.apache.log4j.Logger stramEventLogger = LogManager.getLogger(StramEvent.class);
    Assert.assertEquals(stramEventLogger.getLevel(), Level.DEBUG);

    org.apache.log4j.Logger streamingAppMasterLogger = LogManager.getLogger(StreamingAppMaster.class);
    Assert.assertNull(streamingAppMasterLogger.getLevel());
  }

  @Test
  public void test1()
  {
    Map<String, String> changes = Maps.newHashMap();
    changes.put("com.datatorrent.*", "DEBUG");
    DTLoggerFactory.getInstance().changeLoggersLevel(changes);

    LoggerFactory.getLogger(DTConfiguration.class);
    LoggerFactory.getLogger(StramEvent.class);

    org.apache.log4j.Logger dtConfigLogger = LogManager.getLogger(DTConfiguration.class);
    Assert.assertEquals(dtConfigLogger.getLevel(), Level.DEBUG);

    org.apache.log4j.Logger stramEventLogger = LogManager.getLogger(StramEvent.class);
    Assert.assertEquals(stramEventLogger.getLevel(), Level.DEBUG);

    LoggerFactory.getLogger(StramChild.class);
    org.apache.log4j.Logger stramChildLogger = LogManager.getLogger(StramChild.class);
    Assert.assertEquals(stramChildLogger.getLevel(), Level.DEBUG);
  }
}
