/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package org.apache.log4j;

import java.lang.reflect.Field;
import java.util.Map;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.StreamingAppMaster;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.client.DTConfiguration;
import com.datatorrent.stram.engine.StreamingContainer;
import com.google.common.collect.Maps;

public class DTLoggerFactoryTest
{

  @BeforeClass
  public static void setup() throws Exception
  {
    System.setProperty(DTLoggerFactory.DT_LOGGERS_LEVEL, "com.datatorrent.stram.client.*:INFO,com.datatorrent.stram.api.*:DEBUG");
    Field f = DTLoggerFactory.class.getDeclaredField("initialized");
    f.setAccessible(true);
    f.set(DTLoggerFactory.getInstance(), false);
    DTLoggerFactory.getInstance().initialize();
  }

  @Test
  public void test()
  {
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

    LoggerFactory.getLogger(StreamingContainer.class);
    org.apache.log4j.Logger stramChildLogger = LogManager.getLogger(StreamingContainer.class);
    Assert.assertEquals(stramChildLogger.getLevel(), Level.DEBUG);
  }
}
