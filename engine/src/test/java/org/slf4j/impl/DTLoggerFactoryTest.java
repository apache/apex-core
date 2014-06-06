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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import com.datatorrent.stram.StreamingAppMaster;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.client.DTConfiguration;

public class DTLoggerFactoryTest
{

  public static class Dummy
  {
  }
  @Test
  public void test()
  {
    String loggersLevel = "com.datatorrent.stram.client.*:INFO,com.datatorrent.stram.api.*:DEBUG";
    if (!Strings.isNullOrEmpty(loggersLevel)) {
      Map<String, String> targetChanges = Maps.newHashMap();
      String targets[] = loggersLevel.split(",");
      for (String target : targets) {
        String parts[] = target.split(":");
        targetChanges.put(parts[0], parts[1]);
      }
      DTLoggerFactory.getInstance().changeLoggersLevel(targetChanges);
    }
    LoggerFactory.getLogger(DTConfiguration.class);
    LoggerFactory.getLogger(StramEvent.class);
    LoggerFactory.getLogger(StreamingAppMaster.class);
    LoggerFactory.getLogger(Dummy.class);

    org.apache.log4j.Logger dtConfigLogger = LogManager.getLogger(DTConfiguration.class);
    Assert.assertEquals(dtConfigLogger.getLevel(), Level.INFO);

    org.apache.log4j.Logger stramEventLogger = LogManager.getLogger(StramEvent.class);
    Assert.assertEquals(stramEventLogger.getLevel(), Level.DEBUG);

    org.apache.log4j.Logger streamingAppMasterLogger = LogManager.getLogger(StreamingAppMaster.class);
    Assert.assertNull(streamingAppMasterLogger.getLevel());

    org.apache.log4j.Logger dummyLogger = LogManager.getLogger(Dummy.class);
    Assert.assertNull(dummyLogger);

  }
}
