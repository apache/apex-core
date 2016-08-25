/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.log4j;

import java.lang.reflect.Field;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import com.datatorrent.stram.StreamingAppMaster;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.client.DTConfiguration;
import com.datatorrent.stram.engine.StreamingContainer;

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
    changes.put("com.datatorrent.stram.engine.*", "ERROR");
    DTLoggerFactory.getInstance().changeLoggersLevel(changes);

    LoggerFactory.getLogger(DTConfiguration.class);
    LoggerFactory.getLogger(StramEvent.class);

    org.apache.log4j.Logger dtConfigLogger = LogManager.getLogger(DTConfiguration.class);
    Assert.assertEquals(dtConfigLogger.getLevel(), Level.DEBUG);

    org.apache.log4j.Logger stramEventLogger = LogManager.getLogger(StramEvent.class);
    Assert.assertEquals(stramEventLogger.getLevel(), Level.DEBUG);

    LoggerFactory.getLogger(StreamingContainer.class);
    org.apache.log4j.Logger stramChildLogger = LogManager.getLogger(StreamingContainer.class);
    Assert.assertEquals(stramChildLogger.getLevel(), Level.ERROR);
  }

  @Test
  public void testGetPatternLevels()
  {
    Map<String, String> changes = Maps.newHashMap();
    changes.put("com.datatorrent.io.fs.*", "DEBUG");
    changes.put("com.datatorrent.io.*", "ERROR");
    DTLoggerFactory.getInstance().changeLoggersLevel(changes);

    Map<String, String> levels = DTLoggerFactory.getInstance().getPatternLevels();

    Assert.assertEquals(levels.get("com.datatorrent.io.fs.*"), "DEBUG");
    Assert.assertEquals(levels.get("com.datatorrent.io.*"), "ERROR");
  }
}
