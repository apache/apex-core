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
package com.datatorrent.stram.util;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.LoggerFactory;

import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Category;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.log4j.spi.LoggingEvent;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.stram.client.StramClientUtils;

import static com.datatorrent.api.Context.DAGContext.APPLICATION_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LoggerUtilTest
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerUtilTest.class);
  private static final Logger log4jLogger = LogManager.getLogger(LoggerUtilTest.class);
  private static final Map<AppenderSkeleton, Priority> appenderLevelMap = new HashMap<>();

  @BeforeClass
  public static void setup() throws Exception
  {
    logger.debug("Logger repository before LoggerUtil.changeLoggersLevel() {}", LogManager.getLoggerRepository());
    LoggerUtil.changeLoggersLevel(Maps.<String, String>newHashMap());
    logger.debug("Logger repository after LoggerUtil.changeLoggersLevel() {}", LogManager.getLoggerRepository());
    log4jLogger.setLevel(Level.TRACE);
    Category category = log4jLogger;
    while (category != null) {
      Enumeration appenders = category.getAllAppenders();
      while (appenders.hasMoreElements()) {
        Object o = appenders.nextElement();
        if (o instanceof AppenderSkeleton) {
          AppenderSkeleton appender = (AppenderSkeleton)o;
          if (!appenderLevelMap.containsKey(appender)) {
            appenderLevelMap.put(appender, appender.getThreshold());
            appender.setThreshold(Level.INFO);
          }
        }
      }
      if (category.getAdditivity()) {
        category = category.getParent();
      } else {
        category = null;
      }
    }
  }

  @AfterClass
  public static void teardown()
  {
    for (Map.Entry<AppenderSkeleton, Priority> e : appenderLevelMap.entrySet()) {
      e.getKey().setThreshold(e.getValue());
    }
  }


  @Test
  public void testGetPatternLevels()
  {
    Map<String, String> changes = Maps.newHashMap();
    changes.put("_.org.apache.*", "WARN");
    changes.put("_.com.datatorrent.io.fs.*", "DEBUG");
    changes.put("_.com.datatorrent.io.*", "ERROR");
    LoggerUtil.changeLoggersLevel(changes);

    Map<String, String> levels = LoggerUtil.getPatternLevels();

    assertEquals(3, levels.size());
    assertEquals(levels.get("_.org.apache.*"), "WARN");
    assertEquals(levels.get("_.com.datatorrent.io.fs.*"), "DEBUG");
    assertEquals(levels.get("_.com.datatorrent.io.*"), "ERROR");

    changes.clear();
    changes.put("_.com.datatorrent.*", "WARN");
    LoggerUtil.changeLoggersLevel(changes);

    levels = LoggerUtil.getPatternLevels();
    assertEquals(2, levels.size());
    assertEquals(levels.get("_.org.apache.*"), "WARN");
    assertNull(levels.get("_.com.datatorrent.io.fs.*"));
    assertNull(levels.get("_.com.datatorrent.io.*"));
    assertEquals(levels.get("_.com.datatorrent.*"), "WARN");
  }

  @Test
  public void testLoggerLevels()
  {
    org.slf4j.Logger sl4jLogger;
    org.apache.log4j.Logger log4jLogger;

    Map<String, String> changes = Maps.newHashMap();
    changes.put("_.org.apache.*", "WARN");
    changes.put("_.com.datatorrent.*", "WARN");
    changes.put("_.com.datatorrent.stram.client.*", "INFO");
    changes.put("_.com.datatorrent.stram.api.*", "DEBUG");
    LoggerUtil.changeLoggersLevel(changes);

    sl4jLogger = LoggerFactory.getLogger("_.com.datatorrent.stram.client.DTConfiguration");
    assertTrue(sl4jLogger.isInfoEnabled());
    assertFalse(sl4jLogger.isDebugEnabled());

    sl4jLogger = LoggerFactory.getLogger("_.com.datatorrent.stram.api.StramEvent");
    assertTrue(sl4jLogger.isInfoEnabled());
    assertTrue(sl4jLogger.isDebugEnabled());

    sl4jLogger = LoggerFactory.getLogger("_.com.datatorrent.stram.StreamingAppMaster");
    assertFalse(sl4jLogger.isInfoEnabled());
    assertFalse(sl4jLogger.isDebugEnabled());

    log4jLogger = LogManager.getLogger("_.com.datatorrent.stram.client.DTConfiguration");
    assertSame(log4jLogger.getLevel(), Level.INFO);
    assertSame(log4jLogger.getEffectiveLevel(), Level.INFO);

    org.apache.log4j.Logger stramEventLogger = LogManager.getLogger("_.com.datatorrent.stram.api.StramEvent");
    assertSame(stramEventLogger.getLevel(), Level.DEBUG);
    assertSame(stramEventLogger.getEffectiveLevel(), Level.DEBUG);

    org.apache.log4j.Logger streamingAppMasterLogger = LogManager.getLogger("_.com.datatorrent.stram.StreamingAppMaster");
    assertSame(streamingAppMasterLogger.getLevel(), Level.WARN);
    assertSame(streamingAppMasterLogger.getEffectiveLevel(), Level.WARN);

    changes.clear();
    changes.put("_.com.datatorrent.*", "DEBUG");
    changes.put("_.com.datatorrent.stram.engine.*", "ERROR");
    LoggerUtil.changeLoggersLevel(changes);

    sl4jLogger = LoggerFactory.getLogger("_.com.datatorrent.stram.client.DTConfiguration");
    assertTrue(sl4jLogger.isInfoEnabled());
    assertTrue(sl4jLogger.isDebugEnabled());

    sl4jLogger = LoggerFactory.getLogger("_.com.datatorrent.stram.api.StramEvent");
    assertTrue(sl4jLogger.isInfoEnabled());
    assertTrue(sl4jLogger.isDebugEnabled());

    sl4jLogger = LoggerFactory.getLogger("_.com.datatorrent.stram.StreamingAppMaster");
    assertTrue(sl4jLogger.isInfoEnabled());
    assertTrue(sl4jLogger.isDebugEnabled());

    sl4jLogger = LoggerFactory.getLogger("_.com.datatorrent.stram.engine.StreamingContainer");
    assertFalse(sl4jLogger.isInfoEnabled());
    assertFalse(sl4jLogger.isDebugEnabled());

    log4jLogger = LogManager.getLogger("_.com.datatorrent.stram.client.DTConfiguration");
    assertSame(log4jLogger.getLevel(), Level.DEBUG);
    assertSame(log4jLogger.getEffectiveLevel(), Level.DEBUG);

    log4jLogger = LogManager.getLogger("_.com.datatorrent.stram.api.StramEvent");
    assertSame(log4jLogger.getLevel(), Level.DEBUG);
    assertSame(log4jLogger.getEffectiveLevel(), Level.DEBUG);

    log4jLogger = LogManager.getLogger("_.com.datatorrent.stram.StreamingAppMaster");
    assertSame(log4jLogger.getLevel(), Level.DEBUG);
    assertSame(log4jLogger.getEffectiveLevel(), Level.DEBUG);

    log4jLogger = LogManager.getLogger("_.com.datatorrent.stram.engine.StreamingContainer");
    assertSame(log4jLogger.getLevel(), Level.ERROR);
    assertSame(log4jLogger.getEffectiveLevel(), Level.ERROR);
  }

  @Test
  public void testParentLevel()
  {
    org.apache.log4j.Logger log4jLogger = LogManager.getLogger("com.datatorrent.stram.util.Unknown");
    assertNull(log4jLogger.getLevel());
    Category parent = log4jLogger.getParent();
    while (parent.getLevel() == null) {
      parent = parent.getParent();
    }
    assertSame(log4jLogger.getEffectiveLevel(), parent.getLevel());
  }

  @Test
  public void testAppender()
  {
    String appenderName = "testAppender";
    String appenderName1 = "testAppender1";
    String args = "log4j.appender.testAppender=com.datatorrent.stram.util.LoggerUtilTest$TestAppender"
        + ",log4j.appender.testAppender.layout=org.apache.log4j.PatternLayout"
        + ",log4j.appender.testAppender.layout.ConversionPattern=%d %d{Z} [%t] %-5p (%F:%L) - %m%n"
        + ",log4j.appender.testAppender1=org.apache.log4j.ConsoleAppender"
        + ",log4j.appender.testAppender1.layout=org.apache.log4j.PatternLayout"
        + ",log4j.appender.testAppender1.layout.ConversionPattern=%d %d{Z} [%t] %-5p (%F:%L) - %m%n";

    assertTrue(LoggerUtil.addAppenders(log4jLogger, new String[] {appenderName}, args, ","));
    TestAppender appender = (TestAppender)log4jLogger.getAppender(appenderName);

    LoggerUtilTest.logger.debug(args);
    assertEquals(args, appender.lastMessage);
    assertEquals(appender.level, Level.DEBUG);

    LoggerUtilTest.logger.trace(appenderName1);
    assertEquals(appenderName1, appender.lastMessage);
    assertEquals(appender.level, Level.TRACE);

    // don't allow to add an appender with the same name
    assertFalse(LoggerUtil.addAppenders(log4jLogger, new String[] {appenderName}, args, ","));
    LoggerUtilTest.logger.debug("Test Appender is added: {}", LoggerUtil.getAppendersNames(log4jLogger));
    testAndRemoveAppender(log4jLogger, appenderName);
    LoggerUtilTest.logger.debug("Test Appender is removed: {}", LoggerUtil.getAppendersNames(log4jLogger));

    Logger rootLogger = LogManager.getRootLogger();
    assertTrue(LoggerUtil.addAppenders(rootLogger, new String[] {appenderName}, args, ","));
    LoggerUtilTest.logger.debug("Test Appender is added: {}", LoggerUtil.getAppendersNames());
    testAndRemoveAppender(rootLogger, appenderName);
    LoggerUtilTest.logger.debug("Test Appender is removed: {}", LoggerUtil.getAppendersNames());

    System.setProperty(Context.DAGContext.LOGGER_APPENDER.getLongName(), appenderName + "," + appenderName1 + ";" + args);
    assertTrue(LoggerUtil.addAppenders());
    LoggerUtilTest.logger.debug("Test Appenders are added: {}", LoggerUtil.getAppendersNames());

    testAndRemoveAppender(rootLogger, appenderName);
    testAndRemoveAppender(rootLogger, appenderName1);
    LoggerUtilTest.logger.debug("Test Appenders are removed: {}", LoggerUtil.getAppendersNames());
  }

  private static void testAndRemoveAppender(Logger logger, String name)
  {
    Appender appender = logger.getAppender(name);
    assertNotNull(appender);
    assertTrue(LoggerUtil.getAppendersNames(logger).contains(name));
    LoggerUtil.removeAppender(logger, name);
    assertNull(logger.getAppender(name));
  }

  @Test
  public void testSetupMDC()
  {
    // The test does not test MDC properties that are passed via environment variables

    String appenderName = "mdcTestAppender";
    String service = "test";
    String application = "my application";
    String args = "log4j.appender.mdcTestAppender=com.datatorrent.stram.util.LoggerUtilTest$TestAppender"
        + ",log4j.appender.mdcTestAppender.layout=org.apache.log4j.PatternLayout"
        + ",log4j.appender.mdcTestAppender.layout.ConversionPattern=%d %d{Z} [%t] %-5p (%F:%L) - %m%n";

    System.setProperty(APPLICATION_NAME.getLongName(), application);
    LoggerUtil.setupMDC(service);

    LoggerUtil.addAppenders(log4jLogger, new String[] {appenderName}, args, ",");
    TestAppender appender = (TestAppender)log4jLogger.getAppender(appenderName);

    LoggerUtilTest.logger.debug(args);
    assertEquals(service, appender.mdcProperties.get("apex.service"));
    String node = StramClientUtils.getHostName();
    assertEquals(node == null ? "unknown" : node, appender.mdcProperties.get("apex.node"));
    assertEquals(application, appender.mdcProperties.get("apex.application"));

    assertTrue(LoggerUtil.removeAppender(log4jLogger, appenderName));
  }

  public static class TestAppender extends ConsoleAppender
  {
    private String lastMessage = null;
    private Level level;
    private Map mdcProperties;

    @Override
    public void append(LoggingEvent event)
    {
      mdcProperties = event.getProperties();
      lastMessage = event.getRenderedMessage();
      level = event.getLevel();
    }
  }
}
