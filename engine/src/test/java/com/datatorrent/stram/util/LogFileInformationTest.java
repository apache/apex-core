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

import java.io.File;
import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogFileInformationTest
{
  private static final String APPENDER_NAME = "rfa";
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LogFileInformationTest.class);
  private static String logFileName;

  @BeforeClass
  public static void beforeClass() throws IOException
  {
    String logFileDir = new File("target" + File.separator + "logDir").getAbsolutePath();
    logFileName = logFileDir + File.separator + "appTest.log";
    RollingFileAppender rfa = new RollingFileAppender(new PatternLayout("%d{ISO8601} [%t] %-5p %c{2} %M - %m%n"),
        logFileName);
    rfa.setName(APPENDER_NAME);
    Logger.getRootLogger().addAppender(rfa);
  }

  @Before
  public void setup()
  {
    LoggerUtil.initializeLogger();
  }

  @Test
  public void testGetLogFileInformation()
  {
    long currentLogFileSize = LoggerUtil.getLogFileInformation().fileOffset;
    logger.info("Adding Test log message.");
    assertEquals(logFileName, LoggerUtil.getLogFileInformation().fileName);
    assertTrue(LoggerUtil.getLogFileInformation().fileOffset > currentLogFileSize);
  }

  @Test
  public void testImmediateFlushOff()
  {
    RollingFileAppender rfa = (RollingFileAppender)Logger.getRootLogger().getAppender(APPENDER_NAME);
    rfa.setImmediateFlush(false);
    Logger.getRootLogger().addAppender(rfa);
    LoggerUtil.initializeLogger();

    Assert.assertNull(LoggerUtil.getLogFileInformation());
    rfa.setImmediateFlush(true);
  }

  @Test
  public void testErrorLevelOff()
  {
    Level curLogLevel = Logger.getRootLogger().getLevel();
    Logger.getRootLogger().setLevel(Level.FATAL);
    LoggerUtil.initializeLogger();

    Assert.assertNull(LoggerUtil.getLogFileInformation());
    Logger.getRootLogger().setLevel(curLogLevel);
  }

  @Test
  public void testNoFileAppender()
  {
    RollingFileAppender rfa = (RollingFileAppender)Logger.getRootLogger().getAppender(APPENDER_NAME);
    Logger.getRootLogger().removeAppender(APPENDER_NAME);
    LoggerUtil.initializeLogger();
    Assert.assertNull(LoggerUtil.getLogFileInformation());
    Logger.getRootLogger().addAppender(rfa);
  }

  @AfterClass
  public static void tearDown()
  {
    Logger.getRootLogger().removeAppender(APPENDER_NAME);
    FileUtils.deleteQuietly(new File(logFileName).getParentFile());
  }
}
