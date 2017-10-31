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
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LogFileInformationTest
{
  private static final String APPENDER_NAME = "rfa";
  private static final Logger logger = LogManager.getLogger(LogFileInformationTest.class);
  private static String logFileName;

  @BeforeClass
  public static void beforeClass() throws IOException
  {
    String logFileDir = new File("target" + File.separator + "logDir").getAbsolutePath();
    logFileName = logFileDir + File.separator + "appTest.log";
    RollingFileAppender rfa = new RollingFileAppender(new PatternLayout("%d{ISO8601} [%t] %-5p %c{2} %M - %m%n"),
        logFileName);
    rfa.setName(APPENDER_NAME);
    logger.addAppender(rfa);
  }

  @Test
  public void testGetLogFileInformation()
  {
    long currentLogFileSize = LoggerUtil.getLogFileInformation(logger).fileOffset;
    logger.info("Adding Test log message.");
    assertEquals(logFileName, LoggerUtil.getLogFileInformation(logger).fileName);
    assertTrue(LoggerUtil.getLogFileInformation(logger).fileOffset > currentLogFileSize);
  }

  @Test
  public void testImmediateFlushOff()
  {
    RollingFileAppender rfa = (RollingFileAppender)logger.getAppender(APPENDER_NAME);
    assertTrue(rfa.getImmediateFlush());
    rfa.setImmediateFlush(false);
    assertNull(LoggerUtil.getLogFileInformation());
    rfa.setImmediateFlush(true);
  }

  @Test
  public void testErrorLevelOff()
  {
    RollingFileAppender rfa = (RollingFileAppender)logger.getAppender(APPENDER_NAME);
    assertNull(rfa.getThreshold());
    rfa.setThreshold(Level.FATAL);
    assertNull(LoggerUtil.getLogFileInformation(logger));
    rfa.setThreshold(null);
  }

  @Test
  public void testNoFileAppender()
  {
    RollingFileAppender rfa = (RollingFileAppender)logger.getAppender(APPENDER_NAME);
    logger.removeAppender(APPENDER_NAME);
    assertNull(LoggerUtil.getLogFileInformation());
    logger.addAppender(rfa);
  }

  @Test
  public void testSlf4Logger()
  {
    assertNotNull(LoggerUtil.getLogFileInformation(LoggerFactory.getLogger(LogFileInformationTest.class)));
  }

  @AfterClass
  public static void tearDown()
  {
    LogManager.getLogger(LogFileInformationTest.class).removeAppender(APPENDER_NAME);
  }
}
