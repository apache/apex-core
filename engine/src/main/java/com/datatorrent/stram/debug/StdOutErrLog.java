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
package com.datatorrent.stram.debug;

import java.io.PrintStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.log4j.Appender;
import org.apache.log4j.RollingFileAppender;

import com.datatorrent.stram.util.LoggerUtil;

/**
 * <p>StdOutErrLog class.</p>
 *
 * @since 0.3.2
 */
public class StdOutErrLog
{
  public static final String DT_LOG_APPENDER = "DT";
  public static final String DT_LOGDIR = "dt.logdir";

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public static void tieSystemOutAndErrToLog()
  {

    org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
    Appender appender = rootLogger.getAppender(DT_LOG_APPENDER);
    if (appender instanceof RollingFileAppender) {
      RollingFileAppender rfa = (RollingFileAppender)appender;
      if (rfa.getFile() == null || rfa.getFile().isEmpty()) {
        rfa.setFile(System.getProperty(DT_LOGDIR));
        rfa.activateOptions();
      }
    } else if (appender != null) {
      logger.warn("found appender {} instead of RollingFileAppender", appender);
    }

    LoggerUtil.addAppenders();
    System.setOut(createLoggingProxy(System.out));
    System.setErr(createLoggingProxy(System.err));
  }

  public static PrintStream createLoggingProxy(final PrintStream realPrintStream)
  {
    return new PrintStream(realPrintStream)
    {
      @Override
      public void print(final String string)
      {
        realPrintStream.print(string);
        //logger.info(string);
      }

    };
  }

  private static final Logger logger = LoggerFactory.getLogger(StdOutErrLog.class);
}
