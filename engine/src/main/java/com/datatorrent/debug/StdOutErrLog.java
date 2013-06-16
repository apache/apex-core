/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.debug;

import java.io.PrintStream;
import org.apache.log4j.Appender;
import org.apache.log4j.RollingFileAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class StdOutErrLog
{
  public static final String MALHAR_LOG_APPENDER = "MALHAR";
  public static final String MALHAR_LOGDIR = "malhar.logdir";

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public static void tieSystemOutAndErrToLog()
  {

    org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
    Appender appender = rootLogger.getAppender(MALHAR_LOG_APPENDER);
    if (appender instanceof RollingFileAppender) {
      RollingFileAppender rfa = (RollingFileAppender)appender;
      if (rfa.getFile() == null || rfa.getFile().isEmpty()) {
        rfa.setFile(System.getProperty(MALHAR_LOGDIR));
        rfa.activateOptions();
      }
    }
    else {
      logger.warn("found appender {} instead of RollingFileAppender", appender);
    }

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
        logger.info(string);
      }

    };
  }

  private static final Logger logger = LoggerFactory.getLogger(StdOutErrLog.class);
}