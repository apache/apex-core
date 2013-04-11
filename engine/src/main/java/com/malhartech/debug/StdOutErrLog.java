/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.debug;

import java.io.PrintStream;
import org.apache.log4j.Logger;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class StdOutErrLog
{
  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public static void tieSystemOutAndErrToLog()
  {
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

  private static final Logger logger = Logger.getLogger(StdOutErrLog.class);
}