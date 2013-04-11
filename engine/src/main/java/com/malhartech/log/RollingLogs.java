/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.log;

import java.io.IOException;
import java.io.PrintStream;
import java.util.logging.*;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class RollingLogs
{
  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public static final PrintStream stdout = System.out;
  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public static final PrintStream stderr = System.err;

  public static void initialize(String filename, int limit, int count, boolean append) throws IOException
  {
    // initialize logging to go to rolling log file
    LogManager logManager = LogManager.getLogManager();
    logManager.reset();

    // log file max size 10K, 3 rolling files, append-on-open
    Handler fileHandler = new FileHandler(filename, limit, count, append);
    fileHandler.setFormatter(new SimpleFormatter());
    Logger.getLogger("").addHandler(fileHandler);

    // now rebind stdout/stderr to logger
    Logger logger;
    LoggingOutputStream los;

    logger = Logger.getLogger("stdout");
    los = new LoggingOutputStream(logger, StdOutErrLevel.STDOUT);
    System.setOut(new PrintStream(los, true));

    logger = Logger.getLogger("stderr");
    los = new LoggingOutputStream(logger, StdOutErrLevel.STDERR);
    System.setErr(new PrintStream(los, true));
  }

}
