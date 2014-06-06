/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.util;

import java.util.regex.Pattern;

import org.apache.log4j.Level;

public class ConfigValidator
{
  private static Pattern LOGGERS_PATTERN = Pattern.compile("^(\\w+\\.?)+(\\*|\\w+)$");
  /**
   * Validates the logger pattern and the level.
   * @param pattern
   * @param level
   * @return
   */
  public static boolean validateLoggersLevel(String pattern, String level)
  {
    if (!LOGGERS_PATTERN.matcher(pattern).matches()) {
      return false;
    }
    if (Level.toLevel(level, null) == null) {
      return false;
    }
    return true;
  }
}
