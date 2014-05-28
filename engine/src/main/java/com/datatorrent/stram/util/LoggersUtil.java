/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.util;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class LoggersUtil
{
  public static Set<Logger> getCurrentLoggers()
  {
    Set<Logger> classLoggers = Sets.newHashSet();

    Enumeration<Logger> loggerEnumeration = LogManager.getCurrentLoggers();
    while (loggerEnumeration.hasMoreElements()) {
      org.apache.log4j.Logger logger = loggerEnumeration.nextElement();
      classLoggers.add(logger);
    }
    return classLoggers;
  }

  public static void changeCurrentLoggers(@Nonnull Map<String, String> targetChanges)
  {
    Set<Logger> currentLoggers = getCurrentLoggers();

    for (Map.Entry<String, String> entry : targetChanges.entrySet()) {
      String target = entry.getKey();
      String level = entry.getValue();
      Pattern pattern = Pattern.compile(target);

      for (Iterator<Logger> currentLoggersIter = currentLoggers.iterator(); currentLoggersIter.hasNext(); ) {
        Logger lLogger = currentLoggersIter.next();

        if (pattern.matcher(lLogger.getName()).matches()) {
          if (lLogger.getLevel() == null || !lLogger.getLevel().toString().equalsIgnoreCase(level)) {
            LOG.debug("Setting logger level for {} to {}", lLogger.getName(), level);
            lLogger.setLevel(Level.toLevel(level));
          }
          currentLoggersIter.remove();
        }
      }
    }
  }

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LoggersUtil.class);
}
