/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.stram.util;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class LoggersUtil
{
  public static Map<String, Logger> getCurrentLoggers()
  {
    Map<String, Logger> classLoggers = Maps.newHashMap();

    Enumeration<Logger> loggerEnumeration = LogManager.getCurrentLoggers();
    while (loggerEnumeration.hasMoreElements()) {
      org.apache.log4j.Logger logger = loggerEnumeration.nextElement();
      classLoggers.put(logger.getName(), logger);
    }
    return classLoggers;
  }

  public static void changeCurrentLoggers(@Nonnull Map<String, String> targetChanges)
  {
    Map<String, Logger> currentLoggers = getCurrentLoggers();

    for (Map.Entry<String, String> entry : targetChanges.entrySet()) {
      String target = entry.getKey();
      String level = entry.getValue();
      Pattern pattern = Pattern.compile(target);

      for (Iterator<Map.Entry<String, Logger>> currentLoggersIter = currentLoggers.entrySet().iterator();
           currentLoggersIter.hasNext(); ) {
        Map.Entry<String, Logger> currentLogger = currentLoggersIter.next();

        if (pattern.matcher(currentLogger.getKey()).matches()) {
          Logger lLogger = currentLogger.getValue();
          if (lLogger.getLevel() == null || !lLogger.getLevel().toString().equalsIgnoreCase(level)) {
            LOG.debug("Setting logger level for {} to {}", currentLogger.getKey(), level);
            lLogger.setLevel(Level.toLevel(level));
          }
          currentLoggersIter.remove();
        }
      }
    }
  }

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LoggersUtil.class);
}
