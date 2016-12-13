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
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.apex.log.LogFileInformation;

import org.apache.log4j.Appender;
import org.apache.log4j.Category;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.DefaultRepositorySelector;
import org.apache.log4j.spi.HierarchyEventListener;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.log4j.spi.LoggerRepository;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * @since 3.5.0
 */
public class LoggerUtil
{

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LoggerUtil.class);

  private static final Map<String, Level> patternLevel = Maps.newHashMap();
  private static final Map<String, Pattern> patterns = Maps.newHashMap();
  private static final Function<Level, String> levelToString = new Function<Level, String>()
  {
    @Override
    public String apply(@Nullable Level input)
    {
      return input == null ? "" : input.toString();
    }
  };

  private static class DelegatingLoggerRepository implements LoggerRepository
  {
    private static class DefaultLoggerFactory implements LoggerFactory
    {
      private static class DefaultLogger extends Logger
      {
        public DefaultLogger(String name)
        {
          super(name);
        }
      }

      @Override
      public Logger makeNewLoggerInstance(String name)
      {
        Logger logger = new DefaultLogger(name);
        Level level = getLevelFor(name);
        if (level != null) {
          logger.setLevel(level);
        }
        return logger;
      }
    }

    private final LoggerFactory loggerFactory = new DefaultLoggerFactory();
    private final LoggerRepository loggerRepository;

    private DelegatingLoggerRepository(LoggerRepository loggerRepository)
    {
      this.loggerRepository = loggerRepository;
    }

    @Override
    public void addHierarchyEventListener(HierarchyEventListener listener)
    {
      loggerRepository.addHierarchyEventListener(listener);
    }

    @Override
    public boolean isDisabled(int level)
    {
      return loggerRepository.isDisabled(level);
    }

    @Override
    public void setThreshold(Level level)
    {
      loggerRepository.setThreshold(level);
    }

    @Override
    public void setThreshold(String val)
    {
      loggerRepository.setThreshold(val);
    }

    @Override
    public void emitNoAppenderWarning(Category cat)
    {
      loggerRepository.emitNoAppenderWarning(cat);
    }

    @Override
    public Level getThreshold()
    {
      return loggerRepository.getThreshold();
    }

    @Override
    public Logger getLogger(String name)
    {
      return loggerRepository.getLogger(name, loggerFactory);
    }

    @Override
    public Logger getLogger(String name, LoggerFactory factory)
    {
      return loggerRepository.getLogger(name, factory);
    }

    @Override
    public Logger getRootLogger()
    {
      return loggerRepository.getRootLogger();
    }

    @Override
    public Logger exists(String name)
    {
      return loggerRepository.exists(name);
    }

    @Override
    public void shutdown()
    {
      loggerRepository.shutdown();
    }

    @Override
    public Enumeration<Logger> getCurrentLoggers()
    {
      return loggerRepository.getCurrentLoggers();
    }

    @Override
    public Enumeration<Logger> getCurrentCategories()
    {
      return loggerRepository.getCurrentCategories();
    }

    @Override
    public void fireAddAppenderEvent(Category logger, Appender appender)
    {
      loggerRepository.fireAddAppenderEvent(logger, appender);
    }

    @Override
    public void resetConfiguration()
    {
      loggerRepository.resetConfiguration();
    }
  }

  private static FileAppender fileAppender;
  private static boolean shouldFetchLogFileInfo;
  static {
    logger.debug("initializing LoggerUtil");
    initializeLogger();
  }

  @VisibleForTesting
  static void initializeLogger()
  {
    LogManager.setRepositorySelector(new DefaultRepositorySelector(new DelegatingLoggerRepository(LogManager.getLoggerRepository())), null);
    fileAppender = getFileAppender();
    shouldFetchLogFileInfo = shouldFetchLogFileInformation();
  }

  private static synchronized Level getLevelFor(String name)
  {
    if (patternLevel.isEmpty()) {
      return null;
    }
    String longestPatternKey = null;
    for (String patternKey : patternLevel.keySet()) {
      Pattern pattern = patterns.get(patternKey);
      if (pattern.matcher(name).matches() && (longestPatternKey == null || longestPatternKey.length() < patternKey.length())) {
        longestPatternKey = patternKey;
      }
    }
    if (longestPatternKey != null) {
      return patternLevel.get(longestPatternKey);
    }
    return null;
  }

  public static ImmutableMap<String, String> getPatternLevels()
  {
    return ImmutableMap.copyOf(Maps.transformValues(patternLevel, levelToString));
  }

  public static synchronized void changeLoggersLevel(@Nonnull Map<String, String> targetChanges)
  {
    /* remove existing patterns which are subsets of new patterns. for eg. if x.y.z.* will be removed if
     *  there is x.y.* in the target changes.
     */
    for (Map.Entry<String, String> changeEntry : targetChanges.entrySet()) {
      Iterator<Map.Entry<String, Pattern>> patternsIterator = patterns.entrySet().iterator();
      while ((patternsIterator.hasNext())) {
        Map.Entry<String, Pattern> entry = patternsIterator.next();
        String finer = entry.getKey();
        String wider = changeEntry.getKey();
        if (finer.length() < wider.length()) {
          continue;
        }
        boolean remove = false;
        for (int i = 0; i < wider.length(); i++) {
          if (wider.charAt(i) == '*') {
            remove = true;
            break;
          }
          if (wider.charAt(i) != finer.charAt(i)) {
            break;
          } else if (i == wider.length() - 1) {
            remove = true;
          }
        }
        if (remove) {
          patternsIterator.remove();
          patternLevel.remove(finer);
        }
      }
    }
    for (Map.Entry<String, String> loggerEntry : targetChanges.entrySet()) {
      String target = loggerEntry.getKey();
      patternLevel.put(target, Level.toLevel(loggerEntry.getValue()));
      patterns.put(target, Pattern.compile(target));
    }

    if (!patternLevel.isEmpty()) {
      @SuppressWarnings("unchecked")
      Enumeration<Logger> loggerEnumeration = LogManager.getCurrentLoggers();
      while (loggerEnumeration.hasMoreElements()) {
        Logger classLogger = loggerEnumeration.nextElement();
        Level oldLevel = classLogger.getLevel();
        Level newLevel = getLevelFor(classLogger.getName());
        if (newLevel != null && (oldLevel == null || !newLevel.equals(oldLevel))) {
          logger.info("changing level of {} to {}", classLogger.getName(), newLevel);
          classLogger.setLevel(newLevel);
        }
      }
    }
  }

  public static synchronized ImmutableMap<String, String> getClassesMatching(@Nonnull String searchKey)
  {
    Pattern searchPattern = Pattern.compile(searchKey);
    Map<String, String> matchedClasses = Maps.newHashMap();
    @SuppressWarnings("unchecked")
    Enumeration<Logger> loggerEnumeration = LogManager.getCurrentLoggers();
    while (loggerEnumeration.hasMoreElements()) {
      Logger logger = loggerEnumeration.nextElement();
      if (searchPattern.matcher(logger.getName()).matches()) {
        Level level = logger.getLevel();
        matchedClasses.put(logger.getName(), level == null ? "" : level.toString());
      }
    }
    return ImmutableMap.copyOf(matchedClasses);
  }

  /**
   * Returns logger log file {@link LogFileInformation}
   * @return logFileInformation
   */
  public static LogFileInformation getLogFileInformation()
  {
    if (shouldFetchLogFileInfo) {
      File logFile = new File(fileAppender.getFile());
      LogFileInformation logFileInfo = new LogFileInformation(fileAppender.getFile(), logFile.length());
      return logFileInfo;
    }
    return null;
  }

  private static FileAppender getFileAppender()
  {
    Enumeration<Appender> e = LogManager.getRootLogger().getAllAppenders();
    FileAppender fileAppender = null;
    while (e.hasMoreElements()) {
      Appender appender = e.nextElement();
      if (appender instanceof FileAppender) {
        if (fileAppender == null) {
          fileAppender = (FileAppender)appender;
        } else {
          //skip fetching log file information if we have multiple file Appenders
          return null;
        }
      }
    }
    return fileAppender;
  }

  /*
   * We should return log file information only if,
   * we have single file Appender, the logging level of appender is set to level Error or above and immediateFlush is set to true.
   * In future we should be able to enhance this feature to support multiple file appenders.
   */
  private static boolean shouldFetchLogFileInformation()
  {
    if (fileAppender != null && isErrorLevelEnable() && fileAppender.getImmediateFlush()) {
      return true;
    }
    logger.warn(
        "Log information is unavailable. To enable log information log4j/logging should be configured with single FileAppender that has immediateFlush set to true and log level set to ERROR or greater.");
    return false;
  }

  private static boolean isErrorLevelEnable()
  {
    if (fileAppender != null) {
      Level p = (Level)fileAppender.getThreshold();
      if (p == null) {
        p = LogManager.getRootLogger().getLevel();
      }
      if (p != null) {
        return Level.ERROR.isGreaterOrEqual(p);
      }
    }
    return false;
  }

}
