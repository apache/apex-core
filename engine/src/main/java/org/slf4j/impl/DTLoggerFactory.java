/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package org.slf4j.impl;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * An implementation of {@link ILoggerFactory}
 *
 * @author chandni
 */
public class DTLoggerFactory implements ILoggerFactory
{
  public static final String DT_LOGGERS_LEVEL = "dt.loggers.level";

  private static DTLoggerFactory SINGLETON;

  public synchronized static DTLoggerFactory getInstance()
  {
    if (SINGLETON == null) {
      SINGLETON = new DTLoggerFactory();
    }
    return SINGLETON;
  }

  private final ConcurrentMap<String, Logger> loggerMap;
  private final Map<String, Level> patternLevel;
  private final Map<String, Pattern> patterns;
  private boolean initialized = false;

  private DTLoggerFactory()
  {
    loggerMap = Maps.newConcurrentMap();
    patternLevel = Maps.newHashMap();
    patterns = Maps.newHashMap();
  }

  public synchronized void initialize()
  {
    if (!initialized) {
      LOG.info("initializing DT Logger Factory");
      String loggersLevel = System.getProperty(DT_LOGGERS_LEVEL);
      if (!Strings.isNullOrEmpty(loggersLevel)) {
        Map<String, String> targetChanges = Maps.newHashMap();
        String targets[] = loggersLevel.split(",");
        for (String target : targets) {
          String parts[] = target.split(":");
          targetChanges.put(parts[0], parts[1]);
        }
        changeLoggersLevel(targetChanges);
      }
      initialized = true;
    }
    else {
      LOG.warn("DT Logger Factory already initialized.");
    }
  }

  public synchronized void changeLoggersLevel(@Nonnull Map<String, String> targetChanges)
  {
    /*remove existing patterns which are subsets of new patterns. for eg. if x.y.z.* will be removed if
    there is x.y.* in the target changes.
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
          }
          else if (i == wider.length() - 1) {
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
      Enumeration<org.apache.log4j.Logger> loggerEnumeration = LogManager.getCurrentLoggers();
      while (loggerEnumeration.hasMoreElements()) {
        org.apache.log4j.Logger classLogger = loggerEnumeration.nextElement();
        Level newLevel = getLevelFor(classLogger.getName());
        if (!Objects.equal(classLogger.getLevel(), newLevel)) {
          LOG.info("changing level of " + classLogger.getName() + " to " + newLevel);
          classLogger.setLevel(newLevel);
        }
      }
    }
  }

  @Override
  public Logger getLogger(String name)
  {
    Logger slf4jLogger = loggerMap.get(name);
    if (slf4jLogger != null) {
      return slf4jLogger;
    }
    else {
      org.apache.log4j.Logger log4jLogger;
      if (name.equalsIgnoreCase(Logger.ROOT_LOGGER_NAME)) {
        log4jLogger = LogManager.getRootLogger();
      }
      else {
        log4jLogger = LogManager.getLogger(name);
      }

      Log4jLoggerAdapter newInstance = new Log4jLoggerAdapter(log4jLogger);
      Level level = getLevelFor(name);
      if (level != null) {
        log4jLogger.setLevel(level);
      }
      Logger oldInstance = loggerMap.putIfAbsent(name, newInstance);
      return oldInstance == null ? newInstance : oldInstance;
    }
  }

  private synchronized Level getLevelFor(String name)
  {
    if (patternLevel.isEmpty()) {
      return null;
    }
    String longestPatternKey = null;
    for (String partternKey : patternLevel.keySet()) {
      Pattern pattern = patterns.get(partternKey);
      if (pattern.matcher(name).matches() && (longestPatternKey == null || longestPatternKey.length() < partternKey.length())) {
        longestPatternKey = partternKey;
      }
    }
    if (longestPatternKey != null) {
      return patternLevel.get(longestPatternKey);
    }
    return null;
  }

  public synchronized ImmutableMap<String, String> getClassesMatching(@Nonnull String searchKey)
  {
    Pattern searchPattern = Pattern.compile(searchKey);
    Map<String, String> matchedClasses = Maps.newHashMap();
    Enumeration<org.apache.log4j.Logger> loggerEnumeration = LogManager.getCurrentLoggers();
    while (loggerEnumeration.hasMoreElements()) {
      org.apache.log4j.Logger logger = loggerEnumeration.nextElement();
      if (searchPattern.matcher(logger.getName()).matches()) {
        Level level = logger.getLevel();
        matchedClasses.put(logger.getName(), level == null ? "" : level.toString());
      }
    }
    return ImmutableMap.copyOf(matchedClasses);
  }

  private static final org.apache.log4j.Logger LOG = LogManager.getLogger(DTLoggerFactory.class);
}
