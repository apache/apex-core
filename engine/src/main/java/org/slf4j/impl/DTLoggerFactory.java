/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package org.slf4j.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

import com.google.common.collect.Maps;

/**
 * An implementation of {@link ILoggerFactory}
 * @author chandni
 */
public class DTLoggerFactory implements ILoggerFactory
{
  private static DTLoggerFactory SINGLETON;

  public synchronized static DTLoggerFactory get()
  {
    if (SINGLETON == null) {
      SINGLETON = new DTLoggerFactory();
    }
    return SINGLETON;
  }

  ConcurrentMap<String, DTLoggerAdapter> loggerMap;
  Map<String, Level> patternLevel;
  Map<String, Pattern> patterns;

  private DTLoggerFactory()
  {
    loggerMap = Maps.newConcurrentMap();
    patternLevel = Maps.newHashMap();
    patterns = Maps.newHashMap();
  }

  public void changeLoggersLevel(@Nonnull Map<String, String> targetChanges)
  {
    for (Map.Entry<String, String> loggerEntry : targetChanges.entrySet()) {
      String target = loggerEntry.getKey();
      patternLevel.put(target, Level.toLevel(loggerEntry.getValue()));
      patterns.put(target, Pattern.compile(target));
    }

    if (!patternLevel.isEmpty()) {
      for (DTLoggerAdapter classLogger : loggerMap.values()) {
        Level level = getLevelFor(classLogger.getName());
        if(level!=null){
          classLogger.setLogLevel(level);
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

      DTLoggerAdapter newInstance = new DTLoggerAdapter(log4jLogger);
      Level level = getLevelFor(name);
      if (level != null) {
        newInstance.setLogLevel(level);
      }
      Logger oldInstance = loggerMap.putIfAbsent(name, newInstance);
      return oldInstance == null ? newInstance : oldInstance;
    }
  }

  private Level getLevelFor(String name)
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
}
