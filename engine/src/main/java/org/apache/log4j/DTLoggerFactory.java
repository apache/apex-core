/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package org.apache.log4j;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.apache.log4j.spi.LoggerFactory;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.log4j.spi.RepositorySelector;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * An implementation of {@link LoggerFactory}
 *
 * @author chandni
 */
public class DTLoggerFactory implements LoggerFactory
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
      new RepositorySelectorImpl().initialize();
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
      @SuppressWarnings("unchecked")
      Enumeration<Logger> loggerEnumeration = LogManager.getCurrentLoggers();
      while (loggerEnumeration.hasMoreElements()) {
        Logger classLogger = loggerEnumeration.nextElement();
        Level oldLevel = classLogger.getLevel();
        Level newLevel = getLevelFor(classLogger.getName());
        if (newLevel != null && (oldLevel == null || !newLevel.equals(oldLevel))) {
          LOG.info("changing level of " + classLogger.getName() + " to " + newLevel);
          classLogger.setLevel(newLevel);
        }
      }
    }
  }

  @Override
  public Logger makeNewLoggerInstance(String name)
  {
    Logger newInstance = new Logger(name);
    Level level = getLevelFor(name);
    if (level != null) {
      newInstance.setLevel(level);
    }
    loggerMap.put(name, newInstance);
    return newInstance;
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

  private static class RepositorySelectorImpl implements RepositorySelector
  {

    private boolean initialized;
    private Logger guard;
    private Hierarchy hierarchy;

    private RepositorySelectorImpl()
    {
      initialized = false;
    }
    private void initialize()
    {
      if (!initialized) {
        LOG.info("initializing logger repository selector impl");
        guard = LogManager.getRootLogger();
        LogManager.setRepositorySelector(this, guard);
        hierarchy = new LoggerRepositoryImpl(guard);
        initialized = true;
      }
    }

    @Override
    public LoggerRepository getLoggerRepository()
    {
      return hierarchy;
    }
  }

  private static class LoggerRepositoryImpl extends Hierarchy
  {
    /**
     * Create a new logger hierarchy.
     *
     * @param root The root of the new hierarchy.
     */
    private LoggerRepositoryImpl(Logger root)
    {
      super(root);
    }

    @Override
    public Logger getLogger(String name)
    {
      return super.getLogger(name, DTLoggerFactory.getInstance());
    }
  }

  private static final Logger LOG = LogManager.getLogger(DTLoggerFactory.class);
}
