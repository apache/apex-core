/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

import java.io.File;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * ConfigUtils class.</p>
 *
 * @author Pramod Immaneni <pramod@datatorrent.com>
 * @since 0.3.2
 */
public class ConfigUtils
{
  private static String yarnLogDir;
  private static final Logger LOG = LoggerFactory.getLogger(ConfigUtils.class);

  public static InetSocketAddress getRMAddress(YarnConfiguration conf)
  {
    return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
                              YarnConfiguration.DEFAULT_RM_ADDRESS,
                              YarnConfiguration.DEFAULT_RM_PORT);
  }

  public static String getRMUsername(Configuration conf)
  {
    String principal = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      principal = conf.get(YarnConfiguration.RM_PRINCIPAL);
      int sindex = -1;
      if ((principal != null) && ((sindex = principal.indexOf('/')) != -1)) {
        principal = principal.substring(0, sindex);
      }
    }
    return principal;
  }

  public static String getSchemePrefix(YarnConfiguration conf)
  {
    if (HttpConfig.Policy.HTTPS_ONLY == HttpConfig.Policy.fromString(conf.get(YarnConfiguration.YARN_HTTP_POLICY_KEY, YarnConfiguration.YARN_HTTP_POLICY_DEFAULT))) {
      return "https://";
    }
    else {
      return "http://";
    }
  }

  public static String getYarnLogDir()
  {
    if (yarnLogDir != null) {
      return yarnLogDir;
    }
    // this is a guess (hack?) on where yarn.log.dir is since its value is not passed to stram from node manager

    // first look at YARN_LOG_DIR environment variable. This is applicable for CDH
    String value = System.getenv("YARN_LOG_DIR");
    try {
      if (value != null) {
        return value;
      }
      String opts = System.getenv("YARN_OPTS");
      if (opts != null) {
        value = getDefineValue(opts, "yarn.log.dir");
        if (value != null) {
          return value;
        }
        value = getDefineValue(opts, "hadoop.log.dir");
        if (value != null) {
          return value;
        }
      }
      opts = System.getenv("HADOOP_OPTS");
      if (opts != null) {
        value = getDefineValue(opts, "yarn.log.dir");
        if (value != null) {
          return value;
        }
        value = getDefineValue(opts, "hadoop.log.dir");
        if (value != null) {
          return value;
        }
      }
      return value;
    }
    finally {
      yarnLogDir = value;
    }
  }

  public static String getRawContainerLogsUrl(YarnConfiguration conf, String nodeHttpAddress, String appId, String containerId)
  {
    String logDirs = conf.get(YarnConfiguration.NM_LOG_DIRS);
    if (logDirs.startsWith("${yarn.log.dir}")) {
      return ConfigUtils.getSchemePrefix(conf) + nodeHttpAddress + "/logs" + logDirs.substring("${yarn.log.dir}".length()) + "/" + appId + "/" + containerId;
    }
    else {
      try {
        String logDirsPath = new File(logDirs).getCanonicalPath();
        String yarnLogDirPath = new File(getYarnLogDir()).getCanonicalPath();
        if (logDirsPath.startsWith(yarnLogDirPath)) {
          return ConfigUtils.getSchemePrefix(conf) + nodeHttpAddress + "/logs" + logDirsPath.substring(yarnLogDirPath.length()) + "/" + appId + "/" + containerId;
        }
        else {
          LOG.warn("Cannot determine the location of container logs because of incompatible node manager log location ({}) and yarn log location ({})",
                   logDirsPath, yarnLogDirPath);
        }
      }
      catch (Exception ex) {
        LOG.warn("Cannot determine the location of container logs because of error: ", ex);
      }
    }
    return null;
  }

  private static String getDefineValue(String optString, String name)
  {
    int i = optString.indexOf("-D" + name + "=");
    if (i >= 0) {
      i = i + name.length() + 3;
      int j = optString.indexOf(' ', i);
      if (j == -1) {
        return optString.substring(i);
      }
      else {
        return optString.substring(i, j);
      }
    }
    return null;
  }
}
