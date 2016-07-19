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
import java.net.InetSocketAddress;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * <p>
 * ConfigUtils class.</p>
 *
 * @since 0.3.2
 */
public class ConfigUtils
{
  // TODO: HADOOP UPGRADE - replace with YarnConfiguration constants
  private static final String RM_HA_PREFIX = YarnConfiguration.RM_PREFIX + "ha.";
  public static final String RM_HA_ENABLED = RM_HA_PREFIX + "enabled";
  public static final String RM_HA_IDS = RM_HA_PREFIX + "rm-ids";
  public static final String RM_HA_ID = RM_HA_PREFIX + "id";
  public static final boolean DEFAULT_RM_HA_ENABLED = false;

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

  public static boolean isSSLEnabled(Configuration conf)
  {
    if (HttpConfig.Policy.HTTPS_ONLY == HttpConfig.Policy.fromString(
        conf.get(YarnConfiguration.YARN_HTTP_POLICY_KEY, YarnConfiguration.YARN_HTTP_POLICY_DEFAULT))) {
      return true;
    }
    return false;
  }

  public static String getSchemePrefix(Configuration conf)
  {
    if (isSSLEnabled(conf)) {
      return "https://";
    } else {
      return "http://";
    }
  }

  @Deprecated
  public static String getSchemePrefix(YarnConfiguration conf)
  {
    return getSchemePrefix((Configuration)conf);
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
    } finally {
      yarnLogDir = value;
    }
  }

  private static boolean rawContainerLogWarningPrinted = false;

  public static String getRawContainerLogsUrl(YarnConfiguration conf, String nodeHttpAddress, String appId, String containerId)
  {
    String logDirs = conf.get(YarnConfiguration.NM_LOG_DIRS);
    if (logDirs.startsWith("${yarn.log.dir}")) {
      return ConfigUtils.getSchemePrefix(conf) + nodeHttpAddress + "/logs" + logDirs.substring("${yarn.log.dir}".length()) + "/" + appId + "/" + containerId;
    } else {
      try {
        String logDirsPath = new File(logDirs).getCanonicalPath();
        String yarnLogDirPath = new File(getYarnLogDir()).getCanonicalPath();
        if (logDirsPath.startsWith(yarnLogDirPath)) {
          return ConfigUtils.getSchemePrefix(conf) + nodeHttpAddress + "/logs" + logDirsPath
              .substring(yarnLogDirPath.length()) + "/" + appId + "/" + containerId;
        } else {
          if (!rawContainerLogWarningPrinted) {
            LOG.warn("Cannot determine the location of container logs because of incompatible node manager log location ({}) and yarn log location ({})",
                logDirsPath, yarnLogDirPath);
            rawContainerLogWarningPrinted = true;
          }
        }
      } catch (Exception ex) {
        if (!rawContainerLogWarningPrinted) {
          LOG.warn("Cannot determine the location of container logs because of error: ", ex);
          rawContainerLogWarningPrinted = true;
        }
      }
    }
    return null;
  }

  public static boolean isRMHAEnabled(Configuration conf)
  {
    return conf.getBoolean(RM_HA_ENABLED, DEFAULT_RM_HA_ENABLED);
  }

  public static Collection<String> getRMHAIds(Configuration conf)
  {
    return conf.getStringCollection(RM_HA_IDS);
  }

  private static String getDefineValue(String optString, String name)
  {
    int i = optString.indexOf("-D" + name + "=");
    if (i >= 0) {
      i = i + name.length() + 3;
      int j = optString.indexOf(' ', i);
      if (j == -1) {
        return optString.substring(i);
      } else {
        return optString.substring(i, j);
      }
    }
    return null;
  }
}
