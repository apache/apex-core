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

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This class finds the build version info from the jar file.
 *
 * @since 0.3.2
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class VersionInfo
{
  private String version = "Unknown";
  private String user = "Unknown";
  private String date = "Unknown";
  private String revision = "Unknown";

  public VersionInfo(Class<?> classInJar, String groupId, String artifactId, String gitPropertiesResource)
  {
    try {
      URL res = classInJar.getResource(classInJar.getSimpleName() + ".class");
      URLConnection conn = res.openConnection();
      if (conn instanceof JarURLConnection) {
        Manifest mf = ((JarURLConnection)conn).getManifest();
        Attributes mainAttribs = mf.getMainAttributes();
        String builtBy = mainAttribs.getValue("Built-By");
        if (builtBy != null) {
          this.user = builtBy;
        }
      }

      Enumeration<URL> resources = classInJar.getClassLoader().getResources("META-INF/maven/" + groupId + "/" + artifactId + "/pom.properties");
      while (resources.hasMoreElements()) {
        Properties pomInfo = new Properties();
        pomInfo.load(resources.nextElement().openStream());
        String v = pomInfo.getProperty("version", "unknown");
        this.version = v;
      }

      resources = VersionInfo.class.getClassLoader().getResources(gitPropertiesResource);
      while (resources.hasMoreElements()) {
        Properties gitInfo = new Properties();
        gitInfo.load(resources.nextElement().openStream());
        String commitAbbrev = gitInfo.getProperty("git.commit.id.abbrev", "unknown");
        String branch = gitInfo.getProperty("git.branch", "unknown");
        this.revision = "rev: " + commitAbbrev + " branch: " + branch;
        this.date = gitInfo.getProperty("git.build.time", this.date);
        this.user = gitInfo.getProperty("git.build.user.name", this.user);
        break;
      }

    } catch (IOException e) {
      org.slf4j.LoggerFactory.getLogger(VersionInfo.class).error("Failed to read version info", e);
    }
  }

  /**
   * Get the version.
   *
   * @return the version string, e.g. "0.1.1-SNAPSHOT"
   */
  public String getVersion()
  {
    return version;
  }

  /**
   * The date of the build.
   *
   * @return the compilation date
   */
  public String getDate()
  {
    return date;
  }

  /**
   * The user that made the build.
   *
   * @return the username of the user
   */
  public String getUser()
  {
    return user;
  }

  /**
   * Get the SCM revision number
   *
   * @return the revision number, eg. "451451"
   */
  public String getRevision()
  {
    return revision;
  }

  /**
   * Returns the buildVersion which includes version, revision, user and date.
   */
  public String getBuildVersion()
  {
    return getVersion() + " from " + getRevision() + " by " + getUser() + " on " + getDate();
  }

  private static String groupId = "org.apache.apex";
  private static String artifactId = "apex-engine";
  private static Class<?> classInJar = VersionInfo.class;
  private static String gitPropertiesResource = artifactId + ".git.properties";
  public static final VersionInfo APEX_VERSION = new VersionInfo(classInJar, groupId, artifactId,
      gitPropertiesResource);

  /**
   * Compares two version strings.
   *
   * @param str1 a string of ordinal numbers separated by decimal points.
   * @param str2 a string of ordinal numbers separated by decimal points.
   * @return The result is a negative integer if str1 is _numerically_ less than str2. The result is a positive integer
   * if str1 is _numerically_ greater than str2. The result is zero if the strings are _numerically_ equal.
   */
  public static int compare(String str1, String str2)
  {
    String[] vals1 = normalizeVersion(str1).split("\\.");
    String[] vals2 = normalizeVersion(str2).split("\\.");
    int i = 0;
    while (i < vals1.length && i < vals2.length && vals1[i].equals(vals2[i])) {
      i++;
    }
    if (i < vals1.length && i < vals2.length) {
      if (vals1[i].isEmpty()) {
        vals1[i] = "0";
      }
      if (vals2[i].isEmpty()) {
        vals2[i] = "0";
      }
      int diff = Integer.valueOf(vals1[i]).compareTo(Integer.valueOf(vals2[i]));
      return Integer.signum(diff);
    } else {
      return Integer.signum(vals1.length - vals2.length);
    }
  }

  public static boolean isCompatible(String thisVersion, String requiredVersion)
  {
    String[] thisVersionComponent = normalizeVersion(thisVersion).split("\\.");
    String[] requiredVersionComponent = normalizeVersion(requiredVersion).split("\\.");

    // major version check
    if (!thisVersionComponent[0].equals(requiredVersionComponent[0])) {
      return false;
    }

    // minor version check
    if (Integer.parseInt(thisVersionComponent[1]) < Integer.parseInt(requiredVersionComponent[1])) {
      return false;
    }

    // patch version doesn't matter
    return true;
  }

  private static String normalizeVersion(String ver)
  {
    for (int i = 0; i < ver.length(); i++) {
      char c = ver.charAt(i);
      if (!Character.isDigit(c) && c != '.') {
        return ver.substring(0, i);
      }
    }
    return ver;
  }

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public static void main(String[] args)
  {
    System.out.println("Apex " + APEX_VERSION.getVersion());
    System.out.println("Revision " + APEX_VERSION.getRevision());
    System.out.println("Compiled by " + APEX_VERSION.getUser() + " on " + APEX_VERSION.getDate());
  }

}
