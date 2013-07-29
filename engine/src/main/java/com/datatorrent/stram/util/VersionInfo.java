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
public class VersionInfo {

  private static String version = "Unknown";
  private static String user = "Unknown";
  private static String date = "Unknown";
  private static String revision = "Unknown";

  static {
    try {
      URL res = VersionInfo.class.getResource(VersionInfo.class.getSimpleName() + ".class");
      URLConnection conn = (URLConnection) res.openConnection();
      if (conn instanceof JarURLConnection) {
        Manifest mf = ((JarURLConnection) conn).getManifest();
        Attributes mainAttribs = mf.getMainAttributes();
        String builtBy = mainAttribs.getValue("Built-By");
        if(builtBy != null) {
          VersionInfo.user = builtBy;
        }
      }
      
      Enumeration<URL> resources = VersionInfo.class.getClassLoader().getResources("META-INF/maven/com.datatorrent/malhar-stram/pom.properties");
      while (resources.hasMoreElements()) {
        Properties pomInfo = new Properties();
        pomInfo.load(resources.nextElement().openStream());
        String v = pomInfo.getProperty("version", "unknown");
        VersionInfo.version = v;
      }
      
      resources = VersionInfo.class.getClassLoader().getResources("malhar-stram-git.properties");
      while (resources.hasMoreElements()) {
        Properties gitInfo = new Properties();
        gitInfo.load(resources.nextElement().openStream());
        String commitAbbrev = gitInfo.getProperty("git.commit.id.abbrev", "unknown");
        String branch = gitInfo.getProperty("git.branch", "unknown");
        VersionInfo.revision = "rev: " + commitAbbrev + " branch: " + branch;
        VersionInfo.date = gitInfo.getProperty("git.build.time", VersionInfo.date);
        VersionInfo.user = gitInfo.getProperty("git.build.user.name", VersionInfo.user);
        break;
      }

    }
    catch (IOException e) {
      org.slf4j.LoggerFactory.getLogger(VersionInfo.class).error("Failed to read version info", e);
    }
  }

  /**
   * Get the version.
   *
   * @return the version string, e.g. "0.3.4-SNAPSHOT"
   */
  public static String getVersion() {
    return version;
  }

  /**
   * The date of the build.
   *
   * @return the compilation date
   */
  public static String getDate() {
    return date;
  }

  /**
   * The user that made the build.
   *
   * @return the username of the user
   */
  public static String getUser() {
    return user;
  }

  /**
   * Get the SCM revision number
   * @return the revision number, eg. "451451"
   */
  public static String getRevision() {
    return revision;
  }

  /**
   * Returns the buildVersion which includes version, revision, user and date.
   */
  public static String getBuildVersion() {
    return VersionInfo.getVersion() + " from " + VersionInfo.getRevision() + " by " + VersionInfo.getUser() + " on " + VersionInfo.getDate();
  }

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public static void main(String[] args) {
    System.out.println("Malhar " + getVersion());
    System.out.println("Revision " + getRevision());
    System.out.println("Compiled by " + getUser() + " on " + getDate());
  }

}
