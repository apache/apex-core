package com.malhartech.util;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class finds the build version info from the jar file manifest.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class VersionInfo {
  private static final Logger LOG = LoggerFactory.getLogger(VersionInfo.class);

  private static String version = "Unknown";
  private static String user = "Unknown";
  private static String date = "Unknown";

  static {
    LOG.debug("Version from manifest");
    try {
      Enumeration<URL> resources = VersionInfo.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
      while (resources.hasMoreElements()) {
        Manifest manifest = new Manifest(resources.nextElement().openStream());
        Attributes mainAttribs = manifest.getMainAttributes();
        String version = mainAttribs.getValue("malhar-buildversion");
        if(version != null) {
          VersionInfo.version = version;
          VersionInfo.date = mainAttribs.getValue("malhar-buildtime");
          VersionInfo.user = mainAttribs.getValue("Built-By");
          break;
        }
      }
    }
    catch (IOException e) {
      LOG.error("Failed to read version info", e);
    }
  }

  /**
   * Get the Hadoop version.
   *
   * @return the version string, e.g. "0.6.3-dev"
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
    return "Unknown";
  }

  /**
   * Returns the buildVersion which includes version, revision, user and date.
   */
  public static String getBuildVersion() {
    return VersionInfo.getVersion() + " from " + VersionInfo.getRevision() + " by " + VersionInfo.getUser();
  }

  public static void main(String[] args) {
    System.out.println("Malhar " + getVersion());
    System.out.println("Revision " + getRevision());
    System.out.println("Compiled by " + getUser() + " on " + getDate());
  }

}
