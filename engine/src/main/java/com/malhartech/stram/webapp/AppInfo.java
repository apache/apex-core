/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */

package com.malhartech.stram.webapp;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.util.Times;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.stram.StramAppContext;
import com.malhartech.stram.util.VersionInfo;

/**
 *
 * Provides application level data like user, appId, elapsed time, etc.<p>
 * <br>Current data includes<br>
 * <b>Application Id</b><br>
 * <b>Application Name</b><br>
 * <b>User Name</b><br>
 * <b>Start Time</b><br>
 * <b>Elapsed Time</b><br>
 * <b>Application Path</b><br>
 * <br>
 *
 */


@XmlRootElement(name = StramWebServices.PATH_INFO)
@XmlAccessorType(XmlAccessType.FIELD)
public class AppInfo {
  private static final Logger LOG = LoggerFactory.getLogger(AppInfo.class);

  protected String appId;
  protected String name;
  protected String user;
  protected long startedOn;
  protected long elapsedTime;
  protected String appPath;
  protected String daemonAddress;
  public String appMasterTrackingUrl;
  public String version;
  public AppStats stats;

  /**
   * Default constructor for serialization
   */
  public AppInfo() {
  }


  @XmlRootElement
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class AppStats {
    @javax.xml.bind.annotation.XmlElement
    public int getAllocatedContainers() {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    public int getPlannedContainers() {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    public int getFailedContainers() {
      return 0;
    }

    public int containerMemory;

    @javax.xml.bind.annotation.XmlElement
    public int getNumOperators() {
      return 0;
    }
  }

  /**
   *
   * @param context
   */
  public AppInfo(StramAppContext context) {
    LOG.info("AppInfo called");
    this.appId = context.getApplicationID().toString();
    this.name = context.getApplicationName();
    this.user = context.getUser().toString();
    this.startedOn = context.getStartTime();
    this.elapsedTime = Times.elapsed(this.startedOn, 0);
    this.appPath = context.getApplicationPath();
    this.appMasterTrackingUrl = context.getAppMasterTrackingUrl();
    this.stats = context.getStats();
    this.daemonAddress = context.getDaemonAddress();
    this.version = VersionInfo.getBuildVersion();
  }

  /**
   *
   * @return String
   */
  public String getId() {
    return this.appId;
  }

  /**
   *
   * @return String
   */
  public String getName() {
    return this.name;
  }

  /**
   *
   * @return String
   */
  public String getUser() {
    return this.user;
  }

  /**
   *
   * @return long
   */
  public long getStartTime() {
    return this.startedOn;
  }

  /**
   *
   * @return long
   */
  public long getElapsedTime() {
    return this.elapsedTime;
  }

  public String getAppPath() {
    return this.appPath;
  }

  public String getDaemonAddress() {
    return this.daemonAddress;
  }
}
