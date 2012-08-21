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


/**
 * Provides application level data like user, appId, elapsed time, etc.<p>
 * <br>Current data includes<br>
 * <b>Application Id</b><br>
 * <b>Application Name</b><br>
 * <b>User Name</b><br>
 * <b>Start Time</b><br>
 * <b>Elapsed Time</b><br>
 * 
 */


@XmlRootElement(name = "info")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppInfo {
  private static Logger LOG = LoggerFactory.getLogger(AppInfo.class);
  
  protected String appId;
  protected String name;
  protected String user;
  protected long startedOn;
  protected long elapsedTime;

  public AppInfo() {
  }

  public AppInfo(StramAppContext context) {
    LOG.info("AppInfo called");
    this.appId = context.getApplicationID().toString();
    this.name = context.getApplicationName();
    this.user = context.getUser().toString();
    this.startedOn = context.getStartTime();
    this.elapsedTime = Times.elapsed(this.startedOn, 0);
  }

  public String getId() {
    return this.appId;
  }

  public String getName() {
    return this.name;
  }

  public String getUser() {
    return this.user;
  }

  public long getStartTime() {
    return this.startedOn;
  }

  public long getElapsedTime() {
    return this.elapsedTime;
  }

}
