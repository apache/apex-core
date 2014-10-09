/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */

package com.datatorrent.stram.webapp;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.util.VersionInfo;
import java.util.*;
import javax.xml.bind.annotation.*;
import org.apache.hadoop.yarn.util.Times;

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
 * @since 0.3.2
 */


@XmlRootElement(name = StramWebServices.PATH_INFO)
@XmlAccessorType(XmlAccessType.FIELD)
public class AppInfo {

  protected String appId;
  protected String name;
  protected String docLink;
  protected String user;
  protected long startTime;
  protected long elapsedTime;
  protected String appPath;
  protected String gatewayAddress;
  protected String licenseId;
  protected long remainingLicensedMB;
  protected long totalLicensedMB;
  protected long allocatedMB;
  protected long licenseInfoLastUpdate;
  protected boolean gatewayConnected;
  public Map<String, Object> attributes;
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

    @javax.xml.bind.annotation.XmlElement
    public int getNumOperators() {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    public long getLatency() {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    public List<Integer> getCriticalPath() {
      return null;
    }

    @javax.xml.bind.annotation.XmlElement
    public long getCurrentWindowId()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    public long getRecoveryWindowId()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    public long getTuplesProcessedPSMA()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    public long getTotalTuplesProcessed()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    public long getTuplesEmittedPSMA()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    public long getTotalTuplesEmitted()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    public long getTotalMemoryAllocated()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    public long getTotalBufferServerReadBytesPSMA()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    public long getTotalBufferServerWriteBytesPSMA()
    {
      return 0;
    }
  }

  /**
   *
   * @param context
   */
  public AppInfo(StramAppContext context) {
    this.appId = context.getApplicationID().toString();
    this.name = context.getApplicationName();
    this.docLink = context.getApplicationDocLink();
    this.user = context.getUser().toString();
    this.startTime = context.getStartTime();
    this.elapsedTime = Times.elapsed(this.startTime, 0);
    this.appPath = context.getApplicationPath();
    this.appMasterTrackingUrl = context.getAppMasterTrackingUrl();
    this.stats = context.getStats();
    this.gatewayAddress = context.getGatewayAddress();
    this.version = VersionInfo.getBuildVersion();
    this.licenseId = context.getLicenseId();
    this.remainingLicensedMB = context.getRemainingLicensedMB();
    this.totalLicensedMB = context.getTotalLicensedMB();
    this.allocatedMB = context.getAllocatedMB();
    this.licenseInfoLastUpdate = context.getLicenseInfoLastUpdate();
    this.attributes = new TreeMap<String, Object>();
    for (Map.Entry<Attribute<Object>, Object> entry : AttributeMap.AttributeInitializer.getAllAttributes(context, DAGContext.class).entrySet()) {
      this.attributes.put(entry.getKey().getSimpleName(), entry.getValue());
    }
    this.gatewayConnected = context.isGatewayConnected();
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
    return this.startTime;
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

  public String getGatewayAddress() {
    return this.gatewayAddress;
  }

  public String getLicenseId()
  {
    return this.licenseId;
  }

  public long getLicenseInfoLastUpdate()
  {
    return this.licenseInfoLastUpdate;
  }

  public long getRemainingLicensedMB()
  {
    return remainingLicensedMB;
  }

  public long getTotalLicensedMB()
  {
    return totalLicensedMB;
  }

  public long getAllocatedMB()
  {
    return allocatedMB;
  }

  public boolean isGatewayConnected()
  {
    return gatewayConnected;
  }

}
