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
package com.datatorrent.stram.webapp;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.util.Times;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.api.AppDataSource;
import com.datatorrent.stram.util.VersionInfo;

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
public class AppInfo
{
  protected String appId;
  protected String name;
  protected String docLink;
  protected String user;
  protected long startTime;
  protected long elapsedTime;
  protected String appPath;
  protected String gatewayAddress;
  protected boolean gatewayConnected;
  protected List<AppDataSource> appDataSources;
  protected Map<String, Object> metrics;
  public Map<String, String> attributes;
  public String appMasterTrackingUrl;
  public String version;
  public AppStats stats;

  /**
   * Default constructor for serialization
   */
  public AppInfo()
  {
  }

  @XmlRootElement
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class AppStats
  {
    @javax.xml.bind.annotation.XmlElement
    @AutoMetric
    public int getAllocatedContainers()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    @AutoMetric
    public int getPlannedContainers()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    @AutoMetric
    public int getFailedContainers()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    @AutoMetric
    public int getNumOperators()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    @AutoMetric
    public long getLatency()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    public long getWindowStartMillis()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    public List<Integer> getCriticalPath()
    {
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
    @AutoMetric
    public long getTuplesProcessedPSMA()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    @AutoMetric
    public long getTotalTuplesProcessed()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    @AutoMetric
    public long getTuplesEmittedPSMA()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    @AutoMetric
    public long getTotalTuplesEmitted()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    @AutoMetric
    public long getTotalMemoryAllocated()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    @AutoMetric
    public long getMemoryRequired()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    @AutoMetric
    public int getTotalVCoresAllocated()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    @AutoMetric
    public int getVCoresRequired()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    @AutoMetric
    public long getTotalBufferServerReadBytesPSMA()
    {
      return 0;
    }

    @javax.xml.bind.annotation.XmlElement
    @AutoMetric
    public long getTotalBufferServerWriteBytesPSMA()
    {
      return 0;
    }
  }

  /**
   *
   * @param context
   */
  public AppInfo(StramAppContext context)
  {
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
    this.version = VersionInfo.APEX_VERSION.getBuildVersion();
    this.attributes = new TreeMap<>();
    for (Map.Entry<Attribute<Object>, Object> entry : AttributeMap.AttributeInitializer.getAllAttributes(context, DAGContext.class).entrySet()) {
      this.attributes.put(entry.getKey().getSimpleName(), entry.getKey().codec.toString(entry.getValue()));
    }
    this.gatewayConnected = context.isGatewayConnected();
    this.appDataSources = context.getAppDataSources();
    this.metrics = context.getMetrics();
  }

  /**
   * @return String
   */
  public String getId()
  {
    return this.appId;
  }

  /**
   * @return String
   */
  public String getName()
  {
    return this.name;
  }

  /**
   * @return String
   */
  public String getUser()
  {
    return this.user;
  }

  /**
   * @return long
   */
  public long getStartTime()
  {
    return this.startTime;
  }

  /**
   * @return long
   */
  public long getElapsedTime()
  {
    return this.elapsedTime;
  }

  public String getAppPath()
  {
    return this.appPath;
  }

  public String getGatewayAddress()
  {
    return this.gatewayAddress;
  }

  public boolean isGatewayConnected()
  {
    return gatewayConnected;
  }

  public List<AppDataSource> getAppDataSources()
  {
    return appDataSources;
  }

  public Map<String, Object> getMetrics()
  {
    return metrics;
  }

}
