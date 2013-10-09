/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.Context;

import com.datatorrent.stram.webapp.AppInfo;

/**
 *
 * Context interface for sharing information across components in YARN App<p>
 * <br>
 *
 * @since 0.3.2
 */
@InterfaceAudience.Private
public interface StramAppContext extends Context
{
  ApplicationId getApplicationID();

  ApplicationAttemptId getApplicationAttemptId();

  String getApplicationName();

  long getStartTime();

  String getApplicationPath();

  /**
   * The direct URL to access the app master web services.
   * This is to allow requests other then GET - see YARN-156
   *
   * @return
   */
  String getAppMasterTrackingUrl();

  CharSequence getUser();

  Clock getClock();

  AppInfo.AppStats getStats();

  String getDaemonAddress();

  public class AttributeKey<T> extends AttributeMap.AttributeKey<T>
  {
    public final Class<T> attributeType;
    private final static Set<AttributeKey<?>> INSTANCES = new HashSet<AttributeKey<?>>();

    @SuppressWarnings("LeakingThisInConstructor")
    private AttributeKey(String name, Class<T> type)
    {
      super(StramAppContext.class, name);
      this.attributeType = type;
      INSTANCES.add(this);
    }

  }

}
