/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.stram.api.AppDataSource;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.datatorrent.api.Attribute.AttributeMap.AttributeInitializer;
import com.datatorrent.api.Context;

import com.datatorrent.stram.webapp.AppInfo;
import java.util.List;
import java.util.Map;

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

  String getApplicationDocLink();

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

  String getGatewayAddress();

  @SuppressWarnings("FieldNameHidesFieldInSuperclass")
  long serialVersionUID = AttributeInitializer.initialize(StramAppContext.class);

  boolean isGatewayConnected();

  List<AppDataSource> getAppDataSources();

  Map<String, Object> getCustomMetrics();
}
