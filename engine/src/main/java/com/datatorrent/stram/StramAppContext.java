/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.datatorrent.stram.webapp.AppInfo;


/**
 *
 * Context interface for sharing information across components in YARN App<p>
 * <br>
 *
 */
@InterfaceAudience.Private
public interface StramAppContext {

  ApplicationId getApplicationID();

  ApplicationAttemptId getApplicationAttemptId();

  String getApplicationName();

  long getStartTime();

  String getApplicationPath();

  /**
   * The direct URL to access the app master web services.
   * This is to allow requests other then GET - see YARN-156
   * @return
   */
  String getAppMasterTrackingUrl();

  CharSequence getUser();

  Clock getClock();

  AppInfo.AppStats getStats();

  String getDaemonAddress();

}
