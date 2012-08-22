/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.ClusterInfo;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;


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

  CharSequence getUser();

  Clock getClock();
  
  EventHandler<?> getEventHandler();

  ClusterInfo getClusterInfo();
}
