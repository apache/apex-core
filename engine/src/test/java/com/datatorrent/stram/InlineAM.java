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
package com.datatorrent.stram;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

/**
 * Base class for a simple client that runs the AM logic inline. The AM is not
 * launched and managed by the RM. The client creates a new application on the
 * RM and negotiates a new attempt id. Then it waits for the RM app state to
 * reach be YarnApplicationState.ACCEPTED after which it runs the AM logic.
 */
public abstract class InlineAM
{
  private static final Log LOG = LogFactory.getLog(InlineAM.class);

  // Handle to talk to the Resource Manager/Applications Manager
  private final YarnClientImpl rmClient;

  // Application master specific info to register a new Application with RM/ASM
  private String appName = "";
  // App master priority
  private int amPriority = 0;
  // Queue for App master
  private String amQueue = "";

  /**
   */
  public InlineAM(Configuration conf) throws Exception
  {

    appName = "UnmanagedAM";
    amPriority = 0;
    amQueue = "default";

    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    rmClient = new YarnClientImpl();
    rmClient.init(yarnConf);
  }

  public abstract void runAM(ApplicationAttemptId attemptId) throws Exception;

  public boolean run() throws Exception
  {
    LOG.info("Starting Client");

    // Connect to ResourceManager
    rmClient.start();
    try {
      // Get a new application id
      YarnClientApplication newApp = rmClient.createApplication();
      ApplicationId appId = newApp.getNewApplicationResponse().getApplicationId();

      // Create launch context for app master
      LOG.info("Setting up application submission context for ASM");
      ApplicationSubmissionContext appContext = Records
          .newRecord(ApplicationSubmissionContext.class);

      // set the application id
      appContext.setApplicationId(appId);
      // set the application name
      appContext.setApplicationName(appName);

      // Set the priority for the application master
      Priority pri = Records.newRecord(Priority.class);
      pri.setPriority(amPriority);
      appContext.setPriority(pri);

      // Set the queue to which this application is to be submitted in the RM
      appContext.setQueue(amQueue);

      // Set up the container launch context for the application master
      ContainerLaunchContext amContainer = Records
          .newRecord(ContainerLaunchContext.class);
      appContext.setAMContainerSpec(amContainer);

      // unmanaged AM
      appContext.setUnmanagedAM(true);
      LOG.info("Setting unmanaged AM");

      // Submit the application to the applications manager
      LOG.info("Submitting application to ASM");
      rmClient.submitApplication(appContext);

      // Monitor the application to wait for launch state
      ApplicationReport appReport = monitorApplication(appId,
          EnumSet.of(YarnApplicationState.ACCEPTED));
      ApplicationAttemptId attemptId = appReport.getCurrentApplicationAttemptId();
      LOG.info("Launching application with id: " + attemptId);

      // launch AM
      runAM(attemptId);

      // Monitor the application for end state
      appReport = monitorApplication(appId, EnumSet.of(
          YarnApplicationState.KILLED, YarnApplicationState.FAILED,
          YarnApplicationState.FINISHED));
      YarnApplicationState appState = appReport.getYarnApplicationState();
      FinalApplicationStatus appStatus = appReport.getFinalApplicationStatus();

      LOG.info("App ended with state: " + appReport.getYarnApplicationState()
          + " and status: " + appStatus);

      boolean success;
      if (YarnApplicationState.FINISHED == appState
          && FinalApplicationStatus.SUCCEEDED == appStatus) {
        LOG.info("Application has completed successfully.");
        success = true;
      } else {
        LOG.info("Application did finished unsuccessfully." + " YarnState="
            + appState.toString() + ", FinalStatus=" + appStatus.toString());
        success = false;
      }

      return success;
    } finally {
      rmClient.stop();
    }
  }

  /**
   * Monitor the submitted application for completion. Kill application if time
   * expires.
   *
   * @param appId
   *          Application Id of application to be monitored
   * @return true if application completed successfully
   * @throws YarnRemoteException
   */
  private ApplicationReport monitorApplication(ApplicationId appId,
      Set<YarnApplicationState> finalState) throws YarnException, IOException
  {

    while (true) {

      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in
      ApplicationReport report = rmClient.getApplicationReport(appId);

      LOG.info("Got application report from ASM for" + ", appId="
          + appId.getId() + ", appAttemptId="
          + report.getCurrentApplicationAttemptId() + ", clientToken="
          + report.getClientToAMToken() + ", appDiagnostics="
          + report.getDiagnostics() + ", appMasterHost=" + report.getHost()
          + ", appQueue=" + report.getQueue() + ", appMasterRpcPort="
          + report.getRpcPort() + ", appStartTime=" + report.getStartTime()
          + ", yarnAppState=" + report.getYarnApplicationState().toString()
          + ", distributedFinalState="
          + report.getFinalApplicationStatus().toString() + ", appTrackingUrl="
          + report.getTrackingUrl() + ", appUser=" + report.getUser());

      YarnApplicationState state = report.getYarnApplicationState();
      if (finalState.contains(state)) {
        return report;
      }

    }

  }

}
