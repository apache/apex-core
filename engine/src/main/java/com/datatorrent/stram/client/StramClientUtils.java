/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.client;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Collection of utility classes for command line interface package<p>
 * <br>
 * List includes<br>
 * Yarn Client Helper<br>
 * Resource Mgr Client Helper<br>
 * <br>
 *
 * @since 0.3.2
 */
public class StramClientUtils
{
  /**
   *
   * TBD<p>
   * <br>
   *
   */
  public static class YarnClientHelper
  {
    private static final Logger LOG = LoggerFactory.getLogger(YarnClientHelper.class);
    // Configuration
    private final Configuration conf;
    // RPC to communicate to RM
    private final YarnRPC rpc;

    public YarnClientHelper(Configuration conf)
    {
      // Set up the configuration and RPC
      this.conf = conf;
      this.rpc = YarnRPC.create(conf);
    }

    public Configuration getConf()
    {
      return this.conf;
    }

    public YarnRPC getYarnRPC()
    {
      return rpc;
    }

    /**
     * Connect to the Resource Manager/Applications Manager<p>
     *
     * @return Handle to communicate with the ASM
     * @throws IOException
     */
    public ApplicationClientProtocol connectToASM() throws IOException
    {
      YarnConfiguration yarnConf = new YarnConfiguration(conf);
      InetSocketAddress rmAddress = yarnConf.getSocketAddr(
              YarnConfiguration.RM_ADDRESS,
              YarnConfiguration.DEFAULT_RM_ADDRESS,
              YarnConfiguration.DEFAULT_RM_PORT);
      LOG.debug("Connecting to ResourceManager at " + rmAddress);
      return ((ApplicationClientProtocol)rpc.getProxy(
          ApplicationClientProtocol.class, rmAddress, conf));
    }

    /**
     * Connect to the Resource Manager<p>
     *
     * @return Handle to communicate with the RM
     */
    public ApplicationMasterProtocol connectToRM()
    {
      InetSocketAddress rmAddress = conf.getSocketAddr(
              YarnConfiguration.RM_SCHEDULER_ADDRESS,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
      LOG.debug("Connecting to ResourceManager at " + rmAddress);
      return ((ApplicationMasterProtocol)rpc.getProxy(ApplicationMasterProtocol.class, rmAddress, conf));
    }

  }

  /**
   *
   * Bunch of utilities that ease repeating interactions with {@link ClientRMProtocol}<p>
   *
   */
  public static class ClientRMHelper
  {
    private static final Logger LOG = LoggerFactory.getLogger(ClientRMHelper.class);
    public final ApplicationClientProtocol clientRM;

    public ClientRMHelper(YarnClientHelper yarnClient) throws IOException
    {
      this.clientRM = yarnClient.connectToASM();
    }

    public ApplicationReport getApplicationReport(ApplicationId appId) throws YarnException, IOException
    {
      // Get application report for the appId we are interested in
      GetApplicationReportRequest reportRequest = Records.newRecord(GetApplicationReportRequest.class);
      reportRequest.setApplicationId(appId);
      GetApplicationReportResponse reportResponse = clientRM.getApplicationReport(reportRequest);
      ApplicationReport report = reportResponse.getApplicationReport();
      return report;
    }

    /**
     * Kill a submitted application by sending a call to the ASM
     *
     * @param appId Application Id to be killed.
     * @throws YarnRemoteException
     */
    public void killApplication(ApplicationId appId) throws YarnException, IOException
    {
      KillApplicationRequest request = Records.newRecord(KillApplicationRequest.class);
      // TODO clarify whether multiple jobs with the same app id can be submitted and be running at
      // the same time.
      // If yes, can we kill a particular attempt only?
      request.setApplicationId(appId);
      // KillApplicationResponse response = applicationsManager.forceKillApplication(request);
      // Response can be ignored as it is non-null on success or
      // throws an exception in case of failures
      clientRM.forceKillApplication(request);
    }

    public static interface AppStatusCallback
    {
      boolean exitLoop(ApplicationReport report);

    }

    /**
     * Monitor the submitted application for completion. Kill application if time expires.
     *
     * @param appId Application Id of application to be monitored
     * @return true if application completed successfully
     * @throws YarnRemoteException
     */
    @SuppressWarnings("SleepWhileInLoop")
    public boolean waitForCompletion(ApplicationId appId, AppStatusCallback callback, long timeoutMillis) throws YarnException, IOException
    {
      long startMillis = System.currentTimeMillis();
      while (true) {

        // Check app status every 1 second.
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException e) {
          LOG.debug("Thread sleep in monitoring loop interrupted");
        }

        ApplicationReport report = getApplicationReport(appId);
        if (callback.exitLoop(report) == true) {
          return true;
        }

        YarnApplicationState state = report.getYarnApplicationState();
        FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
        if (YarnApplicationState.FINISHED == state) {
          if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
            LOG.info("Application has completed successfully. Breaking monitoring loop");
            return true;
          }
          else {
            LOG.info("Application did finished unsuccessfully."
                    + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                    + ". Breaking monitoring loop");
            return false;
          }
        }
        else if (YarnApplicationState.KILLED == state
                || YarnApplicationState.FAILED == state) {
          LOG.info("Application did not finish."
                  + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                  + ". Breaking monitoring loop");
          return false;
        }

        if (System.currentTimeMillis() - startMillis > timeoutMillis) {
          LOG.info("Reached specified timeout. Killing application");
          killApplication(appId);
          return false;
        }
      }
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(StramClientUtils.class);
  public static final String DT_HOME = System.getenv("_DT_HOME"); // get rid of this hack

  public static File getSettingsRootDir()
  {
    if (DT_HOME == null || DT_HOME.isEmpty()) {
      return new File(FileUtils.getUserDirectory(), ".stram");
    }

    return new File(DT_HOME, ".stram");
  }

  private static final String STRAM_DEFAULT_XML_FILE = "stram-default.xml";
  public static final String STRAM_SITE_XML_FILE = "stram-site.xml";

  public static Configuration addStramResources(Configuration conf)
  {
    conf.addResource(STRAM_DEFAULT_XML_FILE);
    conf.addResource(STRAM_SITE_XML_FILE);
    File cfgResource = new File(StramClientUtils.getSettingsRootDir(), StramClientUtils.STRAM_SITE_XML_FILE);
    if (cfgResource.exists()) {
      LOG.info("Loading settings: " + cfgResource.toURI());
      conf.addResource(new Path(cfgResource.toURI()));
    }
    return conf;
  }

}
