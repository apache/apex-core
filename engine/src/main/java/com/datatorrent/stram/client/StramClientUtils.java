/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAGContext;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;

import com.datatorrent.stram.license.util.Util;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.commons.lang.StringUtils;

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
  public static final String DT_LICENSE_FILE = LogicalPlanConfiguration.LICENSE_PREFIX + "file";
  public static final String DT_LICENSE_MASTER_MEMORY = LogicalPlanConfiguration.LICENSE_PREFIX + "MASTER_MEMORY_MB";
  public static final String DT_DFS_ROOT_DIR = DAGContext.DT_PREFIX + "dfsRootDirectory";

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

    public List<ApplicationReport> getAllApplicationReports() throws IOException, YarnException
    {
      GetApplicationsRequest applicationsRequest = Records.newRecord(GetApplicationsRequest.class);
      GetApplicationsResponse applicationsResponse = clientRM.getApplications(applicationsRequest);
      return applicationsResponse.getApplicationList();
    }

    public ApplicationReport getApplicationReport(String appId) throws IOException, YarnException
    {
      ApplicationId applicationId = ConverterUtils.toApplicationId(appId);
      return getApplicationReport(applicationId);
    }

    /**
     * Kill a submitted application by sending a call to the ASM
     *
     * @param appId Application Id to be killed.
     * @throws YarnException
     * @throws IOException
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
     * @param callback
     * @param timeoutMillis
     * @return true if application completed successfully
     * @throws YarnException
     * @throws IOException
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
            LOG.info("Application finished unsuccessfully."
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
  /**
   * Provides a way to run applications with different .m2 and .dt location for internal use.
   * The environment variable was renamed as "DT_HOME" is used for other purposes
   */
  public static final String DT_HOME = System.getenv("_DT_CLIENT_ROOT");

  public static File getSettingsRootDir()
  {
    if (DT_HOME == null || DT_HOME.isEmpty()) {
      return new File(FileUtils.getUserDirectory(), ".dt");
    }

    return new File(DT_HOME, ".dt");
  }

  private static final String DT_DEFAULT_XML_FILE = "dt-default.xml";
  public static final String DT_SITE_XML_FILE = "dt-site.xml";
  public static final String DT_ENV_SH_FILE = "dt-env.sh";

  public static Configuration addDTDefaultResources(Configuration conf)
  {
    conf.addResource(DT_DEFAULT_XML_FILE);
    return conf;
  }

  public static Configuration addDTSiteResources(Configuration conf)
  {
    conf.addResource(DT_DEFAULT_XML_FILE);
    conf.addResource(DT_SITE_XML_FILE);
    File cfgResource = new File(StramClientUtils.getSettingsRootDir(), StramClientUtils.DT_SITE_XML_FILE);
    if (cfgResource.exists()) {
      LOG.info("Loading settings: " + cfgResource.toURI());
      conf.addResource(new Path(cfgResource.toURI()));
    }
    return conf;
  }

  public static URL getDTSiteXmlFile()
  {
    URL resource = StramClientUtils.class.getResource(DT_SITE_XML_FILE);
    if (resource == null) {
      try {
        resource = new URL("file:" + System.getProperty("user.home") + "/.dt/dt-site.xml");
      }
      catch (MalformedURLException ex) {
        LOG.error("Caught exception: ", ex);
      }
    }
    return resource;
  }

  public static Path getDTRootDir(FileSystem fs, Configuration conf)
  {
    return conf.get(DT_DFS_ROOT_DIR) == null ? fs.getHomeDirectory() : new Path(fs.getUri().getScheme(), fs.getUri().getAuthority(), conf.get(DT_DFS_ROOT_DIR));
  }

  public static byte[] getLicense(Configuration conf) throws IOException
  {
    String dtLicenseFile = conf.get(DT_LICENSE_FILE);
    if (StringUtils.isBlank(dtLicenseFile)) {
      return Util.getDefaultLicense();
    }
    else {
      return getLicense(dtLicenseFile);
    }
  }

  public static byte[] getLicense(String filePath) throws IOException
  {
    return IOUtils.toByteArray(new FileInputStream(filePath));
  }

  public static int getLicenseMasterMemory(Configuration conf)
  {
    return conf.getInt(DT_LICENSE_MASTER_MEMORY, 256);
  }

  /**
   * Change DT environment variable in the env file.
   * Calling this will require a restart for the new setting to take place
   *
   * @param key
   * @param value
   * @throws IOException
   */
  public static void changeDTEnvironment(String key, String value) throws IOException
  {
    URL resource = StramClientUtils.class.getResource(DT_ENV_SH_FILE);
    if (resource == null) {
      File envFile = new File(StramClientUtils.getSettingsRootDir(), StramClientUtils.DT_ENV_SH_FILE);
      FileOutputStream out = new FileOutputStream(envFile);
      try {
        out.write(("export " + key + "=\"" + value + "\"\n").getBytes());
      }
      finally {
        out.close();
      }
    }
    else {
      try {
        File cfgResource = new File(resource.toURI());
        synchronized (StramClientUtils.class) {
          BufferedReader br = new BufferedReader(new FileReader(cfgResource));
          StringBuilder sb = new StringBuilder();
          try {
            String line;
            boolean changed = false;
            while ((line = br.readLine()) != null) {
              try {
                line = line.trim();
                if (line.startsWith("#")) {
                  continue;
                }
                if (line.matches("export\\s+" + key + "=.*")) {
                  line = "export " + key + "=\"" + value + "\"";
                  changed = true;
                }
              }
              finally {
                sb.append(line).append("\n");
              }
            }
            if (!changed) {
              sb.append("export ").append(key).append("=\"").append(value).append("\"\n");
            }
          }
          finally {
            br.close();
          }
          if (sb.length() > 0) {
            FileOutputStream out = new FileOutputStream(cfgResource);
            try {
              out.write(sb.toString().getBytes());
            }
            finally {
              out.close();
            }
          }
        }
      }
      catch (URISyntaxException ex) {
        LOG.error("Caught exception when getting env resource:", ex);
      }
    }
  }

}
