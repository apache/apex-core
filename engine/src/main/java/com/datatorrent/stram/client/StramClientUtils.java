/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.client;

import com.datatorrent.api.DAGContext;
import com.datatorrent.stram.license.util.Util;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import java.io.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.*;
import java.net.URL;
import java.util.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
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
  public static final String DT_LICENSE_FILE = LogicalPlanConfiguration.LICENSE_PREFIX + "file";
  public static final String DT_LICENSE_MASTER_MEMORY = LogicalPlanConfiguration.LICENSE_PREFIX + "MASTER_MEMORY_MB";
  public static final String DT_DFS_ROOT_DIR = DAGContext.DT_PREFIX + "dfsRootDirectory";
  public static final String DT_CONFIG_STATUS = DAGContext.DT_PREFIX + "configStatus";
  public static final String SUBDIR_APPS = "apps";
  public static final int RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE = 10 * 1000;

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

  public static File getSettingsRootDir()
  {
    return new File(FileUtils.getUserDirectory(), ".dt");
  }

  public static File getConfigDir()
  {
    URL resource = StramClientUtils.class.getClassLoader().getResource(DT_ENV_SH_FILE);
    try {
      if (resource == null) {
        return getSettingsRootDir();
      }
      return new File(resource.toURI()).getParentFile();
    }
    catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
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

    convertDeprecatedProperties(conf);

    //
    // The ridiculous default RESOURCEMANAGER_CONNECT_MAX_WAIT_MS from hadoop is 15 minutes (!!!!), which actually translates to 20 minutes with the connect interval.
    // That means if there is anything wrong with YARN or if YARN is not running, the caller has to wait for up to 20 minutes until it gets an error.
    // We are overriding this to be 10 seconds maximum.
    //

    int rmConnectMaxWait = conf.getInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE);
    if (rmConnectMaxWait > RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE) {
      conf.setInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE);
    }
    return conf;
  }

  @SuppressWarnings("deprecation")
  private static void convertDeprecatedProperties(Configuration conf)
  {
    Iterator<Map.Entry<String, String>> iterator = conf.iterator();
    Map<String, String> newEntries = new HashMap<String, String>();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      if (entry.getKey().startsWith("stram.")) {
        String newKey = DAGContext.DT_PREFIX + entry.getKey().substring(6);
        LOG.warn("Configuration property {} is deprecated. Please use {} instead.", entry.getKey(), newKey);
        newEntries.put(newKey, entry.getValue());
        iterator.remove();
      }
      else if (entry.getKey().equals(DAGContext.DT_PREFIX + LogicalPlan.GATEWAY_ADDRESS.getName())) {
        String newKey = DAGContext.DT_PREFIX + LogicalPlan.GATEWAY_CONNECT_ADDRESS.getName();
        newEntries.put(newKey, entry.getValue());
        LOG.warn("Configuration property {} is deprecated. Please use {} instead.", entry.getKey(), newKey);
        iterator.remove();
      }
      else if (entry.getKey().equals(DAGContext.DT_PREFIX + "gateway.address")) {
        String newKey = LogicalPlanConfiguration.GATEWAY_LISTEN_ADDRESS;
        newEntries.put(newKey, entry.getValue());
        iterator.remove();
      }
    }
    for (Map.Entry<String, String> entry : newEntries.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
  }

  public static URL getDTSiteXmlFile()
  {
    URL resource = StramClientUtils.class.getClassLoader().getResource(DT_SITE_XML_FILE);
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

  public static FileSystem newFileSystemInstance(Configuration conf) throws IOException
  {
    String dfsRootDir = conf.get(DT_DFS_ROOT_DIR);
    if (dfsRootDir == null) {
      return FileSystem.newInstance(conf);
    }
    else {
      try {
        return FileSystem.newInstance(new URI(dfsRootDir), conf);
      }
      catch (URISyntaxException ex) {
        LOG.warn("{} is not a valid URI. Returning the default filesystem", dfsRootDir, ex);
        return FileSystem.newInstance(conf);
      }
    }
  }

  public static Path getDTRootDir(FileSystem fs, Configuration conf)
  {
    String dfsRootDir = conf.get(DT_DFS_ROOT_DIR);
    if (dfsRootDir == null) {
      return new Path(fs.getHomeDirectory(), "datatorrent");
    }
    else {
      try {
        URI uri = new URI(dfsRootDir);
        if (uri.getScheme() != null) {
          return new Path(uri);
        }
      }
      catch (URISyntaxException ex) {
        LOG.warn("{} is not a valid URI. Using the default filesystem to construct the path", dfsRootDir, ex);
      }
      return new Path(fs.getUri().getScheme(), fs.getUri().getAuthority(), dfsRootDir);
    }
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
    URL resource = StramClientUtils.class.getClassLoader().getResource(DT_ENV_SH_FILE);
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
