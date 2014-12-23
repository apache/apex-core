/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.client;

import java.io.*;
import java.net.*;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.mozilla.javascript.Scriptable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.log4j.DTLoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;

import com.datatorrent.stram.StramClient;
import com.datatorrent.stram.license.License;
import com.datatorrent.stram.license.LicenseAuthority;
import com.datatorrent.stram.license.util.Util;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import com.datatorrent.stram.util.ConfigValidator;

/**
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
  public static final String DT_VERSION = StreamingApplication.DT_PREFIX + "version";
  public static final String DT_DFS_ROOT_DIR = StreamingApplication.DT_PREFIX + "dfsRootDirectory";
  public static final String DT_DFS_USER_NAME = "%USER_NAME%";
  public static final String DT_CONFIG_STATUS = StreamingApplication.DT_PREFIX + "configStatus";
  public static final String SUBDIR_APPS = "apps";
  public static final int RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE = 10 * 1000;

  /**
   * TBD<p>
   * <br>
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
      return ((ApplicationClientProtocol) rpc.getProxy(
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
      return ((ApplicationMasterProtocol) rpc.getProxy(ApplicationMasterProtocol.class, rmAddress, conf));
    }

  }

  /**
   * Bunch of utilities that ease repeating interactions with {@link ClientRMProxy}<p>
   */
  public static class ClientRMHelper
  {
    private static final Logger LOG = LoggerFactory.getLogger(ClientRMHelper.class);
    public final YarnClient clientRM;

    public ClientRMHelper(YarnClient yarnClient) throws IOException
    {
      this.clientRM = yarnClient;
    }

    public static interface AppStatusCallback
    {
      boolean exitLoop(ApplicationReport report);

    }

    /**
     * Monitor the submitted application for completion. Kill application if time expires.
     *
     * @param appId         Application Id of application to be monitored
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

        ApplicationReport report = clientRM.getApplicationReport(appId);
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
          clientRM.killApplication(appId);
          return false;
        }
      }
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(StramClientUtils.class);

  public static String getHostName()
  {
    try {
      return java.net.InetAddress.getLocalHost().getHostName();
    }
    catch (UnknownHostException ex) {
      return null;
    }
  }

  public static File getUserDTDirectory()
  {
    return new File(FileUtils.getUserDirectory(), ".dt");
  }

  public static File getConfigDir()
  {
    URL resource = StramClientUtils.class.getClassLoader().getResource(DT_ENV_SH_FILE);
    try {
      if (resource == null) {
        return getUserDTDirectory();
      }
      return new File(resource.toURI()).getParentFile();
    }
    catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static File getInstallationDir()
  {
    URL resource = StramClientUtils.class.getClassLoader().getResource(DT_ENV_SH_FILE);
    try {
      if (resource == null) {
        return null;
      }
      return new File(resource.toURI()).getParentFile().getParentFile();
    }
    catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static boolean isDevelopmentMode()
  {
    return getUserDTDirectory().equals(getConfigDir());
  }

  public static File getBackupsDirectory()
  {
    return new File(getConfigDir(), BACKUPS_DIRECTORY);
  }

  public static final String DT_DEFAULT_XML_FILE = "dt-default.xml";
  public static final String DT_SITE_XML_FILE = "dt-site.xml";
  public static final String DT_SITE_GLOBAL_XML_FILE = "dt-site-global.xml";
  public static final String DT_ENV_SH_FILE = "dt-env.sh";
  public static final String BACKUPS_DIRECTORY = "backups";

  public static Configuration addDTDefaultResources(Configuration conf)
  {
    conf.addResource(DT_DEFAULT_XML_FILE);
    return conf;
  }

  public static Configuration addDTSiteResources(Configuration conf)
  {
    addDTLocalResources(conf);
    FileSystem fs = null;
    try {
      fs = newFileSystemInstance(conf);
      // after getting the dfsRootDirectory config parameter, redo the entire process with the global config
      // load global settings from DFS
      File targetGlobalFile = new File(String.format("/tmp/dt-site-global-%s.xml", UserGroupInformation.getLoginUser().getShortUserName()));
      fs.copyToLocalFile(new org.apache.hadoop.fs.Path(StramClientUtils.getDTDFSConfigDir(fs, conf), StramClientUtils.DT_SITE_GLOBAL_XML_FILE),
        new org.apache.hadoop.fs.Path(targetGlobalFile.toURI()));
      addDTSiteResources(conf, targetGlobalFile);
      if (!isDevelopmentMode()) {
        // load node local config file
        addDTSiteResources(conf, new File(StramClientUtils.getConfigDir(), StramClientUtils.DT_SITE_XML_FILE));
      }
      // load user config file
      addDTSiteResources(conf, new File(StramClientUtils.getUserDTDirectory(), StramClientUtils.DT_SITE_XML_FILE));
    }
    catch (IOException ex) {
      // ignore
      LOG.debug("Caught exception when loading configuration: {}: moving on...", ex.getMessage());
    }
    finally {
      IOUtils.closeQuietly(fs);
    }

    //Validate loggers-level settings
    String loggersLevel = conf.get(DTLoggerFactory.DT_LOGGERS_LEVEL);
    if (loggersLevel != null) {
      String targets[] = loggersLevel.split(",");
      Preconditions.checkArgument(targets.length > 0, "zero loggers level");
      for (String target : targets) {
        String parts[] = target.split(":");
        Preconditions.checkArgument(parts.length == 2, "incorrect " + target);
        Preconditions.checkArgument(ConfigValidator.validateLoggersLevel(parts[0], parts[1]), "incorrect " + target);
      }
    }
    convertDeprecatedProperties(conf);

    //
    // The ridiculous default RESOURCEMANAGER_CONNECT_MAX_WAIT_MS from hadoop is 15 minutes (!!!!), which actually translates to 20 minutes with the connect interval.
    // That means if there is anything wrong with YARN or if YARN is not running, the caller has to wait for up to 20 minutes until it gets an error.
    // We are overriding this to be 10 seconds maximum.
    //

    int rmConnectMaxWait = conf.getInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS);
    if (rmConnectMaxWait > RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE) {
      LOG.info("Overriding {} assigned value of {} to {} because the assigned value is too big.", YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, rmConnectMaxWait, RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE);
      conf.setInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE);
      int rmConnectRetryInterval = conf.getInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS);
      int defaultRetryInterval = Math.max(500, RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE / 5);
      if (rmConnectRetryInterval > defaultRetryInterval) {
        LOG.info("Overriding {} assigned value of {} to {} because the assigned value is too big.", YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, rmConnectRetryInterval, defaultRetryInterval);
        conf.setInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, defaultRetryInterval);
      }
    }
    LOG.info(" conf object in stramclient {}", conf);
    return conf;
  }

  public static void addDTLocalResources(Configuration conf)
  {
    conf.addResource(DT_DEFAULT_XML_FILE);
    if (!isDevelopmentMode()) {
      addDTSiteResources(conf, new File(StramClientUtils.getConfigDir(), StramClientUtils.DT_SITE_XML_FILE));
    }
    addDTSiteResources(conf, new File(StramClientUtils.getUserDTDirectory(), StramClientUtils.DT_SITE_XML_FILE));
  }

  private static Configuration addDTSiteResources(Configuration conf, File confFile)
  {
    if (confFile.exists()) {
      LOG.info("Loading settings: " + confFile.toURI());
      conf.addResource(new Path(confFile.toURI()));
    }
    else {
      LOG.info("Configuration file {} is not found. Skipping...", confFile.toURI());
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
        String newKey = StreamingApplication.DT_PREFIX + entry.getKey().substring(6);
        LOG.warn("Configuration property {} is deprecated. Please use {} instead.", entry.getKey(), newKey);
        newEntries.put(newKey, entry.getValue());
        iterator.remove();
      }
      else if (entry.getKey().equals(StreamingApplication.DT_PREFIX + "gateway.address")) {
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
    File cfgResource = new File(StramClientUtils.getConfigDir(), StramClientUtils.DT_SITE_XML_FILE);
    try {
      return cfgResource.toURI().toURL();
    }
    catch (MalformedURLException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static FileSystem newFileSystemInstance(Configuration conf) throws IOException
  {
    String dfsRootDir = conf.get(DT_DFS_ROOT_DIR);
    if (StringUtils.isBlank(dfsRootDir)) {
      return FileSystem.newInstance(conf);
    }
    else {
      if (dfsRootDir.contains(DT_DFS_USER_NAME)) {
        dfsRootDir = dfsRootDir.replace(DT_DFS_USER_NAME, UserGroupInformation.getLoginUser().getShortUserName());
        conf.set(DT_DFS_ROOT_DIR, dfsRootDir);
      }
      try {
        return FileSystem.newInstance(new URI(dfsRootDir), conf);
      }
      catch (URISyntaxException ex) {
        LOG.warn("{} is not a valid URI. Returning the default filesystem", dfsRootDir, ex);
        return FileSystem.newInstance(conf);
      }
    }
  }

  public static Path getDTDFSRootDir(FileSystem fs, Configuration conf)
  {
    String dfsRootDir = conf.get(DT_DFS_ROOT_DIR);
    if (StringUtils.isBlank(dfsRootDir)) {
      return new Path(fs.getHomeDirectory(), "datatorrent");
    }
    else {
      try {
        if (dfsRootDir.contains(DT_DFS_USER_NAME)) {
          dfsRootDir = dfsRootDir.replace(DT_DFS_USER_NAME, UserGroupInformation.getLoginUser().getShortUserName());
          conf.set(DT_DFS_ROOT_DIR, dfsRootDir);
        }
        URI uri = new URI(dfsRootDir);
        if (uri.isAbsolute()) {
          return new Path(uri);
        }
      }
      catch (IOException ex) {
        LOG.warn("Error getting user login name {}", dfsRootDir, ex);
      }
      catch (URISyntaxException ex) {
        LOG.warn("{} is not a valid URI. Using the default filesystem to construct the path", dfsRootDir, ex);
      }
      return new Path(fs.getUri().getScheme(), fs.getUri().getAuthority(), dfsRootDir);
    }
  }

  public static Path getDTDFSConfigDir(FileSystem fs, Configuration conf)
  {
    return new Path(getDTDFSRootDir(fs, conf), "conf");
  }

  public static byte[] getLicense(Configuration conf) throws IOException, URISyntaxException
  {
    String dtLicenseFile = conf.get(DT_LICENSE_FILE);
    if (StringUtils.isBlank(dtLicenseFile)) {
      return Util.getDefaultLicense();
    }
    else {
      return getLicense(dtLicenseFile, conf);
    }
  }

  public static byte[] getLicense(String uriString, Configuration conf) throws IOException, URISyntaxException
  {
    URI uri = new URI(uriString);
    InputStream is = null;
    FileSystem fs = null;

    try {
      if (uri.getScheme() == null || uri.getScheme().equals("file")) {
        is = new FileInputStream(uri.getPath());
      }
      else {
        fs = FileSystem.newInstance(uri, conf);
        is = fs.open(new Path(uri));
      }
      return IOUtils.toByteArray(is);
    }
    finally {
      IOUtils.closeQuietly(is);
      IOUtils.closeQuietly(fs);
    }
  }

  public static int getLicenseMasterMemory(Configuration conf)
  {
    return conf.getInt(DT_LICENSE_MASTER_MEMORY, 512);
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
    if (isDevelopmentMode()) {
      throw new IllegalStateException("Cannot change DT environment in development mode.");
    }
    URL resource = StramClientUtils.class.getClassLoader().getResource(DT_ENV_SH_FILE);
    if (resource == null) {
      File envFile = new File(StramClientUtils.getUserDTDirectory(), StramClientUtils.DT_ENV_SH_FILE);
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
          StringBuilder sb = new StringBuilder(1024);
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

  public static void copyFromLocalFileNoChecksum(FileSystem fs, File fromLocal, Path toDFS) throws IOException
  {
    // This is to void the hadoop FileSystem API to perform checksum on the local file
    // This "feature" has caused a lot of headache because the local file can be copied from HDFS and modified,
    // and the checksum will fail if the file is again copied to HDFS
    try {
      new File(fromLocal.getParentFile(), "." + fromLocal.getName() + ".crc").delete();
    }
    catch (Exception ex) {
      // ignore
    }
    fs.copyFromLocalFile(new Path(fromLocal.toURI()), toDFS);
  }

  public static String checkAndActivateCurrentLicense(Configuration conf) throws Exception
  {
    // start the license manager
    YarnClient yarnClient = YarnClient.createYarnClient();
    try {
      yarnClient.init(conf);
      yarnClient.start();
      byte[] licenseBytes = StramClientUtils.getLicense(conf);
      License license = LicenseAuthority.getLicense(licenseBytes);
      String licenseId = license.getLicenseId();

      List<ApplicationReport> apps = yarnClient.getApplications(Sets.newHashSet(StramClient.YARN_APPLICATION_TYPE_LICENSE),
        EnumSet.of(YarnApplicationState.RUNNING,
          YarnApplicationState.ACCEPTED,
          YarnApplicationState.NEW,
          YarnApplicationState.NEW_SAVING));
      for (ApplicationReport ar : apps) {
        if (ar.getName().equals(licenseId)) {
          return licenseId;
        }
      }
      return StramClientUtils.activateLicense(null, conf);
    }
    finally {
      yarnClient.stop();
    }
  }

  public static String activateLicense(String file, Configuration conf) throws Exception
  {
    byte[] licenseBytes;
    if (file != null) {
      licenseBytes = StramClientUtils.getLicense(file, conf);
    }
    else {
      licenseBytes = StramClientUtils.getLicense(conf);
    }
    String licenseId = LicenseAuthority.getLicenseID(licenseBytes);
    LicenseAuthority.validateLicense(licenseBytes);
    LogicalPlan lp = new LogicalPlan();
    lp.setAttribute(DAG.APPLICATION_NAME, licenseId);
    lp.setAttribute(LogicalPlan.LICENSE, Base64.encodeBase64URLSafeString(licenseBytes)); // TODO: obfuscate license passing
    int licenseMasterMemoryMB = StramClientUtils.getLicenseMasterMemory(conf);
    lp.setAttribute(Context.DAGContext.MASTER_MEMORY_MB, licenseMasterMemoryMB);
    lp.setAttribute(LogicalPlan.LICENSE_ROOT, conf.get(StramClientUtils.DT_DFS_ROOT_DIR));
    StramClient client = new StramClient(conf, lp);
    try {
      client.start();
      client.setApplicationType(StramClient.YARN_APPLICATION_TYPE_LICENSE);
      client.startApplication();
    }
    finally {
      client.stop();
    }
    return licenseId;
  }

  public static boolean configComplete(Configuration conf)
  {
    String configStatus = conf.get(StramClientUtils.DT_CONFIG_STATUS);
    return "complete".equals(configStatus);
  }

  public static void evalProperties(DTConfiguration launchProperties)
  {
    // Using the hadoop configuration to get the values after variable substitution.

    Configuration hadoopConfig = new Configuration(false);

    for (Map.Entry<String, String> entry : launchProperties) {
      hadoopConfig.set(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, String> entry : hadoopConfig) {
      // cannot use entry.getValue() because it won't perform variable substitution
      launchProperties.setInternal(entry.getKey(), hadoopConfig.get(entry.getKey()));
    }

    Pattern pattern = Pattern.compile("\\{% (.+?) %\\}");

    ScriptEngineManager manager = new ScriptEngineManager();
    ScriptEngine engine = manager.getEngineByName("Javascript");
    org.mozilla.javascript.Context context = org.mozilla.javascript.Context.enter();
    context.setOptimizationLevel(-1);
    Scriptable scope = context.initStandardObjects();
    try {
      context.evaluateString(scope, "var _prop = {}", "EvalLaunchProperties", 0, null);
      for (Map.Entry<String, String> entry : launchProperties) {
        LOG.info("Evaluating: {}", "_prop[\"" + entry.getKey() + "\"] = " + entry.getValue());
        context.evaluateString(scope, "_prop[\"" + entry.getKey() + "\"] = \"" + StringEscapeUtils.escapeJava(entry.getValue()) + "\"", "EvalLaunchProperties", 0, null);
      }

      for (Map.Entry<String, String> entry : launchProperties) {
        String value = entry.getValue();

        Matcher matcher = pattern.matcher(value);
        if (matcher.find()) {
          StringBuilder newValue = new StringBuilder();
          int cursor = 0;
          do {
            newValue.append(value.substring(cursor, matcher.start()));
            String eval = context.evaluateString(scope, matcher.group(1), "EvalLaunchProperties", 0, null).toString();
            if (eval != null) {
              newValue.append(eval);
            }
            cursor = matcher.end();
          }
          while (matcher.find());
          newValue.append(value.substring(cursor));
          try {
            launchProperties.set(entry.getKey(), newValue.toString(), DTConfiguration.Scope.TRANSIENT, null);
          }
          catch (DTConfiguration.ConfigException ex) {
            LOG.error("Caught exception:", ex);
          }
        }
      }
    }
    finally {
      org.mozilla.javascript.Context.exit();
    }
  }

  public static <T> T doAs(String userName, PrivilegedExceptionAction<T> action) throws Exception
  {
    if (StringUtils.isNotBlank(userName) && !userName.equals(UserGroupInformation.getLoginUser().getShortUserName())) {
      LOG.info("Executing command as {}", userName);
      UserGroupInformation ugi
              = UserGroupInformation.createProxyUser(userName, UserGroupInformation.getLoginUser());
      return ugi.doAs(action);
    }
    else {
      LOG.info("Executing command as if there is no login info: {}", userName);
      return action.run();
    }
  }

}
