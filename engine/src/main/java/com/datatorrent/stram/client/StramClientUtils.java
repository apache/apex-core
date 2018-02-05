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
package com.datatorrent.stram.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
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
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.StramClient;
import com.datatorrent.stram.StramUtils;
import com.datatorrent.stram.security.StramUserLogin;
import com.datatorrent.stram.util.ConfigUtils;
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
  public static final String DT_VERSION = StreamingApplication.DT_PREFIX + "version";
  public static final String DT_DFS_ROOT_DIR = StreamingApplication.DT_PREFIX + "dfsRootDirectory";
  public static final String APEX_APP_DFS_ROOT_DIR = StreamingApplication.APEX_PREFIX + "app.dfsRootDirectory";
  public static final String DT_DFS_USER_NAME = "%USER_NAME%";
  public static final String DT_CONFIG_STATUS = StreamingApplication.DT_PREFIX + "configStatus";
  public static final String SUBDIR_APPS = "apps";
  public static final String SUBDIR_PROFILES = "profiles";
  public static final String SUBDIR_CONF = "conf";
  public static final long RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE = 10 * 1000;
  public static final String DT_HDFS_TOKEN_MAX_LIFE_TIME = StreamingApplication.DT_PREFIX + "namenode.delegation.token.max-lifetime";
  public static final String DT_HDFS_TOKEN_RENEW_INTERVAL = StreamingApplication.DT_PREFIX + "namenode.delegation.token.renew-interval";
  public static final String HDFS_TOKEN_MAX_LIFE_TIME = "dfs.namenode.delegation.token.max-lifetime";
  public static final String HDFS_TOKEN_RENEW_INTERVAL = "dfs.namenode.delegation.token.renew-interval";
  public static final String DT_RM_TOKEN_MAX_LIFE_TIME = StreamingApplication.DT_PREFIX + "resourcemanager.delegation.token.max-lifetime";
  public static final String DT_RM_TOKEN_RENEW_INTERVAL = StreamingApplication.DT_PREFIX + "resourcemanager.delegation.token.renew-interval";
  @Deprecated
  public static final String KEY_TAB_FILE = StramUserLogin.DT_AUTH_PREFIX + "store.keytab";
  public static final String TOKEN_ANTICIPATORY_REFRESH_FACTOR = StramUserLogin.DT_AUTH_PREFIX + "token.refresh.factor";
  public static final long DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT = 7 * 24 * 60 * 60 * 1000;
  public static final long DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT = 24 * 60 * 60 * 1000;
  public static final String TOKEN_REFRESH_PRINCIPAL = StramUserLogin.DT_AUTH_PREFIX + "token.refresh.principal";
  public static final String TOKEN_REFRESH_KEYTAB = StramUserLogin.DT_AUTH_PREFIX + "token.refresh.keytab";
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
      return ((ApplicationClientProtocol)rpc.getProxy(ApplicationClientProtocol.class, rmAddress, conf));
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
   * Bunch of utilities that ease repeating interactions with {@link ClientRMProxy}<p>
   */
  public static class ClientRMHelper
  {
    private static final Logger LOG = LoggerFactory.getLogger(ClientRMHelper.class);

    private static final String RM_HOSTNAME_PREFIX = YarnConfiguration.RM_PREFIX + "hostname.";

    private final YarnClient clientRM;
    private final Configuration conf;

    public ClientRMHelper(YarnClient yarnClient, Configuration conf) throws IOException
    {
      this.clientRM = yarnClient;
      this.conf = conf;
    }

    public interface AppStatusCallback
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
        } catch (InterruptedException e) {
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
          } else {
            LOG.info("Application finished unsuccessfully."
                + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                + ". Breaking monitoring loop");
            return false;
          }
        } else if (YarnApplicationState.KILLED == state
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

    // TODO: HADOOP UPGRADE - replace with YarnConfiguration constants
    private Token<RMDelegationTokenIdentifier> getRMHAToken(org.apache.hadoop.yarn.api.records.Token rmDelegationToken)
    {
      // Build a list of service addresses to form the service name
      ArrayList<String> services = new ArrayList<>();
      for (String rmId : ConfigUtils.getRMHAIds(conf)) {
        LOG.info("Yarn Resource Manager id: {}", rmId);
        // Set RM_ID to get the corresponding RM_ADDRESS
        services.add(SecurityUtil.buildTokenService(getRMHAAddress(rmId)).toString());
      }
      Text rmTokenService = new Text(Joiner.on(',').join(services));

      return new Token<>(
          rmDelegationToken.getIdentifier().array(),
          rmDelegationToken.getPassword().array(),
          new Text(rmDelegationToken.getKind()),
          rmTokenService);
    }

    public void addRMDelegationToken(final String renewer, final Credentials credentials) throws IOException, YarnException
    {
      // Get the ResourceManager delegation rmToken
      final org.apache.hadoop.yarn.api.records.Token rmDelegationToken = clientRM.getRMDelegationToken(new Text(renewer));

      Token<RMDelegationTokenIdentifier> token;
      // TODO: Use the utility method getRMDelegationTokenService in ClientRMProxy to remove the separate handling of
      // TODO: HA and non-HA cases when hadoop dependency is changed to hadoop 2.4 or above
      if (ConfigUtils.isRMHAEnabled(conf)) {
        LOG.info("Yarn Resource Manager HA is enabled");
        token = getRMHAToken(rmDelegationToken);
      } else {
        LOG.info("Yarn Resource Manager HA is not enabled");
        InetSocketAddress rmAddress = conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
            YarnConfiguration.DEFAULT_RM_ADDRESS,
            YarnConfiguration.DEFAULT_RM_PORT);

        token = ConverterUtils.convertFromYarn(rmDelegationToken, rmAddress);
      }

      LOG.info("RM dt {}", token);

      credentials.addToken(token.getService(), token);
    }

    public InetSocketAddress getRMHAAddress(String rmId)
    {
      YarnConfiguration yarnConf = StramClientUtils.getYarnConfiguration(conf);
      yarnConf.set(ConfigUtils.RM_HA_ID, rmId);
      InetSocketAddress socketAddr = yarnConf.getSocketAddr(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS, YarnConfiguration.DEFAULT_RM_PORT);
      yarnConf.unset(ConfigUtils.RM_HA_ID);
      return socketAddr;
    }

  }

  public static YarnClient createYarnClient(Configuration conf)
  {
    YarnClient client = YarnClient.createYarnClient();
    client.init(conf);
    client.start();
    return client;
  }

  private static final Logger LOG = LoggerFactory.getLogger(StramClientUtils.class);

  public static String getHostName()
  {
    try {
      return java.net.InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException ex) {
      return null;
    }
  }

  public static File getUserDTDirectory()
  {
    String envHome = System.getenv("HOME");
    if (StringUtils.isEmpty(envHome)) {
      return new File(FileUtils.getUserDirectory(), ".dt");
    } else {
      return new File(envHome, ".dt");
    }
  }

  public static File getConfigDir()
  {
    URL resource = StramClientUtils.class.getClassLoader().getResource(DT_ENV_SH_FILE);
    try {
      if (resource == null) {
        return getUserDTDirectory();
      }
      return new File(resource.toURI()).getParentFile();
    } catch (URISyntaxException ex) {
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
    } catch (URISyntaxException ex) {
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
  public static final String CUSTOM_ENV_SH_FILE = "custom-env.sh";
  public static final String BACKUPS_DIRECTORY = "backups";

  public static Configuration addDTDefaultResources(Configuration conf)
  {
    conf.addResource(DT_DEFAULT_XML_FILE);
    return conf;
  }

  public static Configuration addDTSiteResources(Configuration conf)
  {
    addDTLocalResources(conf);
    File targetGlobalFile;
    try (FileSystem fs = newFileSystemInstance(conf)) {
      // after getting the dfsRootDirectory config parameter, redo the entire process with the global config
      // load global settings from DFS
      targetGlobalFile = new File(String.format("%s/dt-site-global-%s.xml", System.getProperty("java.io.tmpdir"),
          UserGroupInformation.getLoginUser().getShortUserName()));
      org.apache.hadoop.fs.Path hdfsGlobalPath = new org.apache.hadoop.fs.Path(StramClientUtils.getDTDFSConfigDir(fs, conf), StramClientUtils.DT_SITE_GLOBAL_XML_FILE);
      LOG.debug("Copying global dt-site.xml from {} to {}", hdfsGlobalPath, targetGlobalFile.getAbsolutePath());
      fs.copyToLocalFile(hdfsGlobalPath, new org.apache.hadoop.fs.Path(targetGlobalFile.toURI()));
      addDTSiteResources(conf, targetGlobalFile);
      if (!isDevelopmentMode()) {
        // load node local config file
        addDTSiteResources(conf, new File(StramClientUtils.getConfigDir(), StramClientUtils.DT_SITE_XML_FILE));
      }
      // load user config file
      addDTSiteResources(conf, new File(StramClientUtils.getUserDTDirectory(), StramClientUtils.DT_SITE_XML_FILE));
    } catch (IOException ex) {
      // ignore
      LOG.debug("Caught exception when loading configuration: {}: moving on...", ex.getMessage());
    } finally {
      // Cannot delete the file here because addDTSiteResource which eventually calls Configuration.reloadConfiguration
      // does not actually reload the configuration.  The file is actually read later and it needs to exist.
      //
      //if (targetGlobalFile != null) {
      //targetGlobalFile.delete();
      //}
    }

    //Validate loggers-level settings
    String loggersLevel = conf.get(StramUtils.DT_LOGGERS_LEVEL);
    if (loggersLevel != null) {
      String[] targets = loggersLevel.split(",");
      Preconditions.checkArgument(targets.length > 0, "zero loggers level");
      for (String target : targets) {
        String[] parts = target.split(":");
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

    long rmConnectMaxWait = conf.getLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS);
    if (rmConnectMaxWait > RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE) {
      LOG.info("Overriding {} assigned value of {} to {} because the assigned value is too big.", YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, rmConnectMaxWait, RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE);
      conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE);
      long rmConnectRetryInterval = conf.getLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS);
      long defaultRetryInterval = Math.max(500, RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE / 5);
      if (rmConnectRetryInterval > defaultRetryInterval) {
        LOG.info("Overriding {} assigned value of {} to {} because the assigned value is too big.", YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, rmConnectRetryInterval, defaultRetryInterval);
        conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, defaultRetryInterval);
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
    } else {
      LOG.info("Configuration file {} is not found. Skipping...", confFile.toURI());
    }
    return conf;
  }

  @SuppressWarnings("deprecation")
  private static void convertDeprecatedProperties(Configuration conf)
  {
    Iterator<Map.Entry<String, String>> iterator = conf.iterator();
    Map<String, String> newEntries = new HashMap<>();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      if (entry.getKey().startsWith("stram.")) {
        String newKey = StreamingApplication.DT_PREFIX + entry.getKey().substring(6);
        LOG.warn("Configuration property {} is deprecated. Please use {} instead.", entry.getKey(), newKey);
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
    } catch (MalformedURLException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static FileSystem newFileSystemInstance(Configuration conf) throws IOException
  {
    String dfsRootDir = conf.get(DT_DFS_ROOT_DIR);
    if (StringUtils.isBlank(dfsRootDir)) {
      return FileSystem.newInstance(conf);
    } else {
      if (dfsRootDir.contains(DT_DFS_USER_NAME)) {
        dfsRootDir = dfsRootDir.replace(DT_DFS_USER_NAME, UserGroupInformation.getLoginUser().getShortUserName());
        conf.set(DT_DFS_ROOT_DIR, dfsRootDir);
      }
      try {
        return FileSystem.newInstance(new URI(dfsRootDir), conf);
      } catch (URISyntaxException ex) {
        LOG.warn("{} is not a valid URI. Returning the default filesystem", dfsRootDir, ex);
        return FileSystem.newInstance(conf);
      }
    }
  }

  /**
   * Helper function used by both getApexDFSRootDir and getDTDFSRootDir to process dfsRootDir
   *
   * @param fs   FileSystem object for HDFS file system
   * @param conf  Configuration object
   * @param dfsRootDir  value of dt.dfsRootDir or apex.app.dfsRootDir
   * @param userShortName  current user short name (either login user or current user depending on impersonation settings)
   * @param prependHomeDir  prepend user's home dir if dfsRootDir is relative path

   * @return
   */
  private static Path evalDFSRootDir(FileSystem fs, Configuration conf, String dfsRootDir, String userShortName,
      boolean prependHomeDir)
  {
    try {
      if (userShortName != null && dfsRootDir.contains(DT_DFS_USER_NAME)) {
        dfsRootDir = dfsRootDir.replace(DT_DFS_USER_NAME, userShortName);
        conf.set(DT_DFS_ROOT_DIR, dfsRootDir);
      }
      URI uri = new URI(dfsRootDir);
      if (uri.isAbsolute()) {
        return new Path(uri);
      }
      if (userShortName != null && prependHomeDir && dfsRootDir.startsWith("/") == false) {
        dfsRootDir = "/user/" + userShortName + "/" + dfsRootDir;
      }
    } catch (URISyntaxException ex) {
      LOG.warn("{} is not a valid URI. Using the default filesystem to construct the path", dfsRootDir, ex);
    }
    return new Path(fs.getUri().getScheme(), fs.getUri().getAuthority(), dfsRootDir);
  }

  private static String getDefaultRootFolder()
  {
    return "datatorrent";
  }

  /**
   * This gets the DFS Root dir to be used at runtime by Apex applications as per the following logic:
   * Value of apex.app.dfsRootDirectory is referred to as Apex-root-dir below.
   * A "user" refers to either impersonating or impersonated user:
   *    If apex.application.path.impersonated is true then use impersonated user else impersonating user.
   *
   * <ul>
   * <li> if Apex-root-dir is blank then just call getDTDFSRootDir to get the old behavior
   * <li> if Apex-root-dir value has %USER_NAME% in the string then replace it with the user's name, else use the absolute path as is.
   * <li> if Apex-root-dir value is a relative path then append it to the user's home directory.
   * </ul>
   *
   * @param fs FileSystem object for HDFS file system
   * @param conf  Configuration object
   * @return
   * @throws IOException
   */
  public static Path getApexDFSRootDir(FileSystem fs, Configuration conf)
  {
    String apexDfsRootDir = conf.get(APEX_APP_DFS_ROOT_DIR);
    boolean useImpersonated = conf.getBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, false);
    String userShortName = null;
    if (useImpersonated) {
      try {
        userShortName = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException ex) {
        LOG.warn("Error getting current/login user name {}", apexDfsRootDir, ex);
      }
    }
    if (!useImpersonated || userShortName == null) {
      return getDTDFSRootDir(fs, conf);
    }
    if (StringUtils.isBlank(apexDfsRootDir)) {
      apexDfsRootDir = getDefaultRootFolder();
    }
    return evalDFSRootDir(fs, conf, apexDfsRootDir, userShortName, true);
  }

  public static Path getDTDFSRootDir(FileSystem fs, Configuration conf)
  {
    String dfsRootDir = conf.get(DT_DFS_ROOT_DIR);
    if (StringUtils.isBlank(dfsRootDir)) {
      return new Path(fs.getHomeDirectory(), getDefaultRootFolder());
    }
    String userShortName = null;
    try {
      userShortName = UserGroupInformation.getLoginUser().getShortUserName();
    } catch (IOException ex) {
      LOG.warn("Error getting user login name {}", dfsRootDir, ex);
    }
    return evalDFSRootDir(fs, conf, dfsRootDir, userShortName, false);
  }

  public static Path getDTDFSConfigDir(FileSystem fs, Configuration conf)
  {
    return new Path(getDTDFSRootDir(fs, conf), SUBDIR_CONF);
  }

  public static Path getDTDFSProfilesDir(FileSystem fs, Configuration conf)
  {
    return new Path(getDTDFSRootDir(fs, conf), SUBDIR_PROFILES);
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
    URL resource = StramClientUtils.class.getClassLoader().getResource(CUSTOM_ENV_SH_FILE);
    if (resource == null) {
      File envFile = new File(StramClientUtils.getUserDTDirectory(), StramClientUtils.CUSTOM_ENV_SH_FILE);
      try (FileOutputStream out = new FileOutputStream(envFile)) {
        out.write(("export " + key + "=\"" + value + "\"\n").getBytes());
      }
    } else {
      try {
        File cfgResource = new File(resource.toURI());
        synchronized (StramClientUtils.class) {
          StringBuilder sb = new StringBuilder(1024);
          try (BufferedReader br = new BufferedReader(new FileReader(cfgResource))) {
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
              } finally {
                sb.append(line).append("\n");
              }
            }
            if (!changed) {
              sb.append("export ").append(key).append("=\"").append(value).append("\"\n");
            }
          }
          if (sb.length() > 0) {
            try (FileOutputStream out = new FileOutputStream(cfgResource)) {
              out.write(sb.toString().getBytes());
            }
          }
        }
      } catch (URISyntaxException ex) {
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
    } catch (Exception ex) {
      // ignore
    }
    fs.copyFromLocalFile(new Path(fromLocal.toURI()), toDFS);
  }

  public static boolean configComplete(Configuration conf)
  {
    String configStatus = conf.get(StramClientUtils.DT_CONFIG_STATUS);
    return "complete".equals(configStatus);
  }

  public static void evalProperties(Properties target, Configuration vars)
  {
    ScriptEngine engine = new ScriptEngineManager().getEngineByName("javascript");

    Pattern substitutionPattern = Pattern.compile("\\$\\{(.+?)\\}");
    Pattern evalPattern = Pattern.compile("\\{% (.+?) %\\}");

    try {
      engine.eval("var _prop = {}");
      for (Map.Entry<String, String> entry : vars) {
        String evalString = String.format("_prop[\"%s\"] = \"%s\"", StringEscapeUtils.escapeJava(entry.getKey()), StringEscapeUtils.escapeJava(entry.getValue()));
        engine.eval(evalString);
      }
    } catch (ScriptException ex) {
      LOG.warn("Javascript error: {}", ex.getMessage());
    }

    for (Map.Entry<Object, Object> entry : target.entrySet()) {
      String value = entry.getValue().toString();

      Matcher matcher = substitutionPattern.matcher(value);
      if (matcher.find()) {
        StringBuilder newValue = new StringBuilder();
        int cursor = 0;
        do {
          newValue.append(value.substring(cursor, matcher.start()));
          String subst = vars.get(matcher.group(1));
          if (subst != null) {
            newValue.append(subst);
          }
          cursor = matcher.end();
        } while (matcher.find());
        newValue.append(value.substring(cursor));
        target.put(entry.getKey(), newValue.toString());
      }

      matcher = evalPattern.matcher(value);
      if (matcher.find()) {
        StringBuilder newValue = new StringBuilder();
        int cursor = 0;
        do {
          newValue.append(value.substring(cursor, matcher.start()));
          try {
            Object result = engine.eval(matcher.group(1));
            String eval = result.toString();

            if (eval != null) {
              newValue.append(eval);
            }
          } catch (ScriptException ex) {
            LOG.warn("JavaScript exception {}", ex.getMessage());
          }
          cursor = matcher.end();
        } while (matcher.find());
        newValue.append(value.substring(cursor));
        target.put(entry.getKey(), newValue.toString());
      }
    }
  }

  public static void evalConfiguration(Configuration conf)
  {
    Properties props = new Properties();
    for (Map.Entry entry : conf) {
      props.put(entry.getKey(), entry.getValue());
    }
    evalProperties(props, conf);
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      conf.set((String)entry.getKey(), (String)entry.getValue());
    }
  }

  public static <T> T doAs(String userName, PrivilegedExceptionAction<T> action) throws Exception
  {
    if (StringUtils.isNotBlank(userName) && !userName.equals(UserGroupInformation.getLoginUser().getShortUserName())) {
      LOG.info("Executing command as {}", userName);
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(userName, UserGroupInformation.getLoginUser());
      return ugi.doAs(action);
    } else {
      LOG.info("Executing command as if there is no login info: {}", userName);
      return action.run();
    }
  }

  public static ApplicationReport getStartedAppInstanceByName(YarnClient clientRMService, String appName, String user, String excludeAppId) throws YarnException, IOException
  {
    List<ApplicationReport> applications = clientRMService.getApplications(Sets.newHashSet(StramClient.YARN_APPLICATION_TYPE, StramClient.YARN_APPLICATION_TYPE_DEPRECATED), EnumSet.of(YarnApplicationState.RUNNING,
        YarnApplicationState.ACCEPTED,
        YarnApplicationState.NEW,
        YarnApplicationState.NEW_SAVING,
        YarnApplicationState.SUBMITTED));
    // see whether there is an app with the app name and user name running
    for (ApplicationReport app : applications) {
      if (!app.getApplicationId().toString().equals(excludeAppId)
          && app.getName().equals(appName)
          && app.getUser().equals(user)) {
        return app;
      }
    }
    return null;
  }

  /**
   * Return a YarnConfiguration instance from a Configuration instance
   * @param conf The configuration instance
   * @return The YarnConfiguration instance
   */
  private static YarnConfiguration getYarnConfiguration(Configuration conf)
  {
    YarnConfiguration yarnConf;
    if (conf instanceof YarnConfiguration) {
      yarnConf = (YarnConfiguration)conf;
    } else {
      yarnConf = new YarnConfiguration(conf);
    }
    return yarnConf;
  }

  public static InetSocketAddress getRMWebAddress(Configuration conf, String rmId)
  {
    boolean sslEnabled = conf.getBoolean(CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_KEY, CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_DEFAULT);
    return getRMWebAddress(conf, sslEnabled, rmId);
  }

  /**
   * Get the RM webapp address. The configuration that is passed in should not be used by other threads while this
   * method is executing.
   * @param conf The configuration
   * @param sslEnabled Whether SSL is enabled or not
   * @param rmId If HA is enabled the resource manager id
   * @return The webapp socket address
   */
  public static InetSocketAddress getRMWebAddress(Configuration conf, boolean sslEnabled, String rmId)
  {
    boolean isHA = (rmId != null);
    if (isHA) {
      conf = getYarnConfiguration(conf);
      conf.set(ConfigUtils.RM_HA_ID, rmId);
    }
    InetSocketAddress address;
    if (sslEnabled) {
      address = conf.getSocketAddr(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS, YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS, YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT);
    } else {
      address = conf.getSocketAddr(YarnConfiguration.RM_WEBAPP_ADDRESS, YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS, YarnConfiguration.DEFAULT_RM_WEBAPP_PORT);
    }
    if (isHA) {
      conf.unset(ConfigUtils.RM_HA_ID);
    }
    LOG.info("rm webapp address setting {}", address);
    LOG.debug("rm setting sources {}", conf.getPropertySources(YarnConfiguration.RM_WEBAPP_ADDRESS));
    InetSocketAddress resolvedSocketAddress = NetUtils.getConnectAddress(address);
    InetAddress resolved = resolvedSocketAddress.getAddress();
    if (resolved == null || resolved.isAnyLocalAddress() || resolved.isLoopbackAddress()) {
      try {
        resolvedSocketAddress = InetSocketAddress.createUnresolved(InetAddress.getLocalHost().getCanonicalHostName(), address.getPort());
      } catch (UnknownHostException e) {
        //Ignore and fallback.
      }
    }
    return resolvedSocketAddress;
  }

  public static String getSocketConnectString(InetSocketAddress socketAddress)
  {
    String host;
    InetAddress address = socketAddress.getAddress();
    if (address == null) {
      host = socketAddress.getHostString();
    } else if (address.isAnyLocalAddress() || address.isLoopbackAddress()) {
      host = address.getCanonicalHostName();
    } else {
      host = address.getHostName();
    }
    return host + ":" + socketAddress.getPort();
  }

  public static List<InetSocketAddress> getRMAddresses(Configuration conf)
  {

    List<InetSocketAddress> rmAddresses = new ArrayList<>();
    if (ConfigUtils.isRMHAEnabled(conf)) {
      // HA is enabled get all
      for (String rmId : ConfigUtils.getRMHAIds(conf)) {
        InetSocketAddress socketAddress = getRMWebAddress(conf, rmId);
        rmAddresses.add(socketAddress);
      }
    } else {
      InetSocketAddress socketAddress = getRMWebAddress(conf, null);
      rmAddresses.add(socketAddress);
    }
    return rmAddresses;
  }

  public static List<ApplicationReport> cleanAppDirectories(YarnClient clientRMService, Configuration conf, FileSystem fs, long finishedBefore)
      throws IOException, YarnException
  {
    List<ApplicationReport> result = new ArrayList<>();
    List<ApplicationReport> applications = clientRMService.getApplications(Sets.newHashSet(StramClient.YARN_APPLICATION_TYPE, StramClient.YARN_APPLICATION_TYPE_DEPRECATED),
        EnumSet.of(YarnApplicationState.FAILED, YarnApplicationState.FINISHED, YarnApplicationState.KILLED));
    Path appsBasePath = new Path(StramClientUtils.getApexDFSRootDir(fs, conf), StramClientUtils.SUBDIR_APPS);
    for (ApplicationReport ar : applications) {
      long finishTime = ar.getFinishTime();
      if (finishTime < finishedBefore) {
        try {
          Path appPath = new Path(appsBasePath, ar.getApplicationId().toString());
          if (fs.isDirectory(appPath)) {
            LOG.debug("Deleting finished application data for {}", ar.getApplicationId());
            fs.delete(appPath, true);
            result.add(ar);
          }
        } catch (Exception ex) {
          LOG.warn("Cannot delete application data for {}", ar.getApplicationId(), ex);
          continue;
        }
      }
    }
    return result;
  }

  public static AppPackage.AppInfo jsonFileToAppInfo(File file, Configuration config)
  {
    AppPackage.AppInfo appInfo = null;

    try {
      StramAppLauncher.AppFactory appFactory = new StramAppLauncher.JsonFileAppFactory(file);
      StramAppLauncher stramAppLauncher = new StramAppLauncher(file.getName(), config);
      stramAppLauncher.loadDependencies();
      appInfo = new AppPackage.AppInfo(appFactory.getName(), file.getName(), "json");
      appInfo.displayName = appFactory.getDisplayName();
      try {
        appInfo.dag = appFactory.createApp(stramAppLauncher.getLogicalPlanConfiguration());
        appInfo.dag.validate();
      } catch (Exception ex) {
        appInfo.error = ex.getMessage();
        appInfo.errorStackTrace = ExceptionUtils.getStackTrace(ex);
      }
    } catch (Exception ex) {
      LOG.error("Caught exceptions trying to process {}", file.getName(), ex);
    }

    return appInfo;
  }

  public static void addAttributeToArgs(Attribute<String> attribute, Context context, List<CharSequence> vargs)
  {
    String value = context.getValue(attribute);
    if (value != null) {
      vargs.add(String.format("-D%s=$'%s'", attribute.getLongName(),
          value.replace("\\", "\\\\\\\\").replaceAll("['\"$]", "\\\\$0")));
    }
  }
}
