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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.common.util.JarHelper;
import org.apache.apex.engine.security.ACLManager;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.SSLConfig;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.common.util.BasicContainerOptConfigurator;
import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.client.StramClientUtils.ClientRMHelper;
import com.datatorrent.stram.engine.StreamingContainer;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * Submits application to YARN<p>
 * <br>
 *
 * @since 0.3.2
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class StramClient
{
  private static final Logger LOG = LoggerFactory.getLogger(StramClient.class);
  public static final String YARN_APPLICATION_TYPE = "ApacheApex";
  @Deprecated
  public static final String YARN_APPLICATION_TYPE_DEPRECATED = "DataTorrent";

  private static final String javaCmd = "${JAVA_HOME}" + "/bin/java";

  public static final String LIB_JARS_SEP = ",";

  // Configuration
  private final Configuration conf;
  // Handle to talk to the Resource Manager/Applications Manager
  private final YarnClient yarnClient = YarnClient.createYarnClient();
  // Application master specific info to register a new Application with RM/ASM
  // App master priority
  private final int amPriority = 0;
  private ApplicationId appId;
  private final LogicalPlan dag;
  // log4j.properties file
  // if available, add to local resources and set into classpath
  private final String log4jPropFile = "";
  // Timeout threshold for client. Kill app after time interval expires.
  private long clientTimeout = 600000;
  private String originalAppId;
  private String queueName;
  private String applicationType = YARN_APPLICATION_TYPE;
  private String archives;
  private String files;
  private LinkedHashSet<String> resources;
  private Set<String> tags = new HashSet<>();

  // platform dependencies that are not part of Hadoop and need to be deployed,
  // entry below will cause containing jar file from client to be copied to cluster
  private static final Class<?>[] APEX_CLASSES = new Class<?>[]{
      com.datatorrent.netlet.util.Slice.class,
      com.datatorrent.netlet.EventLoop.class,
      com.datatorrent.bufferserver.server.Server.class,
      com.datatorrent.stram.StreamingAppMaster.class,
      com.datatorrent.api.StreamCodec.class,
      com.datatorrent.common.util.FSStorageAgent.class,
      javax.validation.ConstraintViolationException.class,
      com.esotericsoftware.kryo.Kryo.class,
      org.apache.bval.jsr303.ApacheValidationProvider.class,
      org.apache.bval.BeanValidationContext.class,
      org.apache.commons.lang3.ClassUtils.class,
      net.engio.mbassy.bus.MBassador.class,
      org.apache.apex.shaded.ning19.com.ning.http.client.ws.WebSocketUpgradeHandler.class,
      org.codehaus.jackson.annotate.JsonUnwrapped.class,
      org.codehaus.jackson.map.ser.std.RawSerializer.class,
      org.apache.commons.beanutils.BeanUtils.class,
      org.apache.http.client.utils.URLEncodedUtils.class,
      org.apache.http.message.BasicHeaderValueParser.class,
      com.esotericsoftware.minlog.Log.class,
      org.apache.xbean.asm5.tree.ClassNode.class,
      org.jctools.queues.SpscArrayQueue.class
  };

  private static final Class<?>[] APEX_SECURITY_SPECIFIC_CLASSES = new Class<?>[]{
      com.sun.jersey.client.apache4.ApacheHttpClient4Handler.class
  };

  private static final Class<?>[] APEX_SECURITY_CLASSES =
      (Class<?>[])ArrayUtils.addAll(APEX_CLASSES, APEX_SECURITY_SPECIFIC_CLASSES);

  public StramClient(Configuration conf, LogicalPlan dag) throws Exception
  {
    this.conf = conf;
    this.dag = dag;
    dag.validate();
    yarnClient.init(conf);
  }

  public void start()
  {
    yarnClient.start();
  }

  public void stop()
  {
    yarnClient.stop();
  }

  public LinkedHashSet<String> findJars(Class<?>[] defaultClasses)
  {
    List<Class<?>> jarClasses = new ArrayList<>();

    for (String className : dag.getClassNames()) {
      try {
        Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
        jarClasses.add(clazz);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Failed to load class " + className, e);
      }
    }

    for (Class<?> clazz : Lists.newArrayList(jarClasses)) {
      // process class and super classes (super does not require deploy annotation)
      for (Class<?> c = clazz; c != Object.class && c != null; c = c.getSuperclass()) {
        //LOG.debug("checking " + c);
        jarClasses.add(c);
        jarClasses.addAll(Arrays.asList(c.getInterfaces()));
      }
    }

    jarClasses.addAll(Arrays.asList(defaultClasses));

    if (dag.isDebug()) {
      LOG.debug("Deploy dependencies: {}", jarClasses);
    }

    LinkedHashSet<String> localJarFiles = new LinkedHashSet<>(); // avoid duplicates
    JarHelper jarHelper = new JarHelper();

    for (Class<?> jarClass : jarClasses) {
      localJarFiles.addAll(jarHelper.getJars(jarClass));
    }

    String libJarsPath = dag.getValue(Context.DAGContext.LIBRARY_JARS);
    if (!StringUtils.isEmpty(libJarsPath)) {
      String[] libJars = StringUtils.splitByWholeSeparator(libJarsPath, LIB_JARS_SEP);
      localJarFiles.addAll(Arrays.asList(libJars));
    }

    String pluginClassesPaths = conf.get(StreamingAppMasterService.PLUGINS_CONF_KEY);
    if (!StringUtils.isEmpty(pluginClassesPaths)) {
      for (String pluginClassPath : StringUtils.splitByWholeSeparator(pluginClassesPaths, StreamingAppMasterService.PLUGINS_CONF_SEP)) {
        try {
          localJarFiles.addAll(jarHelper.getJars(Thread.currentThread().getContextClassLoader().loadClass(pluginClassPath)));
        } catch (ClassNotFoundException ex) {
          LOG.error("Cannot find the class {}", pluginClassPath, ex);
        }
      }
    }

    LOG.info("Local jar file dependencies: {}", localJarFiles);

    return localJarFiles;
  }

  private String copyFromLocal(FileSystem fs, Path basePath, String[] files) throws IOException
  {
    StringBuilder csv = new StringBuilder(files.length * (basePath.toString().length() + 16));
    for (String localFile : files) {
      Path src = new Path(localFile);
      String filename = src.getName();
      Path dst = new Path(basePath, filename);
      URI localFileURI = null;
      try {
        localFileURI = new URI(localFile);
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
      if (localFileURI.getScheme() == null || localFileURI.getScheme().startsWith("file")) {
        LOG.info("Copy {} from local filesystem to {}", localFile, dst);
        fs.copyFromLocalFile(false, true, src, dst);
      } else {
        LOG.info("Copy {} from DFS to {}", localFile, dst);
        FileUtil.copy(fs, src, fs, dst, false, true, conf);
      }
      if (csv.length() > 0) {
        csv.append(LIB_JARS_SEP);
      }
      csv.append(dst.toString());
    }
    return csv.toString();
  }

  public void copyInitialState(Path origAppDir) throws IOException
  {
    // locate previous snapshot
    long copyStart = System.currentTimeMillis();
    String newAppDir = this.dag.assertAppPath();

    FSRecoveryHandler recoveryHandler = new FSRecoveryHandler(origAppDir.toString(), conf);
    // read snapshot against new dependencies
    Object snapshot = recoveryHandler.restore();
    if (snapshot == null) {
      throw new IllegalArgumentException("No previous application state found in " + origAppDir);
    }
    InputStream logIs = recoveryHandler.getLog();

    // modify snapshot state to switch app id
    ((StreamingContainerManager.CheckpointState)snapshot).setApplicationId(this.dag, conf);
    Path checkpointPath = new Path(newAppDir, LogicalPlan.SUBDIR_CHECKPOINTS);

    FileSystem fs = FileSystem.newInstance(origAppDir.toUri(), conf);
    // remove the path that was created by the storage agent during deserialization and replacement
    fs.delete(checkpointPath, true);

    // write snapshot to new location
    recoveryHandler = new FSRecoveryHandler(newAppDir, conf);
    recoveryHandler.save(snapshot);
    OutputStream logOs = recoveryHandler.rotateLog();
    IOUtils.copy(logIs, logOs);
    logOs.flush();
    logOs.close();
    logIs.close();

    List<String> excludeDirs = Arrays.asList(LogicalPlan.SUBDIR_CHECKPOINTS, LogicalPlan.SUBDIR_EVENTS, LogicalPlan.SUBDIR_STATS);
    // copy sub directories that are not present in target
    FileStatus[] lFiles = fs.listStatus(origAppDir);

    // In case of MapR/MapR-FS, f.getPath().toString() returns path as maprfs:///<orig app dir>
    // whereas origAppDir.toString & newAppDir are in maprfs:/<orig or new app dir> format
    // e.g.
    // f.getPath().toString -> maprfs:///user/dtadmin/datatorrent/apps/application_1481890072066_0004/checkpoints
    // origAppDir -> maprfs:/user/dtadmin/datatorrent/apps/application_1481890072066_0004
    // newAppDir -> maprfs:/user/dtadmin/datatorrent/apps/application_1481890072066_0005

    String origAppDirPath = Path.getPathWithoutSchemeAndAuthority(origAppDir).toString();
    String newAppDirPath = Path.getPathWithoutSchemeAndAuthority(new Path(newAppDir)).toString();

    for (FileStatus f : lFiles) {
      if (f.isDirectory() && !excludeDirs.contains(f.getPath().getName())) {
        String targetPath = f.getPath().toString().replace(origAppDirPath, newAppDirPath);
        if (!fs.exists(new Path(targetPath))) {
          LOG.debug("Copying {} size {} to {}", f.getPath(), f.getLen(), targetPath);
          long start = System.currentTimeMillis();
          FileUtil.copy(fs, f.getPath(), fs, new Path(targetPath), false, conf);
          LOG.debug("Copying {} to {} took {} ms", f.getPath(), f.getLen(), targetPath, System.currentTimeMillis() - start);
        } else {
          LOG.debug("Ignoring {} as it already exists under {}", f.getPath(), targetPath);
        }
      }
    }
    LOG.info("Copying initial state took {} ms", System.currentTimeMillis() - copyStart);
  }

  /**
   * Launch application for the dag represented by this client.
   *
   * @throws YarnException
   * @throws IOException
   */
  public void startApplication() throws YarnException, IOException
  {
    Class<?>[] defaultClasses;

    if (applicationType.equals(YARN_APPLICATION_TYPE)) {
      //TODO restrict the security check to only check if security is enabled for webservices.
      if (UserGroupInformation.isSecurityEnabled()) {
        defaultClasses = APEX_SECURITY_CLASSES;
      } else {
        defaultClasses = APEX_CLASSES;
      }
    } else {
      throw new IllegalStateException(applicationType + " is not a valid application type.");
    }

    LinkedHashSet<String> localJarFiles = findJars(defaultClasses);

    if (resources != null) {
      localJarFiles.addAll(resources);
    }

    YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
    LOG.info("Got Cluster metric info from ASM, numNodeManagers={}", clusterMetrics.getNumNodeManagers());

    //GetClusterNodesRequest clusterNodesReq = Records.newRecord(GetClusterNodesRequest.class);
    //GetClusterNodesResponse clusterNodesResp = rmClient.clientRM.getClusterNodes(clusterNodesReq);
    //LOG.info("Got Cluster node info from ASM");
    //for (NodeReport node : clusterNodesResp.getNodeReports()) {
    //  LOG.info("Got node report from ASM for"
    //           + ", nodeId=" + node.getNodeId()
    //           + ", nodeAddress" + node.getHttpAddress()
    //           + ", nodeRackName" + node.getRackName()
    //           + ", nodeNumContainers" + node.getNumContainers()
    //           + ", nodeHealthStatus" + node.getHealthReport());
    //}
    List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
    for (QueueUserACLInfo aclInfo : listAclInfo) {
      for (QueueACL userAcl : aclInfo.getUserAcls()) {
        LOG.info("User ACL Info for Queue queueName={}, userAcl={}", aclInfo.getQueueName(), userAcl.name());
      }
    }

    // Get a new application id
    YarnClientApplication newApp = yarnClient.createApplication();
    appId = newApp.getNewApplicationResponse().getApplicationId();

    // Dump out information about cluster capability as seen by the resource manager
    int maxMem = newApp.getNewApplicationResponse().getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capability of resources in this cluster " + maxMem);
    int amMemory = dag.getMasterMemoryMB();
    if (amMemory > maxMem) {
      LOG.info("AM memory specified above max threshold of cluster. Using max value, specified={}, max={}",
          amMemory, maxMem);
      amMemory = maxMem;
    }

    if (dag.getAttributes().get(LogicalPlan.APPLICATION_ID) == null) {
      dag.setAttribute(LogicalPlan.APPLICATION_ID, appId.toString());
    }

    // Create launch context for app master
    LOG.info("Setting up application submission context for ASM");
    ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);

    // set the application id
    appContext.setApplicationId(appId);
    // set the application name
    appContext.setApplicationName(dag.getValue(LogicalPlan.APPLICATION_NAME));
    appContext.setApplicationType(this.applicationType);
    if (YARN_APPLICATION_TYPE.equals(this.applicationType)) {
      //appContext.setMaxAppAttempts(1); // no retries until Stram is HA
    }

    appContext.setKeepContainersAcrossApplicationAttempts(true);

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    // Setup security tokens
    // If security is enabled get ResourceManager and NameNode delegation tokens.
    // Set these tokens on the container so that they are sent as part of application submission.
    // This also sets them up for renewal by ResourceManager. The NameNode delegation rmToken
    // is also used by ResourceManager to fetch the jars from HDFS and set them up for the
    // application master launch.
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials = new Credentials();
      String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
      if (tokenRenewer == null || tokenRenewer.length() == 0) {
        throw new IOException(
          "Can't get Master Kerberos principal for the RM to use as renewer");
      }

      // For now, only getting tokens for the default file-system.
      try (FileSystem fs = StramClientUtils.newFileSystemInstance(conf)) {
        final Token<?>[] tokens = fs.addDelegationTokens(tokenRenewer, credentials);
        if (tokens != null) {
          for (Token<?> token : tokens) {
            LOG.info("Got dt for " + fs.getUri() + "; " + token);
          }
        }
      }

      new ClientRMHelper(yarnClient, conf).addRMDelegationToken(tokenRenewer, credentials);

      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      amContainer.setTokens(fsTokens);
    }

    // Setup ACLs for the impersonating user
    LOG.debug("ACL login user {} current user {}", UserGroupInformation.getLoginUser(), UserGroupInformation.getCurrentUser());
    if (!UserGroupInformation.getCurrentUser().equals(UserGroupInformation.getLoginUser())) {
      ACLManager.setupUserACLs(amContainer, UserGroupInformation.getLoginUser().getShortUserName(), conf);
    }

    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the local resources
    Map<String, LocalResource> localResources = new HashMap<>();

    // copy required jar files to dfs, to be localized for containers
    try (FileSystem fs = StramClientUtils.newFileSystemInstance(conf)) {
      Path appsBasePath = new Path(StramClientUtils.getApexDFSRootDir(fs, conf), StramClientUtils.SUBDIR_APPS);
      Path appPath;
      String configuredAppPath = dag.getValue(LogicalPlan.APPLICATION_PATH);
      if (configuredAppPath == null) {
        appPath = new Path(appsBasePath, appId.toString());
      } else {
        appPath = new Path(configuredAppPath);
      }
      String libJarsCsv = copyFromLocal(fs, appPath, localJarFiles.toArray(new String[]{}));
      setupSSLResources(dag.getValue(Context.DAGContext.SSL_CONFIG), fs, appPath, localResources);

      LOG.info("libjars: {}", libJarsCsv);
      dag.getAttributes().put(Context.DAGContext.LIBRARY_JARS, libJarsCsv);
      LaunchContainerRunnable.addFilesToLocalResources(LocalResourceType.FILE, libJarsCsv, localResources, fs);

      if (archives != null) {
        String[] localFiles = archives.split(",");
        String archivesCsv = copyFromLocal(fs, appPath, localFiles);
        LOG.info("archives: {}", archivesCsv);
        dag.getAttributes().put(LogicalPlan.ARCHIVES, archivesCsv);
        LaunchContainerRunnable.addFilesToLocalResources(LocalResourceType.ARCHIVE, archivesCsv, localResources, fs);
      }

      if (files != null) {
        String[] localFiles = files.split(",");
        String filesCsv = copyFromLocal(fs, appPath, localFiles);
        LOG.info("files: {}", filesCsv);
        dag.getAttributes().put(LogicalPlan.FILES, filesCsv);
        LaunchContainerRunnable.addFilesToLocalResources(LocalResourceType.FILE, filesCsv, localResources, fs);
      }

      dag.getAttributes().put(LogicalPlan.APPLICATION_PATH, appPath.toString());
      StorageAgent agent = dag.getAttributes().get(OperatorContext.STORAGE_AGENT);
      if (agent != null && agent instanceof StorageAgent.ApplicationAwareStorageAgent) {
        ((StorageAgent.ApplicationAwareStorageAgent)agent).setApplicationAttributes(dag.getAttributes());
      }

      if (dag.getAttributes().get(OperatorContext.STORAGE_AGENT) == null) { /* which would be the most likely case */
        Path checkpointPath = new Path(appPath, LogicalPlan.SUBDIR_CHECKPOINTS);
        // use conf client side to pickup any proxy settings from dt-site.xml
        dag.setAttribute(OperatorContext.STORAGE_AGENT, new AsyncFSStorageAgent(checkpointPath.toString(), conf));
      }

      if (dag.getAttributes().get(LogicalPlan.CONTAINER_OPTS_CONFIGURATOR) == null) {
        dag.setAttribute(LogicalPlan.CONTAINER_OPTS_CONFIGURATOR, new BasicContainerOptConfigurator());
      }

      // Set the log4j properties if needed
      if (!log4jPropFile.isEmpty()) {
        Path log4jSrc = new Path(log4jPropFile);
        Path log4jDst = new Path(appPath, "log4j.props");
        fs.copyFromLocalFile(false, true, log4jSrc, log4jDst);
        FileStatus log4jFileStatus = fs.getFileStatus(log4jDst);
        LocalResource log4jRsrc = Records.newRecord(LocalResource.class);
        log4jRsrc.setType(LocalResourceType.FILE);
        log4jRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
        log4jRsrc.setResource(ConverterUtils.getYarnUrlFromURI(log4jDst.toUri()));
        log4jRsrc.setTimestamp(log4jFileStatus.getModificationTime());
        log4jRsrc.setSize(log4jFileStatus.getLen());
        localResources.put("log4j.properties", log4jRsrc);
      }

      if (originalAppId != null) {
        Path origAppPath = new Path(appsBasePath, this.originalAppId);
        LOG.info("Restart from {}", origAppPath);
        copyInitialState(origAppPath);
      }

      // push logical plan to DFS location
      Path cfgDst = new Path(appPath, LogicalPlan.SER_FILE_NAME);
      FSDataOutputStream outStream = fs.create(cfgDst, true);
      LogicalPlan.write(this.dag, outStream);
      outStream.close();

      Path launchConfigDst = new Path(appPath, LogicalPlan.LAUNCH_CONFIG_FILE_NAME);
      outStream = fs.create(launchConfigDst, true);
      conf.writeXml(outStream);
      outStream.close();
      LaunchContainerRunnable.addFileToLocalResources(LogicalPlan.SER_FILE_NAME, fs.getFileStatus(cfgDst), LocalResourceType.FILE, localResources);

      // Set local resource info into app master container launch context
      amContainer.setLocalResources(localResources);

      // Set the necessary security tokens as needed
      //amContainer.setContainerTokens(containerToken);
      // Set the env variables to be setup in the env where the application master will be run
      LOG.info("Set the environment for the application master");
      Map<String, String> env = new HashMap<>();

      // Add application jar(s) location to classpath
      // At some point we should not be required to add
      // the hadoop specific classpaths to the env.
      // It should be provided out of the box.
      // For now setting all required classpaths including
      // the classpath to "." for the application jar(s)
      // including ${CLASSPATH} will duplicate the class path in app master, removing it for now
      //StringBuilder classPathEnv = new StringBuilder("${CLASSPATH}:./*");
      StringBuilder classPathEnv = new StringBuilder("./*");
      String classpath = conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH);
      for (String c : StringUtils.isBlank(classpath) ? YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH : classpath.split(",")) {
        if (c.equals("$HADOOP_CLIENT_CONF_DIR")) {
          // SPOI-2501
          continue;
        }
        classPathEnv.append(':');
        classPathEnv.append(c.trim());
      }
      env.put("CLASSPATH", classPathEnv.toString());
      // propagate to replace node managers user name (effective in non-secure mode)
      // also to indicate original login user during impersonation and important for setting ACLs
      env.put("HADOOP_USER_NAME", UserGroupInformation.getLoginUser().getUserName());

      amContainer.setEnvironment(env);

      // Set the necessary command to execute the application master
      ArrayList<CharSequence> vargs = new ArrayList<>(30);

      // Set java executable command
      LOG.info("Setting up app master command");
      vargs.add(javaCmd);
      if (dag.isDebug()) {
        vargs.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n");
      }
      // Set Xmx based on am memory size
      // default heap size 75% of total memory
      if (dag.getMasterJVMOptions() != null) {
        vargs.add(dag.getMasterJVMOptions());
      }
      Path tmpDir = new Path(ApplicationConstants.Environment.PWD.$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
      vargs.add("-Djava.io.tmpdir=" + tmpDir);
      vargs.add("-Xmx" + (amMemory * 3 / 4) + "m");
      vargs.add("-XX:+HeapDumpOnOutOfMemoryError");
      vargs.add("-XX:HeapDumpPath=" + System.getProperty("java.io.tmpdir") + "/dt-heap-" + appId.getId() + ".bin");
      vargs.add("-Dhadoop.root.logger=" + (dag.isDebug() ? "DEBUG" : "INFO") + ",RFA");
      vargs.add("-Dhadoop.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
      vargs.add(String.format("-D%s=%s", StreamingContainer.PROP_APP_PATH, dag.assertAppPath()));
      StramClientUtils.addAttributeToArgs(LogicalPlan.APPLICATION_NAME, dag, vargs);
      StramClientUtils.addAttributeToArgs(LogicalPlan.LOGGER_APPENDER, dag, vargs);
      if (dag.isDebug()) {
        vargs.add("-Dlog4j.debug=true");
      }

      String loggersLevel = conf.get(StramUtils.DT_LOGGERS_LEVEL);
      if (loggersLevel != null) {
        vargs.add(String.format("-D%s=%s", StramUtils.DT_LOGGERS_LEVEL, loggersLevel));
      }
      vargs.add(StreamingAppMaster.class.getName());
      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

      // Get final command
      StringBuilder command = new StringBuilder(9 * vargs.size());
      for (CharSequence str : vargs) {
        command.append(str).append(" ");
      }

      LOG.info("Completed setting up app master command " + command.toString());
      List<String> commands = new ArrayList<>();
      commands.add(command.toString());
      amContainer.setCommands(commands);

      // Set up resource type requirements
      // For now, only memory is supported so we set memory requirements
      Resource capability = Records.newRecord(Resource.class);
      capability.setMemory(amMemory);
      appContext.setResource(capability);

      // Service data is a binary blob that can be passed to the application
      // Not needed in this scenario
      // amContainer.setServiceData(serviceData);
      appContext.setAMContainerSpec(amContainer);

      // Set the priority for the application master
      Priority pri = Records.newRecord(Priority.class);
      pri.setPriority(amPriority);
      appContext.setPriority(pri);
      // Set the queue to which this application is to be submitted in the RM
      appContext.setQueue(queueName);

      // set the application tags
      appContext.setApplicationTags(tags);

      // Submit the application to the applications manager
      // SubmitApplicationResponse submitResp = rmClient.submitApplication(appRequest);
      // Ignore the response as either a valid response object is returned on success
      // or an exception thrown to denote some form of a failure
      String specStr = Objects.toStringHelper("Submitting application: ")
          .add("name", appContext.getApplicationName())
          .add("queue", appContext.getQueue())
          .add("user", UserGroupInformation.getLoginUser())
          .add("resource", appContext.getResource())
          .toString();
      LOG.info(specStr);
      if (dag.isDebug()) {
        //LOG.info("Full submission context: " + appContext);
      }
      yarnClient.submitApplication(appContext);
    }
  }

  /**
   * Process SSLConfig object to set up SSL resources
   *
   * @param sslConfig  SSLConfig object derived from SSL_CONFIG attribute
   * @param fs    HDFS file system object
   * @param appPath    application path for the current application
   * @param localResources  Local resources to modify
   * @throws IOException
   */
  private void setupSSLResources(SSLConfig sslConfig, FileSystem fs, Path appPath, Map<String, LocalResource> localResources) throws IOException
  {
    if (sslConfig != null) {
      String nodeLocalConfig = sslConfig.getConfigPath();

      if (StringUtils.isNotEmpty(nodeLocalConfig)) {
        // all others should be empty
        if (StringUtils.isNotEmpty(sslConfig.getKeyStorePath()) || StringUtils.isNotEmpty(sslConfig.getKeyStorePassword())
            || StringUtils.isNotEmpty(sslConfig.getKeyStoreKeyPassword())) {
          throw new IllegalArgumentException("Cannot specify both nodeLocalConfigPath and other parameters in " + sslConfig);
        }
        // pass thru: Stram will implement reading the node local SSL config file
      } else {
        // need to package and copy the keyStore file
        String keystorePath = sslConfig.getKeyStorePath();
        String[] sslFileArray = {keystorePath};
        String sslFileNames = copyFromLocal(fs, appPath, sslFileArray);
        LaunchContainerRunnable.addFilesToLocalResources(LocalResourceType.FILE, sslFileNames, localResources, fs);
      }
    }
  }

  public ApplicationReport getApplicationReport() throws YarnException, IOException
  {
    return yarnClient.getApplicationReport(this.appId);
  }

  public void killApplication() throws YarnException, IOException
  {
    yarnClient.killApplication(this.appId);
  }

  public void setClientTimeout(long timeoutMillis)
  {
    this.clientTimeout = timeoutMillis;
  }

  /**
   * Monitor the submitted application for completion. Kill application if time expires.
   *
   * @return true if application completed successfully
   * @throws YarnException
   * @throws IOException
   */
  public boolean monitorApplication() throws YarnException, IOException
  {
    ClientRMHelper.AppStatusCallback callback = new ClientRMHelper.AppStatusCallback()
    {
      @Override
      public boolean exitLoop(ApplicationReport report)
      {
        LOG.info("Got application report from ASM for, appId={}, clientToken={}, appDiagnostics={}, appMasterHost={}," +
            "appQueue={}, appMasterRpcPort={}, appStartTime={}, yarnAppState={}, distributedFinalState={}, " +
            "appTrackingUrl={}, appUser={}",
            appId.getId(), report.getClientToAMToken(), report.getDiagnostics(), report.getHost(),
            report.getQueue(), report.getRpcPort(), report.getStartTime(), report.getYarnApplicationState(),
            report.getFinalApplicationStatus(), report.getTrackingUrl(), report.getUser());
        return false;
      }

    };
    ClientRMHelper rmClient = new ClientRMHelper(yarnClient, conf);
    return rmClient.waitForCompletion(appId, callback, clientTimeout);
  }

  public void setApplicationType(String type)
  {
    this.applicationType = type;
  }

  public void setOriginalAppId(String appId)
  {
    this.originalAppId = appId;
  }

  public String getQueueName()
  {
    return queueName;
  }

  public void setQueueName(String queueName)
  {
    this.queueName = queueName;
  }

  public void addTag(String tag)
  {
    this.tags.add(tag);
  }

  public void setResources(LinkedHashSet<String> resources)
  {
    this.resources = resources;
  }

  public void setArchives(String archives)
  {
    this.archives = archives;
  }

  public void setFiles(String files)
  {
    this.files = files;
  }
}
