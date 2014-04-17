/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ShipContainingJars;

import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.client.StramClientUtils.ClientRMHelper;
import com.datatorrent.stram.client.StramClientUtils.YarnClientHelper;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

/**
 *
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
  public static final String YARN_APPLICATION_TYPE = "DataTorrent";
  public static final String YARN_APPLICATION_TYPE_LICENSE = "DataTorrentLicense";
  // Configuration
  private final Configuration conf;
  // Handle to talk to the Resource Manager/Applications Manager
  private ClientRMHelper rmClient;
  // Application master specific info to register a new Application with RM/ASM
  // App master priority
  private int amPriority = 0;
  // Queue for App master
  private String amQueue = "default";
  private ApplicationId appId;
  private LogicalPlan dag;
  public String javaCmd = "${JAVA_HOME}" + "/bin/java";
  // log4j.properties file
  // if available, add to local resources and set into classpath
  private String log4jPropFile = "";
  // Timeout threshold for client. Kill app after time interval expires.
  private long clientTimeout = 600000;
  private String libjars;
  private String files;
  private String archives;
  private String originalAppId;
  private String applicationType = YARN_APPLICATION_TYPE;

  /**
   *
   * @param conf
   * @throws Exception
   */
  StramClient(Configuration conf) throws Exception
  {
    this.conf = conf;
  }

  public StramClient(Configuration conf, LogicalPlan dag) throws Exception
  {
    this(conf);
    this.dag = dag;
    dag.validate();
  }

  /**
   * Parse command line options
   *
   * @param args Parsed command line options
   * @return Whether the init was successful to run the client
   * @throws Exception
   */
  boolean init(String[] args) throws Exception
  {

    Options opts = new Options();
    opts.addOption("appname", true, "Application Name. Default value - Stram");
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
    opts.addOption("user", true, "User to run the application as");
    opts.addOption("timeout", true, "Application timeout in milliseconds");
    opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
    opts.addOption("topologyProperties", true, "Property file defining the dag");
    opts.addOption("container_memory", true, "Amount of memory in MB per child container");
    opts.addOption("num_containers", true, "No. of containers to use for dag");
    opts.addOption("log_properties", true, "log4j.properties file");
    opts.addOption("debug", false, "Dump out debug information");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      throw new IllegalArgumentException("No args specified for client to initialize");
    }

    // dag properties
    String propertyFileName = cliParser.getOptionValue("topologyProperties");
    if (propertyFileName == null) {
      throw new IllegalArgumentException("No dag property file specified, exiting.");
    }
    LOG.info("Configuration: " + propertyFileName);

    dag = new LogicalPlan();
    Configuration appConf = new Configuration(false);
    StreamingApplication app = LogicalPlanConfiguration.create(appConf, propertyFileName);
    app.populateDAG(dag, appConf);
    dag.validate();
    if (cliParser.hasOption("debug")) {
      dag.getAttributes().put(LogicalPlan.DEBUG, true);
    }

    amPriority = Integer.parseInt(cliParser.getOptionValue("priority", String.valueOf(amPriority)));
    amQueue = cliParser.getOptionValue("queue", amQueue);
    int amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "" + dag.getMasterMemoryMB()));

    if (amMemory < 0) {
      throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                                         + " Specified memory=" + amMemory);
    }

    int containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "" + dag.getContainerMemoryMB()));
    int containerCount = Integer.parseInt(cliParser.getOptionValue("num_containers", "" + dag.getMaxContainerCount()));

    if (containerMemory < 0 || dag.getMaxContainerCount() < 1) {
      throw new IllegalArgumentException("Invalid no. of containers or container memory specified, exiting."
                                         + " Specified containerMemory=" + containerMemory
                                         + ", numContainer=" + containerCount);
    }

    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, containerCount);
    dag.getAttributes().put(LogicalPlan.MASTER_MEMORY_MB, amMemory);
    dag.getAttributes().put(LogicalPlan.CONTAINER_MEMORY_MB, containerMemory);

    clientTimeout = Integer.parseInt(cliParser.getOptionValue("timeout", "600000"));
    if (clientTimeout == 0) {
      clientTimeout = Long.MAX_VALUE;
    }

    log4jPropFile = cliParser.getOptionValue("log_properties", "");

    return true;
  }

  public static LinkedHashSet<String> findJars(LogicalPlan dag)
  {
    // platform dependencies that are not part of Hadoop and need to be deployed,
    // entry below will cause containing jar file from client to be copied to cluster
    Class<?>[] defaultClasses = new Class<?>[] {
      com.datatorrent.common.util.Slice.class,
      com.datatorrent.netlet.EventLoop.class,
      com.datatorrent.bufferserver.server.Server.class,
      com.datatorrent.stram.StreamingAppMaster.class,
      com.datatorrent.api.StreamCodec.class,
      javax.validation.ConstraintViolationException.class,
      com.ning.http.client.websocket.WebSocketUpgradeHandler.class,
      com.esotericsoftware.kryo.Kryo.class,
      org.apache.bval.jsr303.ApacheValidationProvider.class,
      org.apache.bval.BeanValidationContext.class,
      org.apache.commons.lang3.ClassUtils.class,
      net.engio.mbassy.bus.MBassador.class,
      org.codehaus.jackson.annotate.JsonUnwrapped.class,
      org.codehaus.jackson.map.ser.std.RawSerializer.class,
      org.apache.commons.beanutils.BeanUtils.class
    };
    List<Class<?>> jarClasses = new ArrayList<Class<?>>();

    for (String className : dag.getClassNames()) {
      try {
        Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
        jarClasses.add(clazz);
      }
      catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Failed to load class " + className, e);
      }
    }

    for (Class<?> clazz : Lists.newArrayList(jarClasses)) {
      // process class and super classes (super does not require deploy annotation)
      for (Class<?> c = clazz; c != Object.class && c != null; c = c.getSuperclass()) {
        //LOG.debug("checking " + c);
        jarClasses.add(c);
        jarClasses.addAll(Arrays.asList(c.getInterfaces()));
        // check for annotated dependencies
        try {
          ShipContainingJars shipJars = c.getAnnotation(ShipContainingJars.class);
          if (shipJars != null) {
            for (Class<?> depClass : shipJars.classes()) {
              jarClasses.add(depClass);
              LOG.info("Including {} as deploy dependency of {}", depClass, c);
              jarClasses.addAll(Arrays.asList(depClass.getInterfaces()));
            }
          }
        }
        catch (ArrayStoreException e) {
          LOG.error("Failed to process ShipContainingJars annotation for class " + c.getName(), e);
        }
      }
    }

    jarClasses.addAll(Arrays.asList(defaultClasses));

    if (dag.isDebug()) {
      LOG.debug("Deploy dependencies: {}", jarClasses);
    }

    LinkedHashSet<String> localJarFiles = new LinkedHashSet<String>(); // avoid duplicates
    HashMap<String, String> sourceToJar = new HashMap<String, String>();

    for (Class<?> jarClass : jarClasses) {
      if (jarClass.getProtectionDomain().getCodeSource() == null) {
        // system class
        continue;
      }
      String sourceLocation = jarClass.getProtectionDomain().getCodeSource().getLocation().toString();
      String jar = sourceToJar.get(sourceLocation);
      if (jar == null) {
        // don't create jar file from folders multiple times
        jar = JarFinder.getJar(jarClass);
        sourceToJar.put(sourceLocation, jar);
        LOG.debug("added sourceLocation {} as {}", sourceLocation, jar);
      }
      if (jar == null) {
        throw new AssertionError("Cannot resolve jar file for " + jarClass);
      }
      localJarFiles.add(jar);
    }

    String libJarsPath = dag.getValue(LogicalPlan.LIBRARY_JARS);
    if (!StringUtils.isEmpty(libJarsPath)) {
      String[] libJars = StringUtils.splitByWholeSeparator(libJarsPath, ",");
      localJarFiles.addAll(Arrays.asList(libJars));
    }
    LOG.info("Local jar file dependencies: " + localJarFiles);

    return localJarFiles;
  }

  private String copyFromLocal(FileSystem fs, Path basePath, String[] files) throws IOException
  {
    StringBuilder csv = new StringBuilder(files.length * (basePath.toString().length() + 16));
    for (String localFile : files) {
      Path src = new Path(localFile);
      String filename = src.getName();
      Path dst = new Path(basePath, filename);
      if (localFile.startsWith("hdfs:")) {
        LOG.info("Copy {} from HDFS to {}", localFile, dst);
        FileUtil.copy(fs, src, fs, dst, false, true, conf);
      }
      else {
        LOG.info("Copy {} from local filesystem to {}", localFile, dst);
        fs.copyFromLocalFile(false, true, src, dst);
      }
      if (csv.length() > 0) {
        csv.append(",");
      }
      csv.append(dst.toString());
    }
    return csv.toString();
  }

  public void copyInitialState(Path origAppDir) throws IOException
  {
    // locate previous snapshot
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

    // write snapshot to new location
    recoveryHandler = new FSRecoveryHandler(newAppDir, conf);
    recoveryHandler.save(snapshot);
    OutputStream logOs = recoveryHandler.rotateLog();
    IOUtils.copy(logIs, logOs);
    logOs.flush();
    logOs.close();
    logIs.close();

    // copy sub directories that are not present in target
    FileSystem fs = FileSystem.newInstance(origAppDir.toUri(), conf);
    FileStatus[] files = fs.listStatus(origAppDir);
    for (FileStatus f : files) {
      if (f.isDirectory()) {
        String targetPath = f.getPath().toString().replace(origAppDir.toString(), newAppDir);
        if (!fs.exists(new Path(targetPath))) {
          LOG.debug("Copying {} to {}", f.getPath(), targetPath);
          FileUtil.copy(fs, f.getPath(), fs, new Path(targetPath), false, conf);
        } else {
          LOG.debug("Ignoring {} as it already exists under {}", f.getPath(), targetPath);
        }
      }
    }

  }

  /**
   * Launch application for the dag represented by this client.
   *
   * @throws YarnException
   * @throws IOException
   */
  public void startApplication() throws YarnException, IOException
  {
    // process dependencies
    LinkedHashSet<String> localJarFiles = findJars(dag);

    if (libjars != null) {
      localJarFiles.addAll(Arrays.asList(libjars.split(",")));
    }

    // Connect to ResourceManager
    YarnClientHelper yarnClient = new YarnClientHelper(conf);
    rmClient = new ClientRMHelper(yarnClient);
    assert (rmClient.clientRM != null);

    // Use ClientRMProtocol handle to general cluster information
    GetClusterMetricsRequest clusterMetricsReq = Records.newRecord(GetClusterMetricsRequest.class);
    GetClusterMetricsResponse clusterMetricsResp = rmClient.clientRM.getClusterMetrics(clusterMetricsReq);
    LOG.info("Got Cluster metric info from ASM"
             + ", numNodeManagers=" + clusterMetricsResp.getClusterMetrics().getNumNodeManagers());

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
    GetQueueUserAclsInfoRequest queueUserAclsReq = Records.newRecord(GetQueueUserAclsInfoRequest.class);
    GetQueueUserAclsInfoResponse queueUserAclsResp = rmClient.clientRM.getQueueUserAcls(queueUserAclsReq);
    List<QueueUserACLInfo> listAclInfo = queueUserAclsResp.getUserAclsInfoList();
    for (QueueUserACLInfo aclInfo : listAclInfo) {
      for (QueueACL userAcl : aclInfo.getUserAcls()) {
        LOG.info("User ACL Info for Queue"
                 + ", queueName=" + aclInfo.getQueueName()
                 + ", userAcl=" + userAcl.name());
      }
    }

    // Get a new application id
    GetNewApplicationResponse newApp = getNewApplication();
    appId = newApp.getApplicationId();

    // Dump out information about cluster capability as seen by the resource manager
    int maxMem = newApp.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
    int amMemory = dag.getMasterMemoryMB();
    if (amMemory > maxMem) {
      LOG.info("AM memory specified above max threshold of cluster. Using max value."
               + ", specified=" + amMemory
               + ", max=" + maxMem);
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
    else if (YARN_APPLICATION_TYPE_LICENSE.equals(this.applicationType)) {
      LOG.debug("Attempts capped at {} ({})", conf.get(YarnConfiguration.RM_AM_MAX_ATTEMPTS), YarnConfiguration.RM_AM_MAX_ATTEMPTS);
    }

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
      FileSystem fs = StramClientUtils.newFileSystemInstance(conf);
      try {
        final Token<?> tokens[] = fs.addDelegationTokens(tokenRenewer, credentials);
        if (tokens != null) {
          for (Token<?> token : tokens) {
            LOG.info("Got dt for " + fs.getUri() + "; " + token);
          }
        }
      }
      finally {
        fs.close();
      }

      InetSocketAddress rmAddress = conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
                                                       YarnConfiguration.DEFAULT_RM_ADDRESS,
                                                       YarnConfiguration.DEFAULT_RM_PORT);

      // Get the ResourceManager delegation rmToken
      GetDelegationTokenRequest gdtr = Records.newRecord(GetDelegationTokenRequest.class);
      gdtr.setRenewer(tokenRenewer);
      GetDelegationTokenResponse gdresp = rmClient.clientRM.getDelegationToken(gdtr);
      org.apache.hadoop.yarn.api.records.Token rmDelToken = gdresp.getRMDelegationToken();
      Token<RMDelegationTokenIdentifier> rmToken = ConverterUtils.convertFromYarn(rmDelToken, rmAddress);
      credentials.addToken(rmToken.getService(), rmToken);

      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      amContainer.setTokens(fsTokens);
    }


    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    // copy required jar files to dfs, to be localized for containers
    FileSystem fs = StramClientUtils.newFileSystemInstance(conf);
    try {
      Path appsBasePath = new Path(StramClientUtils.getDTRootDir(fs, conf), StramClientUtils.SUBDIR_APPS);
      Path appPath = new Path(appsBasePath, appId.toString());

      String libJarsCsv = copyFromLocal(fs, appPath, localJarFiles.toArray(new String[] {}));

      LOG.info("libjars: {}", libJarsCsv);
      dag.getAttributes().put(LogicalPlan.LIBRARY_JARS, libJarsCsv);
      LaunchContainerRunnable.addFilesToLocalResources(LocalResourceType.FILE, libJarsCsv, localResources, fs);

      if (files != null) {
        String[] localFiles = files.split(",");
        String filesCsv = copyFromLocal(fs, appPath, localFiles);
        LOG.info("files: {}", filesCsv);
        dag.getAttributes().put(LogicalPlan.FILES, filesCsv);
        LaunchContainerRunnable.addFilesToLocalResources(LocalResourceType.FILE, filesCsv, localResources, fs);
      }

      if (archives != null) {
        String[] localFiles = archives.split(",");
        String archivesCsv = copyFromLocal(fs, appPath, localFiles);
        LOG.info("archives: {}", archivesCsv);
        dag.getAttributes().put(LogicalPlan.ARCHIVES, archivesCsv);
        LaunchContainerRunnable.addFilesToLocalResources(LocalResourceType.ARCHIVE, archivesCsv, localResources, fs);
      }

      dag.getAttributes().put(LogicalPlan.APPLICATION_PATH, appPath.toString());
      if (dag.getAttributes().get(OperatorContext.STORAGE_AGENT) == null) { /* which would be the most likely case */
        Path checkpointPath = new Path(appPath, LogicalPlan.SUBDIR_CHECKPOINTS);
        // use conf client side to pickup any proxy settings from dt-site.xml
        dag.setAttribute(OperatorContext.STORAGE_AGENT, new FSStorageAgent(checkpointPath.toString(), conf));
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

      FileStatus topologyFileStatus = fs.getFileStatus(cfgDst);
      LocalResource topologyRsrc = Records.newRecord(LocalResource.class);
      topologyRsrc.setType(LocalResourceType.FILE);
      topologyRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
      topologyRsrc.setResource(ConverterUtils.getYarnUrlFromURI(cfgDst.toUri()));
      topologyRsrc.setTimestamp(topologyFileStatus.getModificationTime());
      topologyRsrc.setSize(topologyFileStatus.getLen());
      localResources.put(LogicalPlan.SER_FILE_NAME, topologyRsrc);

      // Set local resource info into app master container launch context
      amContainer.setLocalResources(localResources);

      // Set the necessary security tokens as needed
      //amContainer.setContainerTokens(containerToken);
      // Set the env variables to be setup in the env where the application master will be run
      LOG.info("Set the environment for the application master");
      Map<String, String> env = new HashMap<String, String>();

      // Add application jar(s) location to classpath
      // At some point we should not be required to add
      // the hadoop specific classpaths to the env.
      // It should be provided out of the box.
      // For now setting all required classpaths including
      // the classpath to "." for the application jar(s)
      // including ${CLASSPATH} will duplicate the class path in app master, removing it for now
      //StringBuilder classPathEnv = new StringBuilder("${CLASSPATH}:./*");
      StringBuilder classPathEnv = new StringBuilder("./*");
      for (String c : conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH).split(",")) {
        classPathEnv.append(':');
        classPathEnv.append(c.trim());
      }
      env.put("CLASSPATH", classPathEnv.toString());

      amContainer.setEnvironment(env);

      // Set the necessary command to execute the application master
      ArrayList<CharSequence> vargs = new ArrayList<CharSequence>(30);

      // Set java executable command
      LOG.info("Setting up app master command");
      vargs.add(javaCmd);
      if (dag.isDebug()) {
        vargs.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n");
      }
      // Set Xmx based on am memory size
      // default heap size 75% of total memory
      vargs.add("-Xmx" + (amMemory * 3 / 4) + "m");
      vargs.add("-XX:+HeapDumpOnOutOfMemoryError");
      vargs.add("-XX:HeapDumpPath=/tmp/dt-heap-" + appId.getId() + ".bin");
      vargs.add("-Dhadoop.root.logger=" + (dag.isDebug() ? "DEBUG" : "INFO") + ",RFA");
      vargs.add("-Dhadoop.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR);

      if (YARN_APPLICATION_TYPE_LICENSE.equals(applicationType)) {
        vargs.add(LicensingAppMaster.class.getName());
      }
      else {
        vargs.add(StreamingAppMaster.class.getName());
      }

      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

      // Get final command
      StringBuilder command = new StringBuilder(9 * vargs.size());
      for (CharSequence str : vargs) {
        command.append(str).append(" ");
      }

      LOG.info("Completed setting up app master command " + command.toString());
      List<String> commands = new ArrayList<String>();
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
      appContext.setQueue(amQueue);

      // Create the request to send to the applications manager
      SubmitApplicationRequest appRequest = Records.newRecord(SubmitApplicationRequest.class);
      appRequest.setApplicationSubmissionContext(appContext);

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
      rmClient.clientRM.submitApplication(appRequest);
    }
    finally {
      fs.close();
    }
  }

  public ApplicationReport getApplicationReport() throws YarnException, IOException
  {
    return this.rmClient.getApplicationReport(this.appId);
  }

  public void killApplication() throws YarnException, IOException
  {
    this.rmClient.killApplication(this.appId);
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
        LOG.info("Got application report from ASM for"
                 + ", appId=" + appId.getId()
                 + ", clientToken=" + report.getClientToAMToken()
                 + ", appDiagnostics=" + report.getDiagnostics()
                 + ", appMasterHost=" + report.getHost()
                 + ", appQueue=" + report.getQueue()
                 + ", appMasterRpcPort=" + report.getRpcPort()
                 + ", appStartTime=" + report.getStartTime()
                 + ", yarnAppState=" + report.getYarnApplicationState().toString()
                 + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
                 + ", appTrackingUrl=" + report.getTrackingUrl()
                 + ", appUser=" + report.getUser());
        return false;
      }

    };
    return rmClient.waitForCompletion(appId, callback, clientTimeout);
  }

  /**
   * Get a new application from the ASM
   *
   * @return New Application
   * @throws YarnRemoteException
   */
  private GetNewApplicationResponse getNewApplication() throws YarnException, IOException
  {
    GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
    GetNewApplicationResponse response = rmClient.clientRM.getNewApplication(request);
    LOG.info("Got new application id=" + response.getApplicationId());
    return response;
  }

  public void setFiles(String files)
  {
    this.files = files;
  }

  public void setLibJars(String libjars)
  {
    this.libjars = libjars;
  }

  public void setArchives(String archives)
  {
    this.archives = archives;
  }

  public void setApplicationType(String type)
  {
    this.applicationType = type;
  }

  public void setOriginalAppId(String appId)
  {
    this.originalAppId = appId;
  }

}
