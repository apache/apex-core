/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.malhartech.annotation.ShipContainingJars;
import com.malhartech.stram.cli.StramClientUtils.ClientRMHelper;
import com.malhartech.stram.cli.StramClientUtils.YarnClientHelper;
import com.malhartech.stram.conf.Topology;
import com.malhartech.stram.conf.TopologyBuilder;

/**
 *
 * Submits application to YARN<p>
 * <br>
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class StramClient
{
  private static final Logger LOG = LoggerFactory.getLogger(StramClient.class);
  // Configuration
  private final Configuration conf;
  // Handle to talk to the Resource Manager/Applications Manager
  private ClientRMHelper rmClient;
  // Application master specific info to register a new Application with RM/ASM
  // App master priority
  private int amPriority = 0;
  // Queue for App master
  private String amQueue = "default";
  // User to run app master as
  private String amUser = "";
  private ApplicationId appId;
  private Topology topology;
  public String javaCmd = "${JAVA_HOME}" + "/bin/java";
  // log4j.properties file
  // if available, add to local resources and set into classpath
  private String log4jPropFile = "";
  // Timeout threshold for client. Kill app after time interval expires.
  private long clientTimeout = 600000;

  /**
   * @param args Command line arguments
   */
  public static void main(String[] args)
  {
    boolean result = false;
    try {
      StramClient client = new StramClient();
      LOG.info("Initializing StramClient");
      boolean doRun = client.init(args);
      if (!doRun) {
        System.exit(0);
      }
      client.startApplication();
      result = client.monitorApplication();
    }
    catch (Throwable t) {
      LOG.error("Error running CLient", t);
      System.exit(1);
    }
    if (result) {
      LOG.info("Application finished successfully.");
      System.exit(0);
    }
    LOG.error("Application failed!");
    System.exit(2);
  }

  /**
   *
   * @param conf
   * @throws Exception
   */
  public StramClient(Configuration conf) throws Exception
  {
    // Set up the configuration and RPC
    this.conf = conf;
  }


  /**
   *
   * @throws Exception
   */
  public StramClient() throws Exception
  {
    this(new Configuration());
  }

  public StramClient(Topology tplg) throws Exception
  {
    this(new Configuration());
    this.topology = tplg;
    topology.validate();
  }

  /**
   * Helper function to print out usage
   *
   * @param opts Parsed command line options
   */
  private void printUsage(Options opts)
  {
    new HelpFormatter().printHelp("StramClient", opts);
  }

  /**
   * Parse command line options
   *
   * @param args Parsed command line options
   * @return Whether the init was successful to run the client
   */
  public boolean init(String[] args) throws Exception
  {

    Options opts = new Options();
    opts.addOption("appname", true, "Application Name. Default value - Stram");
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
    opts.addOption("user", true, "User to run the application as");
    opts.addOption("timeout", true, "Application timeout in milliseconds");
    opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
    opts.addOption("topologyProperties", true, "Property file defining the topology");
    opts.addOption("container_memory", true, "Amount of memory in MB per child container");
    opts.addOption("num_containers", true, "No. of containers to use for topology");
    opts.addOption("log_properties", true, "log4j.properties file");
    opts.addOption("debug", false, "Dump out debug information");
    opts.addOption("help", false, "Print usage");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      printUsage(opts);
      throw new IllegalArgumentException("No args specified for client to initialize");
    }

    if (cliParser.hasOption("help")) {
      printUsage(opts);
      return false;
    }

    // topology properties
    String propertyFileName = cliParser.getOptionValue("topologyProperties");
    if (propertyFileName == null) {
      throw new IllegalArgumentException("No topology property file specified, exiting.");
    }
    LOG.info("Configuration: " + propertyFileName);

    topology = TopologyBuilder.createTopology(conf, propertyFileName);
    topology.validate();
    if (cliParser.hasOption("debug")) {
      topology.getConf().setBoolean(Topology.STRAM_DEBUG, true);
    }

    amPriority = Integer.parseInt(cliParser.getOptionValue("priority", String.valueOf(amPriority)));
    amQueue = cliParser.getOptionValue("queue", amQueue);
    amUser = cliParser.getOptionValue("user", amUser);
    int amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", ""+topology.getMasterMemoryMB()));

    if (amMemory < 0) {
      throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                                         + " Specified memory=" + amMemory);
    }

    int containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", ""+topology.getContainerMemoryMB()));
    int containerCount = Integer.parseInt(cliParser.getOptionValue("num_containers", ""+ topology.getMaxContainerCount()));

    if (containerMemory < 0 || topology.getMaxContainerCount() < 1) {
      throw new IllegalArgumentException("Invalid no. of containers or container memory specified, exiting."
                                         + " Specified containerMemory=" + containerMemory
                                         + ", numContainer=" + containerCount);
    }

    topology.setMaxContainerCount(containerCount);
    topology.getConf().setInt(Topology.STRAM_MASTER_MEMORY_MB, amMemory);
    topology.getConf().setInt(Topology.STRAM_CONTAINER_MEMORY_MB, containerMemory);

    clientTimeout = Integer.parseInt(cliParser.getOptionValue("timeout", "600000"));
    if (clientTimeout == 0) {
      clientTimeout = Long.MAX_VALUE;
    }

    log4jPropFile = cliParser.getOptionValue("log_properties", "");

    return true;
  }

  public static LinkedHashSet<String> findJars(Topology tplg) {
    // platform jar files - always required
    Class<?>[] defaultClasses = new Class<?>[]{
      com.malhartech.bufferserver.Server.class,
      com.malhartech.stram.StramAppMaster.class,
      com.malhartech.dag.DefaultSerDe.class,
      io.netty.channel.socket.nio.NioEventLoopGroup.class, // there should be another way of handling this!
      io.netty.util.AttributeMap.class,
      io.netty.buffer.ChannelBufType.class,
      io.netty.handler.codec.MessageToMessageEncoder.class
    };
    List<Class<?>> jarClasses = new ArrayList<Class<?>>();
    jarClasses.addAll(Arrays.asList(defaultClasses));

    for (String className : tplg.getClassNames()) {
      try {
        Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
        jarClasses.add(clazz);
      }
      catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Failed to load class " + className, e);
      }
    }

    LinkedHashSet<String> localJarFiles = new LinkedHashSet<String>(); // avoid duplicates

    for (Class<?> jarClass : jarClasses) {
      localJarFiles.add(JarFinder.getJar(jarClass));
      // check for annotated dependencies
      try {
        ShipContainingJars shipJars = jarClass.getAnnotation(ShipContainingJars.class);
        if (shipJars != null) {
          for (Class<?> depClass : shipJars.classes()) {
            localJarFiles.add(JarFinder.getJar(depClass));
            LOG.info("Including {} as dependency of {}", depClass, jarClass);
          }
        }
      }
      catch (ArrayStoreException e) {
        LOG.error("Failed to process ShipContainingJars annotation for class " + jarClass.getName(), e);
      }
    }
    String libJarsPath = tplg.getLibJars();
    if (!StringUtils.isEmpty(libJarsPath)) {
      String[] libJars = StringUtils.splitByWholeSeparator(libJarsPath, ",");
      localJarFiles.addAll(Arrays.asList(libJars));
    }
    LOG.info("Local jar file dependencies: " + localJarFiles);

    return localJarFiles;
  }


  /**
   * Launch application for the topology represented by this client.
   *
   * @throws IOException
   */
  public void startApplication() throws IOException
  {
    // process dependencies
    LinkedHashSet<String> localJarFiles = findJars(topology);

    // Connect to ResourceManager
    YarnClientHelper yarnClient = new YarnClientHelper(conf);
    rmClient = new ClientRMHelper(yarnClient);
    assert(rmClient.clientRM != null);

    // Use ClientRMProtocol handle to general cluster information
    GetClusterMetricsRequest clusterMetricsReq = Records.newRecord(GetClusterMetricsRequest.class);
    GetClusterMetricsResponse clusterMetricsResp = rmClient.clientRM.getClusterMetrics(clusterMetricsReq);
    LOG.info("Got Cluster metric info from ASM"
             + ", numNodeManagers=" + clusterMetricsResp.getClusterMetrics().getNumNodeManagers());

    GetClusterNodesRequest clusterNodesReq = Records.newRecord(GetClusterNodesRequest.class);
    GetClusterNodesResponse clusterNodesResp = rmClient.clientRM.getClusterNodes(clusterNodesReq);
    LOG.info("Got Cluster node info from ASM");
    for (NodeReport node : clusterNodesResp.getNodeReports()) {
      LOG.info("Got node report from ASM for"
               + ", nodeId=" + node.getNodeId()
               + ", nodeAddress" + node.getHttpAddress()
               + ", nodeRackName" + node.getRackName()
               + ", nodeNumContainers" + node.getNumContainers()
               + ", nodeHealthStatus" + node.getNodeHealthStatus());
    }
    /*
     * This is NPE in 2.0-alpha as request needs to provide specific queue name GetQueueInfoRequest queueInfoReq = Records.newRecord(GetQueueInfoRequest.class);
     * GetQueueInfoResponse queueInfoResp = rmClient.getQueueInfo(queueInfoReq); QueueInfo queueInfo = queueInfoResp.getQueueInfo(); LOG.info("Queue
     * info" + ", queueName=" + queueInfo.getQueueName() + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity() + ", queueMaxCapacity=" +
     * queueInfo.getMaximumCapacity() + ", queueApplicationCount=" + queueInfo.getApplications().size() + ", queueChildQueueCount=" +
     * queueInfo.getChildQueues().size());
     */
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

    // TODO get min/max resource capabilities from RM and change memory ask if needed
    // If we do not have min/max, we may not be able to correctly request
    // the required resources from the RM for the app master
    // Memory ask has to be a multiple of min and less than max.
    // Dump out information about cluster capability as seen by the resource manager
    int minMem = newApp.getMinimumResourceCapability().getMemory();
    int maxMem = newApp.getMaximumResourceCapability().getMemory();
    LOG.info("Min mem capabililty of resources in this cluster " + minMem);
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

    // A resource ask has to be atleast the minimum of the capability of the cluster, the value has to be
    // a multiple of the min value and cannot exceed the max.
    // If it is not an exact multiple of min, the RM will allocate to the nearest multiple of min
    int amMemory = topology.getMasterMemoryMB();
    if (amMemory < minMem) {
      LOG.info("AM memory specified below min threshold of cluster. Using min value."
               + ", specified=" + amMemory
               + ", min=" + minMem);
      amMemory = minMem;
    }
    else if (amMemory > maxMem) {
      LOG.info("AM memory specified above max threshold of cluster. Using max value."
               + ", specified=" + amMemory
               + ", max=" + maxMem);
      amMemory = maxMem;
    }

    // Create launch context for app master
    LOG.info("Setting up application submission context for ASM");
    ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);

    // set the application id
    appContext.setApplicationId(appId);
    // set the application name
    appContext.setApplicationName(topology.getConf().get(Topology.STRAM_APPNAME, StramConstants.APPNAME));

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    String pathSuffix = StramConstants.APPNAME + "/" + appId.getId();

    // copy required jar files to dfs, to be localized for containers
    FileSystem fs = FileSystem.get(conf);
    String libJarsCsv = "";
    for (String localJarFile : localJarFiles) {
      Path src = new Path(localJarFile);
      String jarName = src.getName();
      Path dst = new Path(fs.getHomeDirectory(), pathSuffix + "/" + jarName);
      LOG.info("Copy {} from local filesystem to {}", localJarFile, dst);
      fs.copyFromLocalFile(false, true, src, dst);
      if (libJarsCsv.length() > 0) {
        libJarsCsv += ",";
      }
      libJarsCsv += dst.toString();
    }

    LOG.info("libjars: {}", libJarsCsv);
    topology.getConf().set(Topology.STRAM_LIBJARS, libJarsCsv);
    topology.getConf().set(Topology.STRAM_CHECKPOINT_DIR, new Path(fs.getHomeDirectory(), pathSuffix + "/checkpoints").toString());


    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    LaunchContainerRunnable.addLibJarsToLocalResources(libJarsCsv, localResources, fs);

    // Set the log4j properties if needed
    if (!log4jPropFile.isEmpty()) {
      Path log4jSrc = new Path(log4jPropFile);
      Path log4jDst = new Path(fs.getHomeDirectory(), "log4j.props");
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

    // push application configuration to dfs location
    Path cfgDst = new Path(fs.getHomeDirectory(), pathSuffix + "/" + Topology.SER_FILE_NAME);
    FSDataOutputStream outStream = fs.create(cfgDst, true);
    Topology.write(this.topology, outStream);
    outStream.close();

    FileStatus topologyFileStatus = fs.getFileStatus(cfgDst);
    LocalResource topologyRsrc = Records.newRecord(LocalResource.class);
    topologyRsrc.setType(LocalResourceType.FILE);
    topologyRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    topologyRsrc.setResource(ConverterUtils.getYarnUrlFromURI(cfgDst.toUri()));
    topologyRsrc.setTimestamp(topologyFileStatus.getModificationTime());
    topologyRsrc.setSize(topologyFileStatus.getLen());
    localResources.put(Topology.SER_FILE_NAME, topologyRsrc);

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
    StringBuilder classPathEnv = new StringBuilder("${CLASSPATH}:./*");
    for (String c : conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH).split(",")) {
      classPathEnv.append(':');
      classPathEnv.append(c.trim());
    }
    env.put("CLASSPATH", classPathEnv.toString());

    amContainer.setEnvironment(env);

    // Set the necessary command to execute the application master
    Vector<CharSequence> vargs = new Vector<CharSequence>(30);

    // Set java executable command
    LOG.info("Setting up app master command");
    vargs.add(javaCmd);
    if (topology.isDebug()) {
      vargs.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n");
    }
    // Set Xmx based on am memory size
    vargs.add("-Xmx" + amMemory + "m");
    // Set class name
    vargs.add(StramAppMaster.class.getName());

    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

    // Get final commmand
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    LOG.info("Completed setting up app master command " + command.toString());
    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());
    amContainer.setCommands(commands);

    // For launching an AM Container, setting user here is not needed
    // Set user in ApplicationSubmissionContext
    // amContainer.setUser(amUser);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(amMemory);
    amContainer.setResource(capability);

    // Service data is a binary blob that can be passed to the application
    // Not needed in this scenario
    // amContainer.setServiceData(serviceData);

    // The following are not required for launching an application master
    // amContainer.setContainerId(containerId);

    appContext.setAMContainerSpec(amContainer);

    // Set the priority for the application master
    Priority pri = Records.newRecord(Priority.class);
    // TODO - what is the range for priority? how to decide?
    pri.setPriority(amPriority);
    appContext.setPriority(pri);

    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue(amQueue);
    // Set the user submitting this application
    // TODO can it be empty?
    appContext.setUser(amUser);

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
      .add("user", appContext.getUser())
      .add("resource", appContext.getAMContainerSpec().getResource())
      .toString();
    LOG.info(specStr);
    if (topology.isDebug()) {
      LOG.info("Full submission context: " + appContext);
    }
    rmClient.clientRM.submitApplication(appRequest);

  }

  public ApplicationReport getApplicationReport() throws YarnRemoteException
  {
    return this.rmClient.getApplicationReport(this.appId);
  }

  public void killApplication() throws YarnRemoteException {
    this.rmClient.killApplication(this.appId);
  }

  /**
   * Monitor the submitted application for completion. Kill application if time expires.
   *
   * @return true if application completed successfully
   * @throws YarnRemoteException
   */
  public boolean monitorApplication() throws YarnRemoteException
  {
    ClientRMHelper.AppStatusCallback callback = new ClientRMHelper.AppStatusCallback() {
      @Override
      public boolean exitLoop(ApplicationReport report) {
        LOG.info("Got application report from ASM for"
            + ", appId=" + appId.getId()
            + ", clientToken=" + report.getClientToken()
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
  private GetNewApplicationResponse getNewApplication() throws YarnRemoteException
  {
    GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
    GetNewApplicationResponse response = rmClient.clientRM.getNewApplication(request);
    LOG.info("Got new application id=" + response.getApplicationId());
    return response;
  }

}
