
package com.malhartech.stram;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.stram.conf.TopologyBuilder;


/**
 * StramClient for Distributed Shell application submission to YARN.
 * 
 * <p> The distributed shell client allows an application master to be launched that in turn would run 
 * the provided shell command on a set of containers. </p>
 * 
 * <p>This client is meant to act as an example on how to write yarn-based applications. </p>
 * 
 * <p> To submit an application, a client first needs to connect to the <code>ResourceManager</code> 
 * aka ApplicationsManager or ASM via the {@link ClientRMProtocol}. The {@link ClientRMProtocol} 
 * provides a way for the client to get access to cluster information and to request for a
 * new {@link ApplicationId}. <p>
 * 
 * <p> For the actual job submission, the client first has to create an {@link ApplicationSubmissionContext}. 
 * The {@link ApplicationSubmissionContext} defines the application details such as {@link ApplicationId} 
 * and application name, user submitting the application, the priority assigned to the application and the queue 
 * to which this application needs to be assigned. In addition to this, the {@link ApplicationSubmissionContext}
 * also defines the {@link ContainerLaunchContext} which describes the <code>Container</code> with which 
 * the {@link ApplicationMaster} is launched. </p>
 * 
 * <p> The {@link ContainerLaunchContext} in this scenario defines the resources to be allocated for the 
 * {@link ApplicationMaster}'s container, the local resources (jars, configuration files) to be made available 
 * and the environment to be set for the {@link ApplicationMaster} and the commands to be executed to run the 
 * {@link ApplicationMaster}. <p>
 * 
 * <p> Using the {@link ApplicationSubmissionContext}, the client submits the application to the 
 * <code>ResourceManager</code> and then monitors the application by requesting the <code>ResourceManager</code> 
 * for an {@link ApplicationReport} at regular time intervals. In case of the application taking too long, the client 
 * kills the application by submitting a {@link KillApplicationRequest} to the <code>ResourceManager</code>. </p>
 *
 * 
 * Run as:
 * hadoop jar stramproto-0.1-SNAPSHOT.jar com.malhartech.stram.StramClient --jar stramproto-0.1-SNAPSHOT.jar --shell_command date --num_containers 1
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class StramClient {

  private static final Logger LOG = LoggerFactory.getLogger(StramClient.class);

  // Configuration
  private Configuration conf;

  // RPC to communicate to RM
  private YarnRPC rpc;

  // Handle to talk to the Resource Manager/Applications Manager
  private ClientRMProtocol applicationsManager;

  // Application master specific info to register a new Application with RM/ASM
  private String appName = StramConstants.APPNAME;
  // App master priority
  private int amPriority = 0;
  // Queue for App master
  private String amQueue = "";
  // User to run app master as
  private String amUser = "";
  // Amt. of memory resource to request for to run the App Master
  private int amMemory = 10; 

  // Main class to invoke application master
  private String appMasterMainClass = "";

  private String topologyPropertyFile;

  // Amt of memory to request for container in which shell script will be executed
  private int containerMemory = 10; 
  // No. of containers in which the shell script needs to be executed
  private int numContainers = 1;

  public String javaCmd = "${JAVA_HOME}" + "/bin/java";
  
  // log4j.properties file 
  // if available, add to local resources and set into classpath 
  private String log4jPropFile = "";	

  // Start time for client
  private final long clientStartTime = System.currentTimeMillis();
  // Timeout threshold for client. Kill app after time interval expires.
  private long clientTimeout = 600000;

  // Debug flag
  boolean debugFlag = false;	

  /**
   * @param args Command line arguments 
   */
  public static void main(String[] args) {
    boolean result = false;
    try {
      StramClient client = new StramClient();
      LOG.info("Initializing StramClient");
      boolean doRun = client.init(args);
      if (!doRun) {
        System.exit(0);
      }
      result = client.run();
    } catch (Throwable t) {
      LOG.error("Error running CLient", t);
      System.exit(1);
    }
    if (result) {
      LOG.info("Application completed successfully");
      System.exit(0);			
    } 
    LOG.error("Application failed to complete successfully");
    System.exit(2);
  }

  /**
   */
  public StramClient(Configuration conf) throws Exception  {
    // Set up the configuration and RPC
    this.conf = conf;
    rpc = YarnRPC.create(conf);
  }

  /**
   */
  public StramClient() throws Exception  {
    this(new Configuration());
  }

  /**
   * Helper function to print out usage
   * @param opts Parsed command line options 
   */
  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("StramClient", opts);
  }

  /**
   * Parse command line options
   * @param args Parsed command line options 
   * @return Whether the init was successful to run the client
   */
  public boolean init(String[] args) throws ParseException {

    Options opts = new Options();
    opts.addOption("appname", true, "Application Name. Default value - Stram");
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
    opts.addOption("user", true, "User to run the application as");
    opts.addOption("timeout", true, "Application timeout in milliseconds");
    opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
    opts.addOption("topologyProperties", true, "File defining the topology");
    opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command");
    opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed");
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

    if (cliParser.hasOption("debug")) {
      debugFlag = true;

    }

    amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
    amQueue = cliParser.getOptionValue("queue", "default");
    amUser = cliParser.getOptionValue("user", "");
    amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "10"));		

    if (amMemory < 0) {
      throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
          + " Specified memory=" + amMemory);
    }

    appMasterMainClass = StramAppMaster.class.getName();		
    topologyPropertyFile = cliParser.getOptionValue("topologyProperties");
    if (topologyPropertyFile == null) {
      throw new IllegalArgumentException("No topology property file specified, exiting.");
    }
    
    containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
    numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));

    if (containerMemory < 0 || numContainers < 1) {
      throw new IllegalArgumentException("Invalid no. of containers or container memory specified, exiting."
          + " Specified containerMemory=" + containerMemory
          + ", numContainer=" + numContainers);
    }

    clientTimeout = Integer.parseInt(cliParser.getOptionValue("timeout", "600000"));

    log4jPropFile = cliParser.getOptionValue("log_properties", "");

    return true;
  }

  /**
   * Main run function for the client
   * @return true if application completed successfully
   * @throws IOException
   */
  public boolean run() throws IOException {
    LOG.info("Starting StramClient");

    // Connect to ResourceManager 	
    connectToASM();
    assert(applicationsManager != null);		

    // Use ClientRMProtocol handle to general cluster information 
    GetClusterMetricsRequest clusterMetricsReq = Records.newRecord(GetClusterMetricsRequest.class);
    GetClusterMetricsResponse clusterMetricsResp = applicationsManager.getClusterMetrics(clusterMetricsReq);
    LOG.info("Got Cluster metric info from ASM" 
        + ", numNodeManagers=" + clusterMetricsResp.getClusterMetrics().getNumNodeManagers());

    GetClusterNodesRequest clusterNodesReq = Records.newRecord(GetClusterNodesRequest.class);
    GetClusterNodesResponse clusterNodesResp = applicationsManager.getClusterNodes(clusterNodesReq);
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
 This is NPE in 2.0-alpha as request needs to provide specific queue name
    GetQueueInfoRequest queueInfoReq = Records.newRecord(GetQueueInfoRequest.class);
    GetQueueInfoResponse queueInfoResp = applicationsManager.getQueueInfo(queueInfoReq);		
    QueueInfo queueInfo = queueInfoResp.getQueueInfo();
    LOG.info("Queue info"
        + ", queueName=" + queueInfo.getQueueName()
        + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
        + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
        + ", queueApplicationCount=" + queueInfo.getApplications().size()
        + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());		
*/
    GetQueueUserAclsInfoRequest queueUserAclsReq = Records.newRecord(GetQueueUserAclsInfoRequest.class);
    GetQueueUserAclsInfoResponse queueUserAclsResp = applicationsManager.getQueueUserAcls(queueUserAclsReq);				
    List<QueueUserACLInfo> listAclInfo = queueUserAclsResp.getUserAclsInfoList();
    for (QueueUserACLInfo aclInfo : listAclInfo) {
      for (QueueACL userAcl : aclInfo.getUserAcls()) {
        LOG.info("User ACL Info for Queue"
            + ", queueName=" + aclInfo.getQueueName()			
            + ", userAcl=" + userAcl.name());
      }
    }		

    // Get a new application id 
    GetNewApplicationResponse newApp = getApplication();
    ApplicationId appId = newApp.getApplicationId();

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
    appContext.setApplicationName(appName);

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
    
    // platform jar files
    Class<?>[] jarClasses = new Class<?>[] {
        com.malhartech.bufferserver.Server.class,
        com.malhartech.stram.StramAppMaster.class,
        //com.malhartech.bufferserver.Server.class
    };

    FileSystem fs = FileSystem.get(conf);
    List<String> localJarFiles = new ArrayList<String>();

    for (Class<?> jarClass : jarClasses) {
      localJarFiles.add(JarFinder.getJar(jarClass));
    }
    
    // topology properties
    Properties topologyProperties = StramAppMaster.readProperties(topologyPropertyFile);
    if (topologyProperties.getProperty(TopologyBuilder.LIBJARS) != null) {
      String[] libJars = StringUtils.splitByWholeSeparator(topologyProperties.getProperty(TopologyBuilder.LIBJARS), ",");
      localJarFiles.addAll(Arrays.asList(libJars));
    }
    
    // copy required jar files to dfs, to be localized for containers
    String libJarsCsv = "";
    for (String localJarFile : localJarFiles) {
      Path src = new Path(localJarFile);
      String jarName = src.getName();
      String pathSuffix = appName + "/" + appId.getId() + "/" + jarName;	    
      Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
      LOG.info("Copy {} from local filesystem to {}", localJarFile, dst);
      fs.copyFromLocalFile(false, true, src, dst);
      if (libJarsCsv.length() > 0) {
        libJarsCsv += ",";
      }
      libJarsCsv += dst.toString();
    }
    
    LOG.info("libjars: {}", libJarsCsv);
    topologyProperties.put(TopologyBuilder.LIBJARS, libJarsCsv);
    
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
    
    // write the topology properties to the dfs location
    Path topologyDst = new Path(fs.getHomeDirectory(), appName + "/" + appId.getId() + "/stram.properties");
    FSDataOutputStream outStream = fs.create(topologyDst, true);
    topologyProperties.store(outStream, "topology for " + appId.getId());
    outStream.close();
    
    FileStatus topologyFileStatus = fs.getFileStatus(topologyDst);
    LocalResource topologyRsrc = Records.newRecord(LocalResource.class);
    topologyRsrc.setType(LocalResourceType.FILE);
    topologyRsrc.setVisibility(LocalResourceVisibility.APPLICATION);    
    topologyRsrc.setResource(ConverterUtils.getYarnUrlFromURI(topologyDst.toUri()));
    topologyRsrc.setTimestamp(topologyFileStatus.getModificationTime());
    topologyRsrc.setSize(topologyFileStatus.getLen());
    localResources.put("stram.properties", topologyRsrc);
    
    // Set local resource info into app master container launch context
    amContainer.setLocalResources(localResources);

    // Set the necessary security tokens as needed
    //amContainer.setContainerTokens(containerToken);

    // Set the env variables to be setup in the env where the application master will be run
    LOG.info("Set the environment for the application master");
    Map<String, String> env = new HashMap<String, String>();

    // Add AppMaster.jar location to classpath 		
    // At some point we should not be required to add 
    // the hadoop specific classpaths to the env. 
    // It should be provided out of the box. 
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv = new StringBuilder("${CLASSPATH}:./*");
    for (String c : conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH)
        .split(",")) {
      classPathEnv.append(':');
      classPathEnv.append(c.trim());
    }
    classPathEnv.append(":./log4j.properties");

    env.put("CLASSPATH", classPathEnv.toString());
    
    amContainer.setEnvironment(env);

    // Set the necessary command to execute the application master 
    Vector<CharSequence> vargs = new Vector<CharSequence>(30);

    // Set java executable command 
    LOG.info("Setting up app master command");
    vargs.add(javaCmd);
    // Set Xmx based on am memory size
    vargs.add("-Xmx" + amMemory + "m");
    // Set class name 
    vargs.add(appMasterMainClass);
    // Set params for Application Master
    vargs.add("--container_memory " + String.valueOf(containerMemory));
    vargs.add("--num_containers " + String.valueOf(numContainers));
    if (debugFlag) {
      vargs.add("--debug");
    }

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
    // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
    // Ignore the response as either a valid response object is returned on success 
    // or an exception thrown to denote some form of a failure
    LOG.info("Submitting application to ASM");
    applicationsManager.submitApplication(appRequest);

    // TODO
    // Try submitting the same request again
    // app submission failure?

    // Monitor the application
    return monitorApplication(appId);

  }

  /**
   * Monitor the submitted application for completion. 
   * Kill application if time expires. 
   * @param appId Application Id of application to be monitored
   * @return true if application completed successfully
   * @throws YarnRemoteException
   */
  private boolean monitorApplication(ApplicationId appId) throws YarnRemoteException {

    while (true) {

      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in 
      GetApplicationReportRequest reportRequest = Records.newRecord(GetApplicationReportRequest.class);
      reportRequest.setApplicationId(appId);
      GetApplicationReportResponse reportResponse = applicationsManager.getApplicationReport(reportRequest);
      ApplicationReport report = reportResponse.getApplicationReport();

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

      if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
        LOG.info("Reached client specified timeout for application. Killing application");
        killApplication(appId);
        return false;				
      }
    }			

  }

  /**
   * Kill a submitted application by sending a call to the ASM
   * @param appId Application Id to be killed. 
   * @throws YarnRemoteException
   */
  private void killApplication(ApplicationId appId) throws YarnRemoteException {
    KillApplicationRequest request = Records.newRecord(KillApplicationRequest.class);		
    // TODO clarify whether multiple jobs with the same app id can be submitted and be running at 
    // the same time. 
    // If yes, can we kill a particular attempt only?
    request.setApplicationId(appId);
    // KillApplicationResponse response = applicationsManager.forceKillApplication(request);		
    // Response can be ignored as it is non-null on success or 
    // throws an exception in case of failures
    applicationsManager.forceKillApplication(request);	
  }

  /**
   * Connect to the Resource Manager/Applications Manager
   * @return Handle to communicate with the ASM
   * @throws IOException 
   */
  private void connectToASM() throws IOException {

    /*
		UserGroupInformation user = UserGroupInformation.getCurrentUser();
		applicationsManager = user.doAs(new PrivilegedAction<ClientRMProtocol>() {
			public ClientRMProtocol run() {
				InetSocketAddress rmAddress = NetUtils.createSocketAddr(conf.get(
					YarnConfiguration.RM_SCHEDULER_ADDRESS,
					YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));		
				LOG.info("Connecting to ResourceManager at " + rmAddress);
				Configuration appsManagerServerConf = new Configuration(conf);
				appsManagerServerConf.setClass(YarnConfiguration.YARN_SECURITY_INFO,
				ClientRMSecurityInfo.class, SecurityInfo.class);
				ClientRMProtocol asm = ((ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class, rmAddress, appsManagerServerConf));
				return asm;
			}
		});
     */
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    InetSocketAddress rmAddress = yarnConf.getSocketAddr(
        YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_PORT);
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    applicationsManager = ((ClientRMProtocol) rpc.getProxy(
        ClientRMProtocol.class, rmAddress, conf));
  }		

  /**
   * Get a new application from the ASM 
   * @return New Application
   * @throws YarnRemoteException
   */
  private GetNewApplicationResponse getApplication() throws YarnRemoteException {
    GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);		
    GetNewApplicationResponse response = applicationsManager.getNewApplication(request);
    LOG.info("Got new application id=" + response.getApplicationId());		
    return response;		
  }


}
