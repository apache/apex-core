package com.malhar.app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * see {@link org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster}  
 *
 */
public class StramAppMaster {

  private static Logger LOG = LoggerFactory.getLogger(StramAppMaster.class);
  boolean runShell = false;	  

	  // Configuration 
	  private Configuration conf;
	  // YARN RPC to communicate with the Resource Manager or Node Manager
	  private YarnRPC rpc;

	  // Handle to communicate with the Resource Manager
	  private AMRMProtocol resourceManager;

	  // Application Attempt Id ( combination of attemptId and fail count )
	  private ApplicationAttemptId appAttemptID;

	  // TODO
	  // For status update for clients - yet to be implemented
	  // Hostname of the container 
	  private String appMasterHostname = "";
	  // Port on which the app master listens for status update requests from clients
	  private int appMasterRpcPort = 0;
	  // Tracking url to which app master publishes info for clients to monitor 
	  private String appMasterTrackingUrl = "";

	  // App Master configuration
	  // No. of containers to run shell command on
	  private int numTotalContainers = 1;
	  // Memory to request for the container on which the shell command will run 
	  private int containerMemory = 10;
	  // Priority of the request
	  private int requestPriority; 

	  // Incremental counter for rpc calls to the RM
	  private AtomicInteger rmRequestID = new AtomicInteger();

	  // Simple flag to denote whether all works is done
	  private boolean appDone = false; 
	  // Counter for completed containers ( complete denotes successful or failed )
	  private AtomicInteger numCompletedContainers = new AtomicInteger();
	  // Allocated container count so that we know how many containers has the RM
	  // allocated to us
	  private AtomicInteger numAllocatedContainers = new AtomicInteger();
	  // Count of failed containers 
	  private AtomicInteger numFailedContainers = new AtomicInteger();
	  // Count of containers already requested from the RM
	  // Needed as once requested, we should not request for containers again and again. 
	  // Only request for more if the original requirement changes. 
	  private AtomicInteger numRequestedContainers = new AtomicInteger();

	  // Shell command to be executed 
	  private String shellCommand = ""; 
	  // Args to be passed to the shell command
	  private String shellArgs = "";
	  // Env variables to be setup for the shell command 
	  private Map<String, String> shellEnv = new HashMap<String, String>();

	  // Location of shell script ( obtained from info set in env )
	  // Shell script path in fs
	  private String shellScriptPath = ""; 
	  // Timestamp needed for creating a local resource
	  private long shellScriptPathTimestamp = 0;
	  // File length needed for local resource
	  private long shellScriptPathLen = 0;

	  // Hardcoded path to shell script in launch container's local env
	  private final String ExecShellStringPath = "ExecShellScript.sh";

	  // Containers to be released
	  private CopyOnWriteArrayList<ContainerId> releasedContainers = new CopyOnWriteArrayList<ContainerId>();

	  // Launch threads
	  private List<Thread> launchThreads = new ArrayList<Thread>();

	  // child container callback
	  private StreamingNodeParent rpcImpl;
	  
	  
	  /**
	   * @param args Command line args
	   */
	  public static void main(String[] args) {
	    boolean result = false;
	    try {
	      StramAppMaster appMaster = new StramAppMaster();
	      LOG.info("Initializing ApplicationMaster");
	      boolean doRun = appMaster.init(args);
	      if (!doRun) {
	        System.exit(0);
	      }
	      result = appMaster.run();
	    } catch (Throwable t) {
	      LOG.error("Error running ApplicationMaster", t);
	      System.exit(1);
	    }
	    if (result) {
	      LOG.info("Application Master completed successfully. exiting");
	      System.exit(0);
	    }
	    else {
	      LOG.info("Application Master failed. exiting");
	      System.exit(2);
	    }
	  }

	  /**
	   * Dump out contents of $CWD and the environment to stdout for debugging
	   */
	  private void dumpOutDebugInfo() {

	    LOG.info("Dump debug output");
	    Map<String, String> envs = System.getenv();
	    for (Map.Entry<String, String> env : envs.entrySet()) {
	      LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
	      System.out.println("System env: key=" + env.getKey() + ", val=" + env.getValue());
	    }

	    String cmd = "ls -al";
	    Runtime run = Runtime.getRuntime();
	    Process pr = null;
	    try {
	      pr = run.exec(cmd);
	      pr.waitFor();

	      BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
	      String line = "";
	      while ((line=buf.readLine())!=null) {
	        LOG.info("System CWD content: " + line);
	        System.out.println("System CWD content: " + line);
	      }
	      buf.close();
	    } catch (IOException e) {
	      e.printStackTrace();
	    } catch (InterruptedException e) {
	      e.printStackTrace();
	    } 
	  }

	  public StramAppMaster() throws Exception {
	    // Set up the configuration and RPC
	    conf = new Configuration();
	    rpc = YarnRPC.create(conf);
	  }
	  /**
	   * Parse command line options
	   * @param args Command line args 
	   * @return Whether init successful and run should be invoked 
	   * @throws ParseException
	   * @throws IOException 
	   */
	  public boolean init(String[] args) throws ParseException, IOException {

	    Options opts = new Options();
	    opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes");
	    opts.addOption("shell_command", true, "Shell command to be executed by the Application Master");
	    opts.addOption("shell_script", true, "Location of the shell script to be executed");
	    opts.addOption("shell_args", true, "Command line args for the shell script");
	    opts.addOption("shell_env", true, "Environment for shell script. Specified as env_key=env_val pairs");
	    opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command");
	    opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed");
	    opts.addOption("priority", true, "Application Priority. Default 0");
	    opts.addOption("debug", false, "Dump out debug information");

	    opts.addOption("help", false, "Print usage");
	    CommandLine cliParser = new GnuParser().parse(opts, args);

	    if (args.length == 0) {
	      printUsage(opts);
	      throw new IllegalArgumentException("No args specified for application master to initialize");
	    }

	    if (cliParser.hasOption("help")) {
	      printUsage(opts);
	      return false;
	    }

	    if (cliParser.hasOption("debug")) {
	      dumpOutDebugInfo();
	    }

	    Map<String, String> envs = System.getenv();

	    appAttemptID = Records.newRecord(ApplicationAttemptId.class);
	    if (!envs.containsKey(ApplicationConstants.AM_CONTAINER_ID_ENV)) {
	      if (cliParser.hasOption("app_attempt_id")) {
	        String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
	        appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
	      } 
	      else {
	        throw new IllegalArgumentException("Application Attempt Id not set in the environment");
	      }
	    } else {
	      ContainerId containerId = ConverterUtils.toContainerId(envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV));
	      appAttemptID = containerId.getApplicationAttemptId();
	    }

	    LOG.info("Application master for app"
	        + ", appId=" + appAttemptID.getApplicationId().getId()
	        + ", clustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp()
	        + ", attemptId=" + appAttemptID.getAttemptId());

	    if (!cliParser.hasOption("shell_command")) {
	      throw new IllegalArgumentException("No shell command specified to be executed by application master");
	    }
	    shellCommand = cliParser.getOptionValue("shell_command");

	    if (cliParser.hasOption("shell_args")) {
	      shellArgs = cliParser.getOptionValue("shell_args");
	    }
	    if (cliParser.hasOption("shell_env")) { 
	      String shellEnvs[] = cliParser.getOptionValues("shell_env");
	      for (String env : shellEnvs) {
	        env = env.trim();
	        int index = env.indexOf('=');
	        if (index == -1) {
	          shellEnv.put(env, "");
	          continue;
	        }
	        String key = env.substring(0, index);
	        String val = "";
	        if (index < (env.length()-1)) {
	          val = env.substring(index+1);
	        }
	        shellEnv.put(key, val);
	      }
	    }

	    
	    if (envs.containsKey(StramConstants.DISTRIBUTEDSHELLSCRIPTLOCATION)) {
	      shellScriptPath = envs.get(StramConstants.DISTRIBUTEDSHELLSCRIPTLOCATION);

	      if (envs.containsKey(StramConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP)) {
	        shellScriptPathTimestamp = Long.valueOf(envs.get(StramConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP));
	      }
	      if (envs.containsKey(StramConstants.DISTRIBUTEDSHELLSCRIPTLEN)) {
	        shellScriptPathLen = Long.valueOf(envs.get(StramConstants.DISTRIBUTEDSHELLSCRIPTLEN));
	      }

	      if (!shellScriptPath.isEmpty()
	          && (shellScriptPathTimestamp <= 0 
	          || shellScriptPathLen <= 0)) {
	        LOG.error("Illegal values in env for shell script path"
	            + ", path=" + shellScriptPath
	            + ", len=" + shellScriptPathLen
	            + ", timestamp=" + shellScriptPathTimestamp);
	        throw new IllegalArgumentException("Illegal values in env for shell script path");
	      }
	    }


	    containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
	    numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
	    requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));

	    // start RPC server
	    rpcImpl = new StreamingNodeParent(this.getClass().getName());
	    rpcImpl.init(conf);
	    rpcImpl.start();
	    LOG.info("Container callback server listening at " + rpcImpl.getAddress());
	    return true;
	  }

	  /**
	   * Helper function to print usage 
	   * @param opts Parsed command line options
	   */
	  private void printUsage(Options opts) {
	    new HelpFormatter().printHelp("ApplicationMaster", opts);
	  }

	  /**
	   * Main run function for the application master
	   * @throws YarnRemoteException
	   */
	  public boolean run() throws YarnRemoteException {
	    LOG.info("Starting ApplicationMaster");

	    // Connect to ResourceManager
	    resourceManager = connectToRM();

	    // Setup local RPC Server to accept status requests directly from clients 
	    // TODO need to setup a protocol for client to be able to communicate to the RPC server 
	    // TODO use the rpc port info to register with the RM for the client to send requests to this app master

	    // Register self with ResourceManager 
	    RegisterApplicationMasterResponse response = registerToRM();
	    // Dump out information about cluster capability as seen by the resource manager
	    int minMem = response.getMinimumResourceCapability().getMemory();
	    int maxMem = response.getMaximumResourceCapability().getMemory();
	    LOG.info("Min mem capabililty of resources in this cluster " + minMem);
	    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

	    // A resource ask has to be atleast the minimum of the capability of the cluster, the value has to be 
	    // a multiple of the min value and cannot exceed the max. 
	    // If it is not an exact multiple of min, the RM will allocate to the nearest multiple of min
	    if (containerMemory < minMem) {
	      LOG.info("Container memory specified below min threshold of cluster. Using min value."
	          + ", specified=" + containerMemory
	          + ", min=" + minMem);
	      containerMemory = minMem; 
	    } 
	    else if (containerMemory > maxMem) {
	      LOG.info("Container memory specified above max threshold of cluster. Using max value."
	          + ", specified=" + containerMemory
	          + ", max=" + maxMem);
	      containerMemory = maxMem;
	    }

	    // Setup heartbeat emitter
	    // TODO poll RM every now and then with an empty request to let RM know that we are alive
	    // The heartbeat interval after which an AM is timed out by the RM is defined by a config setting: 
	    // RM_AM_EXPIRY_INTERVAL_MS with default defined by DEFAULT_RM_AM_EXPIRY_INTERVAL_MS
	    // The allocate calls to the RM count as heartbeats so, for now, this additional heartbeat emitter 
	    // is not required.

	    // Setup ask for containers from RM
	    // Send request for containers to RM
	    // Until we get our fully allocated quota, we keep on polling RM for containers
	    // Keep looping until all the containers are launched and shell script executed on them 
	    // ( regardless of success/failure). 

	    int loopCounter = -1;

	    while (numCompletedContainers.get() < numTotalContainers
	        && !appDone) {
	      loopCounter++;

	      // log current state
	      LOG.info("Current application state: loop=" + loopCounter 
	          + ", appDone=" + appDone
	          + ", total=" + numTotalContainers
	          + ", requested=" + numRequestedContainers
	          + ", completed=" + numCompletedContainers
	          + ", failed=" + numFailedContainers
	          + ", currentAllocated=" + numAllocatedContainers);

	      // Sleep before each loop when asking RM for containers
	      // to avoid flooding RM with spurious requests when it 
	      // need not have any available containers 
	      // Sleeping for 1000 ms.
	      try {
	        Thread.sleep(1000);
	      } catch (InterruptedException e) {
	        LOG.info("Sleep interrupted " + e.getMessage());
	      }

	      // No. of containers to request 
	      // For the first loop, askCount will be equal to total containers needed 
	      // From that point on, askCount will always be 0 as current implementation 
	      // does not change its ask on container failures. 
	      int askCount = numTotalContainers - numRequestedContainers.get();
	      numRequestedContainers.addAndGet(askCount);

	      // Setup request to be sent to RM to allocate containers
	      List<ResourceRequest> resourceReq = new ArrayList<ResourceRequest>();
	      if (askCount > 0) {
	        ResourceRequest containerAsk = setupContainerAskForRM(askCount);
	        resourceReq.add(containerAsk);
	      }

	      // Send the request to RM 
	      LOG.info("Asking RM for containers"
	          + ", askCount=" + askCount);
	      AMResponse amResp =sendContainerAskToRM(resourceReq);

	      // Retrieve list of allocated containers from the response 
	      List<Container> allocatedContainers = amResp.getAllocatedContainers();
	      LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
	      numAllocatedContainers.addAndGet(allocatedContainers.size());
	      for (Container allocatedContainer : allocatedContainers) {
	        LOG.info("Launching shell command on a new container."
	            + ", containerId=" + allocatedContainer.getId()
	            + ", containerNode=" + allocatedContainer.getNodeId().getHost() 
	            + ":" + allocatedContainer.getNodeId().getPort()
	            + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
	            + ", containerState" + allocatedContainer.getState()
	            + ", containerResourceMemory" + allocatedContainer.getResource().getMemory());
	        //+ ", containerToken" + allocatedContainer.getContainerToken().getIdentifier().toString());

	        LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer);
	        Thread launchThread = new Thread(runnableLaunchContainer);

	        // launch and start the container on a separate thread to keep the main thread unblocked
	        // as all containers may not be allocated at one go.
	        launchThreads.add(launchThread);
	        launchThread.start();
	      }

	      // Check what the current available resources in the cluster are
	      // TODO should we do anything if the available resources are not enough? 
	      Resource availableResources = amResp.getAvailableResources();
	      LOG.info("Current available resources in the cluster " + availableResources);

	      // Check the completed containers
	      List<ContainerStatus> completedContainers = amResp.getCompletedContainersStatuses();
	      LOG.info("Got response from RM for container ask, completedCnt=" + completedContainers.size());
	      for (ContainerStatus containerStatus : completedContainers) {
	        LOG.info("Got container status for containerID= " + containerStatus.getContainerId()
	            + ", state=" + containerStatus.getState()
	            + ", exitStatus=" + containerStatus.getExitStatus() 
	            + ", diagnostics=" + containerStatus.getDiagnostics());

	        // non complete containers should not be here 
	        assert(containerStatus.getState() == ContainerState.COMPLETE);

	        // increment counters for completed/failed containers
	        int exitStatus = containerStatus.getExitStatus();
	        if (0 != exitStatus) {
	          // container failed 
	          if (-100 != exitStatus) {
	            // shell script failed
	            // counts as completed 
	            numCompletedContainers.incrementAndGet();
	            numFailedContainers.incrementAndGet();
	          }
	          else { 
	            // something else bad happened 
	            // app job did not complete for some reason 
	            // we should re-try as the container was lost for some reason
	            numAllocatedContainers.decrementAndGet();
	            numRequestedContainers.decrementAndGet();
	            // we do not need to release the container as it would be done
	            // by the RM/CM.
	          }
	        }
	        else { 
	          // nothing to do 
	          // container completed successfully 
	          numCompletedContainers.incrementAndGet();
	          LOG.info("Container completed successfully."
	              + ", containerId=" + containerStatus.getContainerId());
	        }

	      }
	      if (numCompletedContainers.get() == numTotalContainers) {
	        appDone = true;
	      }

	      LOG.info("Current application state: loop=" + loopCounter
	          + ", appDone=" + appDone
	          + ", total=" + numTotalContainers
	          + ", requested=" + numRequestedContainers
	          + ", completed=" + numCompletedContainers
	          + ", failed=" + numFailedContainers
	          + ", currentAllocated=" + numAllocatedContainers);

	      // TODO 
	      // Add a timeout handling layer 
	      // for misbehaving shell commands
	    }

	    // Join all launched threads
	    // needed for when we time out 
	    // and we need to release containers
	    for (Thread launchThread : launchThreads) {
	      try {
	        launchThread.join(10000);
	      } catch (InterruptedException e) {
	        LOG.info("Exception thrown in thread join: " + e.getMessage());
	        e.printStackTrace();
	      }
	    }

	    // When the application completes, it should send a finish application signal 
	    // to the RM
	    LOG.info("Application completed. Signalling finish to RM");

	    FinishApplicationMasterRequest finishReq = Records.newRecord(FinishApplicationMasterRequest.class);
	    finishReq.setAppAttemptId(appAttemptID);
	    boolean isSuccess = true;
	    if (numFailedContainers.get() == 0) {
	      finishReq.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED);
	    }
	    else {
	      finishReq.setFinishApplicationStatus(FinalApplicationStatus.FAILED);
	      String diagnostics = "Diagnostics."
	          + ", total=" + numTotalContainers
	          + ", completed=" + numCompletedContainers.get()
	          + ", allocated=" + numAllocatedContainers.get()
	          + ", failed=" + numFailedContainers.get();
	      finishReq.setDiagnostics(diagnostics);
	      isSuccess = false;
	    }
	    resourceManager.finishApplicationMaster(finishReq);
	    return isSuccess;
	  }

	  /**
	   * Thread to connect to the {@link ContainerManager} and 
	   * launch the container that will execute the shell command. 
	   */
	  private class LaunchContainerRunnable implements Runnable {

	    // Allocated container 
	    Container container;
	    // Handle to communicate with ContainerManager
	    ContainerManager cm;

	    /**
	     * @param lcontainer Allocated container
	     */
	    public LaunchContainerRunnable(Container lcontainer) {
	      this.container = lcontainer;
	    }

	    /**
	     * Helper function to connect to CM
	     */
	    private void connectToCM() {
	      LOG.debug("Connecting to ContainerManager for containerid=" + container.getId());
	      String cmIpPortStr = container.getNodeId().getHost() + ":"
	          + container.getNodeId().getPort();
	      InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);
	      LOG.info("Connecting to ContainerManager at " + cmIpPortStr);
	      this.cm = ((ContainerManager) rpc.getProxy(ContainerManager.class, cmAddress, conf));
	    }

	    private void setClasspath(Map<String, String> env) {
	      // add localized application jar files to classpath    
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
	      LOG.info("CLASSPATH: {}", classPathEnv);
	    }
	    
	    private void addLocalResources(Map<String, LocalResource> resources) throws IOException {
        // child VM dependencies
        // make our own jar file available to new container, in the location that CLASSPATH references
        // same as when launching the appMaster, except that the file is already distributed to dfs
        
        // Create a local resource to point to the destination jar path 
        FileSystem fs = FileSystem.get(conf);
        ApplicationId appId = container.getId().getApplicationAttemptId().getApplicationId();
        String pathSuffix = StramConstants.APPNAME + "/" + appId.getId() + "/Stram.jar";     
        LOG.info("localize application jar from: " + pathSuffix);
        Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
        LocalResource appJarRsrc = Records.newRecord(LocalResource.class);
        appJarRsrc.setType(LocalResourceType.FILE);
        appJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);    
        appJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst)); 
        // Set timestamp and length of file so that the framework 
        // can do basic sanity checks for the local resource 
        // after it has been copied over to ensure it is the same 
        // resource the client intended to use with the application
        FileStatus destStatus = fs.getFileStatus(dst);
        appJarRsrc.setTimestamp(destStatus.getModificationTime());
        appJarRsrc.setSize(destStatus.getLen());
        resources.put("Stram.jar",  appJarRsrc);
	    }
	    

	    @Override
	    /**
	     * Connects to CM, sets up container launch context 
	     * for shell command and eventually dispatches the container 
	     * start request to the CM. 
	     */
	    public void run() {
	      // Connect to ContainerManager 
	      connectToCM();

	      LOG.info("Setting up container launch container for containerid=" + container.getId());
	      ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

	      ctx.setContainerId(container.getId());
	      ctx.setResource(container.getResource());

	      try {
	        ctx.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
	      } catch (IOException e) {
	        LOG.info("Getting current user info failed when trying to launch the container"
	            + e.getMessage());
	      }
	      
	      setClasspath(shellEnv);
	      
	      // Set the environment 
	      ctx.setEnvironment(shellEnv);

	      // Set the local resources 
	      Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

	      
        // The container for the eventual shell commands needs its own local resources too. 
        // In this scenario, if a shell script is specified, we need to have it copied 
        // and made available to the container. 
        if (!shellScriptPath.isEmpty()) {
          LocalResource shellRsrc = Records.newRecord(LocalResource.class);
          shellRsrc.setType(LocalResourceType.FILE);
          shellRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
          try {
            shellRsrc.setResource(ConverterUtils.getYarnUrlFromURI(new URI(shellScriptPath)));
          } catch (URISyntaxException e) {
            LOG.error("Error when trying to use shell script path specified in env"
                + ", path=" + shellScriptPath);
            e.printStackTrace();

            // A failure scenario on bad input such as invalid shell script path 
            // We know we cannot continue launching the container 
            // so we should release it.
            // TODO
            numCompletedContainers.incrementAndGet();
            numFailedContainers.incrementAndGet();
            return;
          }
          shellRsrc.setTimestamp(shellScriptPathTimestamp);
          shellRsrc.setSize(shellScriptPathLen);
          localResources.put(ExecShellStringPath, shellRsrc);
        }
       
        // add resources for child VM
        try {
  	      addLocalResources(localResources);
  	      ctx.setLocalResources(localResources);
        } catch (IOException e) {
          LOG.error("Failed to prepare local resources.", e);
          return;
        }

	      // Set the necessary command to execute on the allocated container 
	      List<CharSequence> vargs = new Vector<CharSequence>(5);
	      // Set executable command 
        vargs.add(shellCommand);

        // Set shell script path 
	      if (!shellScriptPath.isEmpty()) {
	        vargs.add(ExecShellStringPath);
	      }
	      // Set args for the shell command if any
	      vargs.add(shellArgs);
	      // Add log redirect params
	      // TODO
	      // We should redirect the output to hdfs instead of local logs 
	      // so as to be able to look at the final output after the containers 
	      // have been released. 
	      // Could use a path suffixed with /AppId/AppAttempId/ContainerId/std[out|err] 
	      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
	      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

if (!runShell) {
  vargs = getChildVMCommand(container.getId().toString());
}
	      
	      // Get final commmand
	      StringBuilder command = new StringBuilder();
	      for (CharSequence str : vargs) {
	        command.append(str).append(" ");
	      }
LOG.info("Final command is: {}", command);
	      
	      List<String> commands = new ArrayList<String>();
	      commands.add(command.toString());
	      ctx.setCommands(commands);

	      StartContainerRequest startReq = Records.newRecord(StartContainerRequest.class);
	      startReq.setContainerLaunchContext(ctx);
	      try {
	        cm.startContainer(startReq);
	      } catch (YarnRemoteException e) {
	        LOG.error("Start container failed for :"
	            + ", containerId=" + container.getId());
	        e.printStackTrace();
	        // TODO do we need to release this container? 
	      }

	      // Get container status?
	      // Left commented out as the shell scripts are short lived 
	      // and we are relying on the status for completed containers from RM to detect status

	      //    GetContainerStatusRequest statusReq = Records.newRecord(GetContainerStatusRequest.class);
	      //    statusReq.setContainerId(container.getId());
	      //    GetContainerStatusResponse statusResp;
	      //try {
	      //statusResp = cm.getContainerStatus(statusReq);
	      //    LOG.info("Container Status"
	      //    + ", id=" + container.getId()
	      //    + ", status=" +statusResp.getStatus());
	      //} catch (YarnRemoteException e) {
	      //e.printStackTrace();
	      //}
	    }
	    
	    /**
	     * Build the command to launch the child VM in the container
	     * TODO: Build based on streaming node configuration
	     * @param callbackListenerAddr
	     * @param task
	     * @param jvmID
	     * @return
	     */
	    public List<CharSequence> getChildVMCommand(
	        String jvmID) {

	      List<CharSequence> vargs = new ArrayList<CharSequence>(8);

	      //vargs.add("exec");
	      vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
	    
	      Path childTmpDir = new Path(Environment.PWD.$(),
	          YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
	      vargs.add("-Djava.io.tmpdir=" + childTmpDir);
	      
	      // Add main class and its arguments 
	      vargs.add(StramChild.class.getName());  // main of Child
	      // pass TaskAttemptListener's address
	      vargs.add(rpcImpl.getAddress().getAddress().getHostAddress()); 
	      vargs.add(Integer.toString(rpcImpl.getAddress().getPort())); 

	      // Finally add the jvmID
	      vargs.add(String.valueOf(jvmID));
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

	      // Final commmand
	      StringBuilder mergedCommand = new StringBuilder();
	      for (CharSequence str : vargs) {
	        mergedCommand.append(str).append(" ");
	      }
	      List<CharSequence> vargsFinal = new ArrayList<CharSequence>(1);
	      vargsFinal.add(mergedCommand.toString());
	      return vargsFinal;	      
	      	      
	    }
	    
	  }

	  /**
	   * Connect to the Resource Manager
	   * @return Handle to communicate with the RM
	   */
	  private AMRMProtocol connectToRM() {
	    YarnConfiguration yarnConf = new YarnConfiguration(conf);
	    InetSocketAddress rmAddress = yarnConf.getSocketAddr(
	        YarnConfiguration.RM_SCHEDULER_ADDRESS,
	        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
	        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
	    LOG.info("Connecting to ResourceManager at " + rmAddress);
	    return ((AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf));
	  }

	  /** 
	   * Register the Application Master to the Resource Manager
	   * @return the registration response from the RM
	   * @throws YarnRemoteException
	   */
	  private RegisterApplicationMasterResponse registerToRM() throws YarnRemoteException {
	    RegisterApplicationMasterRequest appMasterRequest = Records.newRecord(RegisterApplicationMasterRequest.class);

	    // set the required info into the registration request: 
	    // application attempt id, 
	    // host on which the app master is running
	    // rpc port on which the app master accepts requests from the client 
	    // tracking url for the app master
	    appMasterRequest.setApplicationAttemptId(appAttemptID);
	    appMasterRequest.setHost(appMasterHostname);
	    appMasterRequest.setRpcPort(appMasterRpcPort);
	    appMasterRequest.setTrackingUrl(appMasterTrackingUrl);

	    return resourceManager.registerApplicationMaster(appMasterRequest);
	  }

	  /**
	   * Setup the request that will be sent to the RM for the container ask.
	   * @param numContainers Containers to ask for from RM
	   * @return the setup ResourceRequest to be sent to RM
	   */
	  private ResourceRequest setupContainerAskForRM(int numContainers) {
	    ResourceRequest request = Records.newRecord(ResourceRequest.class);

	    // setup requirements for hosts 
	    // whether a particular rack/host is needed 
	    // Refer to apis under org.apache.hadoop.net for more 
	    // details on how to get figure out rack/host mapping.
	    // using * as any host will do for the distributed shell app
	    request.setHostName("*");

	    // set no. of containers needed
	    request.setNumContainers(numContainers);

	    // set the priority for the request
	    Priority pri = Records.newRecord(Priority.class);
	    // TODO - what is the range for priority? how to decide? 
	    pri.setPriority(requestPriority);
	    request.setPriority(pri);

	    // Set up resource type requirements
	    // For now, only memory is supported so we set memory requirements
	    Resource capability = Records.newRecord(Resource.class);
	    capability.setMemory(containerMemory);
	    request.setCapability(capability);

	    return request;
	  }

	  /**
	   * Ask RM to allocate given no. of containers to this Application Master
	   * @param requestedContainers Containers to ask for from RM
	   * @return Response from RM to AM with allocated containers 
	   * @throws YarnRemoteException
	   */
	  private AMResponse sendContainerAskToRM(List<ResourceRequest> requestedContainers)
	      throws YarnRemoteException {
	    AllocateRequest req = Records.newRecord(AllocateRequest.class);
	    req.setResponseId(rmRequestID.incrementAndGet());
	    req.setApplicationAttemptId(appAttemptID);
	    req.addAllAsks(requestedContainers);
	    req.addAllReleases(releasedContainers);
	    req.setProgress((float)numCompletedContainers.get()/numTotalContainers);

	    LOG.info("Sending request to RM for containers"
	        + ", requestedSet=" + requestedContainers.size()
	        + ", releasedSet=" + releasedContainers.size()
	        + ", progress=" + req.getProgress());

	    for (ResourceRequest  rsrcReq : requestedContainers) {
	      LOG.info("Requested container ask: " + rsrcReq.toString());
	    }
	    for (ContainerId id : releasedContainers) {
	      LOG.info("Released container, id=" + id.getId());
	    }

	    AllocateResponse resp = resourceManager.allocate(req);
	    return resp.getAMResponse();
	  }

}
