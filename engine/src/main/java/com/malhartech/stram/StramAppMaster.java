/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.ClusterInfo;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.bufferserver.Server;
import com.malhartech.stram.StramChildAgent.DeployRequest;
import com.malhartech.stram.cli.StramClientUtils.YarnClientHelper;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.webapp.StramWebApp;

/**
 * see {@link org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster}
 *
 */
public class StramAppMaster
{
  private static Logger LOG = LoggerFactory.getLogger(StramAppMaster.class);
  // Configuration
  private Configuration conf;
  private YarnClientHelper yarnClient;
  private TopologyBuilder logicalTopology;
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
  // Priority of the request
  private int requestPriority;
  // Incremental counter for rpc calls to the RM
  private AtomicInteger rmRequestID = new AtomicInteger();
  // Simple flag to denote whether all works is done
  private boolean appDone = false;
  // Counter for completed containers ( complete denotes successful or failed )
  private AtomicInteger numCompletedContainers = new AtomicInteger();
  // Containers that the RM has allocated to us
  private Map<String, Container> allAllocatedContainers = new HashMap<String, Container>();

  // Count of failed containers
  private AtomicInteger numFailedContainers = new AtomicInteger();
  // Launch threads
  private List<Thread> launchThreads = new ArrayList<Thread>();
  // child container callback
  private StreamingNodeParent rpcImpl;
  private DNodeManager dnmgr;
  private InetSocketAddress bufferServerAddress;
  final private Clock clock = new SystemClock();
  final private long startTime = clock.getTime();

  private class ClusterAppContextImpl implements StramAppContext
  {
    @Override
    public ApplicationId getApplicationID()
    {
      return appAttemptID.getApplicationId();
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId()
    {
      return appAttemptID;
    }

    @Override
    public String getApplicationName()
    {
      // TODO get from the app master env / ClientRMProtocol
      return "TODO: application name";
    }

    @Override
    public long getStartTime()
    {
      return startTime;
    }

    @Override
    public CharSequence getUser()
    {
      // TODO get from the app master env / ClientRMProtocol
      return "TODO: user";
    }

    @Override
    public Clock getClock()
    {
      return clock;
    }

    @Override
    public EventHandler<?> getEventHandler()
    {
      return null;
    }

    @Override
    public ClusterInfo getClusterInfo()
    {
      return null;
    }
  }

  /**
   * @param args Command line args
   */
  public static void main(String[] args)
  {
    boolean result = false;

    StringWriter sw = new StringWriter();
    for (Map.Entry<String, String> e : System.getenv().entrySet()) {
      sw.append("\n").append(e.getKey()).append("=").append(e.getValue());
    }
    LOG.info("appmaster env:" + sw.toString());

    try {
      StramAppMaster appMaster = new StramAppMaster();
      LOG.info("Initializing ApplicationMaster");
      boolean doRun = appMaster.init(args);
      if (!doRun) {
        System.exit(0);
      }
      result = appMaster.run();
    }
    catch (Throwable t) {
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
  private void dumpOutDebugInfo()
  {

    LOG.info("Dump debug output");
    Map<String, String> envs = System.getenv();
    LOG.info("\nDumping System Env: begin");
    for (Map.Entry<String, String> env : envs.entrySet()) {
      LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
      System.out.println("System env: key=" + env.getKey() + ", val=" + env.getValue());
    }
    LOG.info("Dumping System Env: end");

    String cmd = "ls -al";
    Runtime run = Runtime.getRuntime();
    Process pr = null;
    try {
      pr = run.exec(cmd);
      pr.waitFor();

      BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
      String line = "";
      LOG.info("\nDumping files in local dir: begin");
      while ((line = buf.readLine()) != null) {
        LOG.info("System CWD content: " + line);
        System.out.println("System CWD content: " + line);
      }
      LOG.info("Dumping files in local dir: end");
      buf.close();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }

    LOG.info("Classpath: {}", System.getProperty("java.class.path"));
    LOG.info("Config resources: {}", conf.toString());
    try {
      Configuration.dumpConfiguration(conf, new PrintWriter(System.out));
    }
    catch (Exception e) {
      LOG.error("Error dumping configuration.", e);
    };

    try {
      LOG.info("Topology: ");
      Properties tplgProperties = TopologyBuilder.toProperties(logicalTopology.getConf());
      tplgProperties.store(System.out, "topology for " + appAttemptID);
    }
    catch (Exception e) {
      LOG.error("Error dumping topology.", e);
    };

  }

  public StramAppMaster() throws Exception
  {
    // Set up the configuration and RPC
    this.conf = new YarnConfiguration();
    this.yarnClient = new YarnClientHelper(this.conf);
  }

  /**
   * Parse command line options
   *
   * @param args Command line args
   * @return Whether init successful and run should be invoked
   * @throws ParseException
   * @throws IOException
   */
  public boolean init(String[] args) throws ParseException, IOException, Exception
  {

    Options opts = new Options();
    opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes");
    opts.addOption("priority", true, "Application Priority. Default 0");

    opts.addOption("help", false, "Print usage");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    // option "help" overrides and cancels any run
    if (cliParser.hasOption("help")) {
      printUsage(opts);
      return false;
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
    }
    else {
      ContainerId containerId = ConverterUtils.toContainerId(envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV));
      appAttemptID = containerId.getApplicationAttemptId();
    }

    LOG.info("Application master for app"
             + ", appId=" + appAttemptID.getApplicationId().getId()
             + ", clustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp()
             + ", attemptId=" + appAttemptID.getAttemptId());

    requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));

    // set topology - read from localized dfs location populated by submit client
    logicalTopology = new TopologyBuilder(conf);
    Properties tplgProperties = readProperties("./stram.properties");
    logicalTopology.addFromProperties(tplgProperties);

    // "debug" simply dumps all data using LOG.info
    if (logicalTopology.isDebug()) {
      dumpOutDebugInfo();
    }

    this.dnmgr = new DNodeManager(logicalTopology);

    // start RPC server
    rpcImpl = new StreamingNodeParent(this.getClass().getName(), dnmgr);
    rpcImpl.init(conf);
    rpcImpl.start();
    LOG.info("Container callback server listening at " + rpcImpl.getAddress());

    LOG.info("Initializing logical topology with {} nodes in {} containers", logicalTopology.getAllNodes().size(), dnmgr.getNumRequiredContainers());

    // start buffer server
    com.malhartech.bufferserver.Server s = new Server(0);
    SocketAddress bindAddr = s.run();
    this.bufferServerAddress = ((InetSocketAddress) bindAddr);
    LOG.info("Buffer server started: {}", bufferServerAddress);

    StramAppContext appContext = new ClusterAppContextImpl();
    // start web service
    try {
      WebApp webApp = WebApps.$for("stram", StramAppContext.class, appContext, "ws").with(conf).
        start(new StramWebApp(this.dnmgr));
      LOG.info("Started web service at port: " + webApp.port());
      this.appMasterTrackingUrl = NetUtils.getConnectAddress(rpcImpl.getAddress()).getHostName() + ":" + webApp.port();
      LOG.info("Setting tracking URL to: " + appMasterTrackingUrl);
    }
    catch (Exception e) {
      LOG.error("Webapps failed to start. Ignoring for now:", e);
    }

    return true;
  }

  public static Properties readProperties(String filePath) throws IOException
  {
    InputStream is = new FileInputStream(filePath);
    Properties props = new Properties(System.getProperties());
    props.load(is);
    is.close();
    return props;
  }

  /**
   * Helper function to print usage
   *
   * @param opts Parsed command line options
   */
  private void printUsage(Options opts)
  {
    new HelpFormatter().printHelp("ApplicationMaster", opts);
  }

  /**
   * Main run function for the application master
   *
   * @throws YarnRemoteException
   */
  public boolean run() throws YarnRemoteException
  {
    LOG.info("Starting ApplicationMaster");

    // Connect to ResourceManager
    resourceManager = yarnClient.connectToRM();

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
    int containerMemory = logicalTopology.getContainerMemoryMB();
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
    // Keep looping until all containers finished processing
    // ( regardless of success/failure).

    int loopCounter = -1;
    List<ContainerId> releasedContainers = new ArrayList<ContainerId>();
    int numTotalContainers = 0;
    // keep track of already requested containers to not request them again while waiting for allocation
    int numRequestedContainers = 0;

    while (!appDone) {
      loopCounter++;

      // log current state
      LOG.info("Current application state: loop=" + loopCounter
               + ", appDone=" + appDone
               + ", total=" + numTotalContainers
               + ", requested=" + numRequestedContainers
               + ", completed=" + numCompletedContainers
               + ", failed=" + numFailedContainers
               + ", currentAllocated=" + this.allAllocatedContainers.size());

      // Sleep before each loop when asking RM for containers
      // to avoid flooding RM with spurious requests when it
      // need not have any available containers
      // Sleeping for 1000 ms.
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        LOG.info("Sleep interrupted " + e.getMessage());
      }

      // No. of containers to request
      // For the first loop, askCount will be equal to total containers needed
      // From that point on, askCount will be based on incremental deploy requests
      int askCount = 0;

      // request containers for pending deploy requests
      if (!dnmgr.containerStartRequests.isEmpty()) {
        askCount = dnmgr.containerStartRequests.size() - numRequestedContainers;
      }

      // Setup request to be sent to RM to allocate containers
      List<ResourceRequest> resourceReq = new ArrayList<ResourceRequest>();
      if (askCount > 0) {
        ResourceRequest containerAsk = setupContainerAskForRM(askCount, containerMemory);
        resourceReq.add(containerAsk);
        numTotalContainers += askCount;
        numRequestedContainers += askCount;
      } else {
        if (askCount < 0) {
          LOG.warn("negative container ask count: {}", askCount);
          // container will be released once allocated and no deploy request pending
        }
      }

      // Send the request to RM
      LOG.info("Asking RM for containers"
               + ", askCount=" + askCount);
      AMResponse amResp = sendContainerAskToRM(resourceReq, releasedContainers);
      releasedContainers.clear();

      // Retrieve list of allocated containers from the response
      List<Container> newAllocatedContainers = amResp.getAllocatedContainers();
      LOG.info("Got response from RM for container ask, allocatedCnt=" + newAllocatedContainers.size());
      numRequestedContainers -= newAllocatedContainers.size();
      for (Container allocatedContainer : newAllocatedContainers) {
        LOG.info("Got new container."
                 + ", containerId=" + allocatedContainer.getId()
                 + ", containerNode=" + allocatedContainer.getNodeId().getHost()
                 + ":" + allocatedContainer.getNodeId().getPort()
                 + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
                 + ", containerState" + allocatedContainer.getState()
                 + ", containerResourceMemory" + allocatedContainer.getResource().getMemory());
        //+ ", containerToken" + allocatedContainer.getContainerToken().getIdentifier().toString());

        // get next pending deploy requests
        DeployRequest cdr = dnmgr.containerStartRequests.poll();
        if (cdr == null) {
          // allocated container no longer needed, add release request
          LOG.warn("Container {} allocated but nothing to deploy, going to release this container.", allocatedContainer.getId());
          releasedContainers.add(allocatedContainer.getId());
        } else {
          // setup new container context
          dnmgr.assignContainer(cdr, allocatedContainer.getId().toString(), NetUtils.getConnectAddress(this.bufferServerAddress));
          this.allAllocatedContainers.put(allocatedContainer.getId().toString(), allocatedContainer);
          // launch and start the container on a separate thread to keep the main thread unblocked
          LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, yarnClient, logicalTopology, rpcImpl.getAddress());
          Thread launchThread = new Thread(runnableLaunchContainer);
          launchThreads.add(launchThread);
          launchThread.start();
        }
      }

      // TODO: we need to obtain the initial list...
      // keep track of updated nodes - we use this info to make decisions about where to request new containers
      List<NodeReport> nodeReports = amResp.getUpdatedNodes();
      LOG.info("Got {} updated node reports.", nodeReports.size());
      for (NodeReport nr : nodeReports) {
        StringBuilder sb = new StringBuilder();
        sb.append("rackName=").append(nr.getRackName()).append("nodeid=").append(nr.getNodeId()).append("numContainers=").append(nr.getNumContainers()).append("capability=").append(nr.getCapability()).append("used=").append(nr.getUsed()).append("state=").append(nr.getNodeState());
        LOG.info("Node report: " + sb);
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
        assert (containerStatus.getState() == ContainerState.COMPLETE);
        allAllocatedContainers.remove(containerStatus.getContainerId().toString());

        // increment counters for completed/failed containers
        int exitStatus = containerStatus.getExitStatus();
        LOG.info("Container {} exit status {}.", containerStatus.getContainerId(), exitStatus);
        if (0 != exitStatus) {
          // StramChild failure or process killed
          numFailedContainers.incrementAndGet();
          LOG.info("Container {} failed, launching new container.", containerStatus.getContainerId());
          dnmgr.restartContainer(containerStatus.getContainerId().toString());
        }
        else {
          // container completed successfully
          numCompletedContainers.incrementAndGet();
          dnmgr.markComplete(containerStatus.getContainerId().toString());
          LOG.info("Container completed successfully."
                   + ", containerId=" + containerStatus.getContainerId());
        }

      }

      if (allAllocatedContainers.size() == 0 && dnmgr.containerStartRequests.isEmpty()) {
        appDone = true;
      }

      LOG.info("Current application state: loop=" + loopCounter
               + ", appDone=" + appDone
               + ", total=" + numTotalContainers
               + ", requested=" + numRequestedContainers
               + ", completed=" + numCompletedContainers
               + ", failed=" + numFailedContainers
               + ", currentAllocated=" + allAllocatedContainers.size());

      // monitor child containers
      dnmgr.monitorHeartbeat();
    }

    // Join all launched threads
    // needed for when we time out
    // and we need to release containers
    for (Thread launchThread : launchThreads) {
      try {
        launchThread.join(10000);
      }
      catch (InterruptedException e) {
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
                           + ", allocated=" + allAllocatedContainers.size()
                           + ", failed=" + numFailedContainers.get();
      finishReq.setDiagnostics(diagnostics);
      isSuccess = false;
    }
    resourceManager.finishApplicationMaster(finishReq);
    return isSuccess;
  }

  /**
   * Register the Application Master to the Resource Manager
   *
   * @return the registration response from the RM
   * @throws YarnRemoteException
   */
  private RegisterApplicationMasterResponse registerToRM() throws YarnRemoteException
  {
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
   *
   * @param numContainers Containers to ask for from RM
   * @return the setup ResourceRequest to be sent to RM
   */
  private ResourceRequest setupContainerAskForRM(int numContainers, int containerMemory)
  {
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
   *
   * @param requestedContainers Containers to ask for from RM
   * @return Response from RM to AM with allocated containers
   * @throws YarnRemoteException
   */
  private AMResponse sendContainerAskToRM(List<ResourceRequest> requestedContainers, List<ContainerId> releasedContainers)
    throws YarnRemoteException
  {
    AllocateRequest req = Records.newRecord(AllocateRequest.class);
    req.setResponseId(rmRequestID.incrementAndGet());
    req.setApplicationAttemptId(appAttemptID);
    req.addAllAsks(requestedContainers);

    for (String containerIdStr : dnmgr.containerStopRequests.values()) {
      Container allocatedContainer = this.allAllocatedContainers.get(containerIdStr);
      if (allocatedContainer != null) {
         // issue stop container - TODO: separate thread to not block heartbeat
        ContainerManager cm = yarnClient.connectToCM(allocatedContainer);
        StopContainerRequest stopContainer = Records.newRecord(StopContainerRequest.class);
        stopContainer.setContainerId(allocatedContainer.getId());
        cm.stopContainer(stopContainer);
        LOG.info("Stopped container {}", containerIdStr);
        // RM does not report container as completed when stopped from here
        this.numFailedContainers.incrementAndGet();
        //releasedContainers.add(allocatedContainer.getId());
      }
      dnmgr.containerStopRequests.remove(containerIdStr);
    }

    req.addAllReleases(releasedContainers);
    //req.setProgress((float) numCompletedContainers.get() / numTotalContainers);

    LOG.info("Sending request to RM for containers"
             + ", requestedSet=" + requestedContainers.size()
             + ", releasedSet=" + releasedContainers.size()
             + ", progress=" + req.getProgress());

    for (ResourceRequest rsrcReq : requestedContainers) {
      LOG.info("Requested container ask: " + rsrcReq.toString());
    }
    for (ContainerId id : releasedContainers) {
      LOG.info("Released container, id=" + id.getId());
    }

    AllocateResponse resp = resourceManager.allocate(req);
    return resp.getAMResponse();
  }
}
