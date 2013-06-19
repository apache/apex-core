/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import static java.lang.Thread.sleep;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.bind.annotation.XmlElement;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.Clock;
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
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.debug.StdOutErrLog;
import com.datatorrent.stram.PhysicalPlan.PTContainer;
import com.datatorrent.stram.StreamingContainerManager.ContainerResource;
import com.datatorrent.stram.cli.StramClientUtils.YarnClientHelper;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.security.StramDelegationTokenManager;
import com.datatorrent.stram.util.VersionInfo;
import com.datatorrent.stram.webapp.AppInfo;
import com.datatorrent.stram.webapp.StramWebApp;

/**
 *
 * Streaming Application Master<p>
 * The engine of the streaming platform. Runs as a YARN application master<br>
 * As part of initialization the following tasks are done<br>
 * The DAG is parsed, and properties are read to create a physical query map<br>
 * ResourceMgr is queried to get the requisite containers<br>
 * Then {@link com.datatorrent.stram.StreamingContainerManager} provisions the DAG into those containers and starts them<br>
 * Once the dag is starts {@link com.datatorrent.stram.StramAppMaster} runs the dag on a continual basis<br>
 * Stram can be shut down in the following ways<br>
 * cli command shutdown<br>
 * Currently stram high availability (integration with zookeeper) is not available<br>
 * <br>
 *
 */
public class StramAppMaster //extends License for licensing using native
{
  static {
    // set system properties so they can be used in logger configuration
    Map<String, String> envs = System.getenv();
    String containerIdString = envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV);
    if (containerIdString == null) {
      // container id should always be set in the env by the framework
      throw new IllegalArgumentException(
              "ContainerId not set in the environment");
    }
    System.setProperty("stram.cid", containerIdString);
    //ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
    //ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();
  }

  private static final Logger LOG = LoggerFactory.getLogger(StramAppMaster.class);
  private static final long DELEGATION_KEY_UPDATE_INTERVAL = 24 * 60 * 60 * 1000;
  private static final long DELEGATION_TOKEN_MAX_LIFETIME = 7 * 24 * 60 * 60 * 1000;
  private static final long DELEGATION_TOKEN_RENEW_INTERVAL = 24 * 60 * 60 * 1000;
  private static final long DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL = 3600000;
  // Configuration
  private final Configuration conf;
  private final YarnClientHelper yarnClient;
  private LogicalPlan dag;
  // Handle to communicate with the Resource Manager
  private AMRMProtocol resourceManager;
  // Application Attempt Id ( combination of attemptId and fail count )
  private ApplicationAttemptId appAttemptID;
  // TODO
  // For status update for clients - yet to be implemented
  // Hostname of the container
  private final String appMasterHostname = "";
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = "";
  // App Master configuration
  // Incremental counter for rpc calls to the RM
  private final AtomicInteger rmRequestID = new AtomicInteger();
  // Simple flag to denote whether all works is done
  private boolean appDone = false;
  // Counter for completed containers ( complete denotes successful or failed )
  private final AtomicInteger numCompletedContainers = new AtomicInteger();
  // Containers that the RM has allocated to us
  private final Map<String, Container> allAllocatedContainers = new HashMap<String, Container>();
  // Count of failed containers
  private final AtomicInteger numFailedContainers = new AtomicInteger();
  // Launch threads
  private final List<Thread> launchThreads = new ArrayList<Thread>();
  // child container callback
  private StreamingContainerParent rpcImpl;
  private StreamingContainerManager dnmgr;
  private final Clock clock = new SystemClock();
  private final long startTime = clock.getTime();
  private final ClusterAppStats stats = new ClusterAppStats();
  //private AbstractDelegationTokenSecretManager<? extends TokenIdentifier> delegationTokenManager;
  private StramDelegationTokenManager delegationTokenManager;

  /**
   * Overrides getters to pull live info.
   */
  protected class ClusterAppStats extends AppInfo.AppStats
  {
    @Override
    public int getAllocatedContainers()
    {
      return allAllocatedContainers.size();
    }

    @Override
    public int getPlannedContainers()
    {
      return dnmgr.getPhysicalPlan().getContainers().size();
    }

    @Override
    @XmlElement
    public int getFailedContainers()
    {
      return numFailedContainers.get();
    }

    @Override
    public int getNumOperators()
    {
      int num = 0;
      for (PTContainer c : dnmgr.getPhysicalPlan().getContainers()) {
        num += c.operators.size();
      }
      return num;
    }

  }

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
      if (dag != null) {
        return dag.getAttributes().attr(LogicalPlan.APPLICATION_NAME).get();
      }
      return "unknown";
    }

    @Override
    public long getStartTime()
    {
      return startTime;
    }

    @Override
    public String getApplicationPath()
    {
      if (dag != null) {
        return dag.getAttributes().attr(LogicalPlan.APPLICATION_PATH).get();
      }
      return "unknown";
    }

    @Override
    public CharSequence getUser()
    {
      return System.getenv(ApplicationConstants.Environment.USER.toString());
    }

    @Override
    public Clock getClock()
    {
      return clock;
    }

    @Override
    public String getAppMasterTrackingUrl()
    {
      return appMasterTrackingUrl;
    }

    @Override
    public ClusterAppStats getStats()
    {
      return stats;
    }

    @Override
    public String getDaemonAddress()
    {
      if (dag != null) {
        return dag.attrValue(LogicalPlan.DAEMON_ADDRESS, null);
      }
      return "unknown";
    }

  }

  /**
   * @param args Command line args
   * @throws Throwable
   */
  public static void main(final String[] args) throws Throwable
  {
    StdOutErrLog.tieSystemOutAndErrToLog();
    LOG.info("Master starting with classpath: {}", System.getProperty("java.class.path"));

    LOG.info("version: {}", VersionInfo.getBuildVersion());
    StringWriter sw = new StringWriter();
    for (Map.Entry<String, String> e : System.getenv().entrySet()) {
      sw.append("\n").append(e.getKey()).append("=").append(e.getValue());
    }
    LOG.info("appmaster env:" + sw.toString());

    boolean result = false;
    StramAppMaster appMaster = null;

    try {
      appMaster = new StramAppMaster();
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
    finally {
      if (appMaster != null) {
        appMaster.destroy();
      }
    }

    if (result) {
      LOG.info("Application Master completed. exiting");
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
  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  private void dumpOutDebugInfo()
  {
    LOG.info("Dump debug output");
    Map<String, String> envs = System.getenv();
    LOG.info("\nDumping System Env: begin");
    for (Map.Entry<String, String> env : envs.entrySet()) {
      LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
    }
    LOG.info("Dumping System Env: end");

    String cmd = "ls -al";
    Runtime run = Runtime.getRuntime();
    Process pr;
    try {
      pr = run.exec(cmd);
      pr.waitFor();

      BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
      String line;
      LOG.info("\nDumping files in local dir: begin");
      while ((line = buf.readLine()) != null) {
        LOG.info("System CWD content: " + line);
      }
      LOG.info("Dumping files in local dir: end");
      buf.close();
    }
    catch (IOException e) {
      LOG.debug("Exception", e);
    }
    catch (InterruptedException e) {
      LOG.info("Interrupted", e);
    }

    LOG.info("Classpath: {}", System.getProperty("java.class.path"));
    LOG.info("Config resources: {}", conf.toString());
    try {
      // find a better way of logging this using the logger.
      Configuration.dumpConfiguration(conf, new PrintWriter(System.out));
    }
    catch (Exception e) {
      LOG.error("Error dumping configuration.", e);
    }

    if (dag != null) {
      try {
        LOG.info("DAG: {}", dag.toString());
      }
      catch (Exception e) {
        LOG.error("Error dumping topology.", e);
      }
    }
  }

  public StramAppMaster() throws Exception
  {
    // Set up the configuration and RPC
    this.conf = new YarnConfiguration();
    this.yarnClient = new YarnClientHelper(this.conf);
    //TODO :- Need to perform token renewal
    delegationTokenManager = new StramDelegationTokenManager(DELEGATION_KEY_UPDATE_INTERVAL,
                                                             DELEGATION_TOKEN_MAX_LIFETIME,
                                                             DELEGATION_TOKEN_RENEW_INTERVAL,
                                                             DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL);
  }

  /**
   * Parse command line options
   *
   * @param args Command line args
   * @return Whether init successful and run should be invoked
   * @throws ParseException
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public boolean init(String[] args) throws ParseException, IOException, ClassNotFoundException
  {

    Options opts = new Options();
    opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes");

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

    FileInputStream fis = new FileInputStream("./" + LogicalPlan.SER_FILE_NAME);
    this.dag = LogicalPlan.read(fis);
    fis.close();
    // "debug" simply dumps all data using LOG.info
    if (dag.isDebug()) {
      dumpOutDebugInfo();
    }

    this.dnmgr = new StreamingContainerManager(dag);

    // start the secret manager
    delegationTokenManager.startThreads();

    // start RPC server
    rpcImpl = new StreamingContainerParent(this.getClass().getName(), dnmgr, delegationTokenManager);
    rpcImpl.init(conf);
    rpcImpl.start();
    LOG.info("Container callback server listening at " + rpcImpl.getAddress());

    LOG.info("Initializing application with {} operators in {} containers", dag.getAllOperators().size(), dnmgr.getPhysicalPlan().getContainers().size());

    StramAppContext appContext = new ClusterAppContextImpl();
    // start web service
    try {
      org.mortbay.log.Log.setLog(null);
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

  public void destroy()
  {
    if (delegationTokenManager != null) {
      delegationTokenManager.stopThreads();
    }
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

  public boolean run() throws YarnRemoteException
  {
    try {
      StramChild.eventloop.start();
      //executeLicensedCode(); for licensing using native
      execute();
    }
    catch (RuntimeException re) {
      if (re.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException)re.getCause();
      }
    }
    finally {
      StramChild.eventloop.stop();
    }

    return status;
  }

  /**
   * Main run function for the application master
   *
   * @throws YarnRemoteException
   */
  private boolean status = true;

  //@Override - for licensing using native
  @SuppressWarnings("SleepWhileInLoop")
  public void execute()
  {
    LOG.info("Starting ApplicationMaster");

    // Connect to ResourceManager
    resourceManager = yarnClient.connectToRM();

    // Register self with ResourceManager
    RegisterApplicationMasterResponse response;
    try {
      response = registerToRM();
    }
    catch (YarnRemoteException ex) {
      throw new RuntimeException(ex);
    }
    // Dump out information about cluster capability as seen by the resource manager
    int minMem = response.getMinimumResourceCapability().getMemory();
    int maxMem = response.getMaximumResourceCapability().getMemory();
    LOG.info("Min mem capabililty of resources in this cluster " + minMem);
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

    // A resource ask has to be atleast the minimum of the capability of the cluster, the value has to be
    // a multiple of the min value and cannot exceed the max.
    // If it is not an exact multiple of min, the RM will allocate to the nearest multiple of min
    int containerMemory = dag.getContainerMemoryMB();
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
    stats.containerMemory = containerMemory;

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
    int nextRequestPriority = 0;

    while (!appDone) {
      loopCounter++;

      // log current state
      /*
       LOG.info("Current application state: loop=" + loopCounter
       + ", appDone=" + appDone
       + ", total=" + numTotalContainers
       + ", requested=" + numRequestedContainers
       + ", completed=" + numCompletedContainers
       + ", failed=" + numFailedContainers
       + ", currentAllocated=" + this.allAllocatedContainers.size());
       */
      // Sleep before each loop when asking RM for containers
      // to avoid flooding RM with spurious requests when it
      // need not have any available containers
      try {
        sleep(1000);
      }
      catch (InterruptedException e) {
        LOG.info("Sleep interrupted " + e.getMessage());
      }

      // Setup request to be sent to RM to allocate containers
      List<ResourceRequest> resourceReq = new ArrayList<ResourceRequest>();
      // request containers for pending deploy requests
      if (!dnmgr.containerStartRequests.isEmpty()) {
        StramChildAgent.ContainerStartRequest csr;
        while ((csr = dnmgr.containerStartRequests.poll()) != null) {
          csr.container.setResourceRequestPriority(nextRequestPriority++);
          ResourceRequest containerAsk = setupContainerAskForRM(csr.container.getResourceRequestPriority(), 1, containerMemory);
          resourceReq.add(containerAsk);
          numTotalContainers++;
          numRequestedContainers++;
        }
      }

      AMResponse amResp;
      try {
        amResp = sendContainerAskToRM(resourceReq, releasedContainers);
      }
      catch (YarnRemoteException ex) {
        throw new RuntimeException(ex);
      }
      releasedContainers.clear();

      // Retrieve list of allocated containers from the response
      List<Container> newAllocatedContainers = amResp.getAllocatedContainers();
      //LOG.info("Got response from RM for container ask, allocatedCnt=" + newAllocatedContainers.size());
      numRequestedContainers -= newAllocatedContainers.size();
      for (Container allocatedContainer : newAllocatedContainers) {
        LOG.info("Got new container."
                + ", containerId=" + allocatedContainer.getId()
                + ", containerNode=" + allocatedContainer.getNodeId()
                + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
                + ", containerState" + allocatedContainer.getState()
                + ", containerResourceMemory" + allocatedContainer.getResource().getMemory()
                + ", priority" + allocatedContainer.getPriority());
        //+ ", containerToken" + allocatedContainer.getContainerToken().getIdentifier().toString());

        // allocate resource to container
        ContainerResource resource = new ContainerResource(allocatedContainer.getPriority().getPriority(), allocatedContainer.getId().toString(), allocatedContainer.getNodeId().getHost(), allocatedContainer.getResource().getMemory());
        StramChildAgent sca = dnmgr.assignContainer(resource, null);
        if (sca == null) {
          // allocated container no longer needed, add release request
          LOG.warn("Container {} allocated but nothing to deploy, going to release this container.", allocatedContainer.getId());
          releasedContainers.add(allocatedContainer.getId());
        }
        else {
          this.allAllocatedContainers.put(allocatedContainer.getId().toString(), allocatedContainer);
          // launch and start the container on a separate thread to keep the main thread unblocked
          LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, yarnClient, dag, delegationTokenManager, rpcImpl.getAddress());
          Thread launchThread = new Thread(runnableLaunchContainer);
          launchThreads.add(launchThread);
          launchThread.start();
        }
      }

      // TODO: we need to obtain the initial list...
      // keep track of updated operators - we use this info to make decisions about where to request new containers
      List<NodeReport> nodeReports = amResp.getUpdatedNodes();
      //LOG.debug("Got {} updated node reports.", nodeReports.size());
      for (NodeReport nr : nodeReports) {
        StringBuilder sb = new StringBuilder();
        sb.append("rackName=").append(nr.getRackName()).append("nodeid=").append(nr.getNodeId()).append("numContainers=").append(nr.getNumContainers()).append("capability=").append(nr.getCapability()).append("used=").append(nr.getUsed()).append("state=").append(nr.getNodeState());
        LOG.info("Node report: " + sb);
      }

      // Check what the current available resources in the cluster are
      //Resource availableResources = amResp.getAvailableResources();
      //LOG.debug("Current available resources in the cluster " + availableResources);

      // Check the completed containers
      List<ContainerStatus> completedContainers = amResp.getCompletedContainersStatuses();
      //LOG.debug("Got response from RM for container ask, completedCnt=" + completedContainers.size());
      for (ContainerStatus containerStatus : completedContainers) {
        LOG.info("Got container status for containerID= " + containerStatus.getContainerId()
                + ", state=" + containerStatus.getState()
                + ", exitStatus=" + containerStatus.getExitStatus()
                + ", diagnostics=" + containerStatus.getDiagnostics());

        // non complete containers should not be here
        assert (containerStatus.getState() == ContainerState.COMPLETE);
        Container allocatedContainer = allAllocatedContainers.remove(containerStatus.getContainerId().toString());

        // increment counters for completed/failed containers
        int exitStatus = containerStatus.getExitStatus();
        LOG.info("Container {} exit status {}.", containerStatus.getContainerId(), exitStatus);
        if (0 != exitStatus) {
          if (allocatedContainer != null) {
            numFailedContainers.incrementAndGet();
          }
          if (exitStatus == 1) {
            // StramChild failure
            appDone = true;
            dnmgr.shutdownDiagnosticsMessage = "Unrecoverable failure " + containerStatus.getContainerId();
            LOG.info("Exiting due to: {}", dnmgr.shutdownDiagnosticsMessage);
          }
          else {
            // Recoverable failure or process killed (externally or via stop request by AM)
            LOG.info("Container {} failed or killed.", containerStatus.getContainerId());
            dnmgr.scheduleContainerRestart(containerStatus.getContainerId().toString());
          }
        }
        else {
          // container completed successfully
          numCompletedContainers.incrementAndGet();
          dnmgr.markComplete(containerStatus.getContainerId().toString());
          LOG.info("Container completed successfully."
                  + ", containerId=" + containerStatus.getContainerId());
        }

      }

      if (allAllocatedContainers.isEmpty() && numRequestedContainers == 0) {
        appDone = true;
      }

      LOG.debug("Current application state: loop=" + loopCounter
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
        LOG.info("Exception thrown in thread join", e);
      }
    }

    // When the application completes, it should send a finish application signal
    // to the RM
    LOG.info("Application completed. Signalling finish to RM");

    FinishApplicationMasterRequest finishReq = Records.newRecord(FinishApplicationMasterRequest.class);
    finishReq.setAppAttemptId(appAttemptID);
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
      if (!StringUtils.isEmpty(dnmgr.shutdownDiagnosticsMessage)) {
        diagnostics += "\n";
        diagnostics += dnmgr.shutdownDiagnosticsMessage;
      }
      // YARN-208 - as of 2.0.1-alpha dropped by the RM
      finishReq.setDiagnostics(diagnostics);
      // return true to indicates expected termination of the master process
      // application status and diagnostics message are set above
      status = true;
    }
    LOG.info("diagnostics: " + finishReq.getDiagnostics());
    try {
      resourceManager.finishApplicationMaster(finishReq);
    }
    catch (YarnRemoteException ex) {
      throw new RuntimeException(ex);
    }
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
    appMasterRequest.setApplicationAttemptId(appAttemptID);
    appMasterRequest.setHost(appMasterHostname);
    appMasterRequest.setRpcPort(0);
    appMasterRequest.setTrackingUrl(appMasterTrackingUrl);
    return resourceManager.registerApplicationMaster(appMasterRequest);
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   *
   * @param numContainers Containers to ask for from RM
   * @return the setup ResourceRequest to be sent to RM
   */
  private ResourceRequest setupContainerAskForRM(int requestPriority, int numContainers, int containerMemory)
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
    // Send the request to RM
    if (requestedContainers.size() > 0) {
      LOG.info("Asking RM for containers" + ", askCount=" + requestedContainers.size());
    }

    for (String containerIdStr : dnmgr.containerStopRequests.values()) {
      Container allocatedContainer = this.allAllocatedContainers.get(containerIdStr);
      if (allocatedContainer != null) {
        // issue stop container - TODO: separate thread to not block heartbeat
        ContainerManager cm = yarnClient.connectToCM(allocatedContainer);
        StopContainerRequest stopContainer = Records.newRecord(StopContainerRequest.class);
        stopContainer.setContainerId(allocatedContainer.getId());
        cm.stopContainer(stopContainer);
        LOG.info("Stopped container {}", containerIdStr);
      }
      dnmgr.containerStopRequests.remove(containerIdStr);
    }

    req.addAllReleases(releasedContainers);
    //req.setProgress((float) numCompletedContainers.get() / numTotalContainers);

    //LOG.info("Sending request to RM for containers"
    //         + ", requestedSet=" + requestedContainers.size()
    //         + ", releasedSet=" + releasedContainers.size()
    //         + ", progress=" + req.getProgress());
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
