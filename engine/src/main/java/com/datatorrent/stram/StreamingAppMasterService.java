/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.api.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;

import javax.xml.bind.annotation.XmlElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;

import com.datatorrent.stram.StreamingContainerManager.ContainerResource;
import com.datatorrent.stram.api.BaseContext;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.license.License;
import com.datatorrent.stram.license.LicensingAgentClient;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.OperatorStatus.PortStatus;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.security.StramDelegationTokenManager;
import com.datatorrent.stram.security.StramWSFilterInitializer;
import com.datatorrent.stram.webapp.AppInfo;
import com.datatorrent.stram.webapp.StramWebApp;
import com.google.common.collect.Maps;

/**
 * Streaming Application Master
 *
 * @since 0.3.2
 */
public class StreamingAppMasterService extends CompositeService
{
  private static final Logger LOG = LoggerFactory.getLogger(StreamingAppMasterService.class);
  private static final long DELEGATION_KEY_UPDATE_INTERVAL = 24 * 60 * 60 * 1000;
  private static final long DELEGATION_TOKEN_MAX_LIFETIME = 365 * 24 * 60 * 60 * 1000;
  private static final long DELEGATION_TOKEN_RENEW_INTERVAL = 365 * 24 * 60 * 60 * 1000;
  private static final long DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL = 24 * 60 * 60 * 1000;
  private static final int NUMBER_MISSED_HEARTBEATS = 30;
  private AMRMClient<ContainerRequest> amRmClient;
  private NMClientAsync nmClient;
  private LogicalPlan dag;
  // Application Attempt Id ( combination of attemptId and fail count )
  final private ApplicationAttemptId appAttemptID;
  // Hostname of the container
  private final String appMasterHostname = "";
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = "";
  // Simple flag to denote whether all works is done
  private boolean appDone = false;
  // Counter for completed containers ( complete denotes successful or failed )
  private final AtomicInteger numCompletedContainers = new AtomicInteger();
  // Containers that the RM has allocated to us
  private final ConcurrentMap<String, AllocatedContainer> allocatedContainers = Maps.newConcurrentMap();
  // Count of failed containers
  private final AtomicInteger numFailedContainers = new AtomicInteger();
  private final ConcurrentLinkedQueue<Runnable> pendingTasks = new ConcurrentLinkedQueue<Runnable>();
  // child container callback
  private StreamingContainerParent heartbeatListener;
  private StreamingContainerManager dnmgr;
  private final Clock clock = new SystemClock();
  private final long startTime = clock.getTime();
  private final ClusterAppStats stats = new ClusterAppStats();
  private StramDelegationTokenManager delegationTokenManager = null;
  private LicensingAgentClient licenseClient;

  public StreamingAppMasterService(ApplicationAttemptId appAttemptID)
  {
    super(StreamingAppMasterService.class.getName());
    this.appAttemptID = appAttemptID;
  }

  /**
   * Overrides getters to pull live info.
   */
  protected class ClusterAppStats extends AppInfo.AppStats
  {
    @Override
    public int getAllocatedContainers()
    {
      return allocatedContainers.size();
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
      return dnmgr.getPhysicalPlan().getAllOperators().size();
    }

    @Override
    public long getCurrentWindowId()
    {
      long min = Long.MAX_VALUE;
      for (Map.Entry<Integer, PTOperator> entry : dnmgr.getPhysicalPlan().getAllOperators().entrySet()) {
        long windowId = entry.getValue().stats.currentWindowId.get();
        if (min > windowId) {
          min = windowId;
        }
      }
      return StreamingContainerManager.toWsWindowId(min == Long.MAX_VALUE ? 0 : min);
    }

    @Override
    public long getRecoveryWindowId()
    {
      return StreamingContainerManager.toWsWindowId(dnmgr.getCommittedWindowId());
    }

    @Override
    public long getTuplesProcessedPSMA()
    {
      long result = 0;
      for (Map.Entry<Integer, PTOperator> entry : dnmgr.getPhysicalPlan().getAllOperators().entrySet()) {
        result += entry.getValue().stats.tuplesProcessedPSMA.get();
      }
      return result;
    }

    @Override
    public long getTotalTuplesProcessed()
    {
      long result = 0;
      for (Map.Entry<Integer, PTOperator> entry : dnmgr.getPhysicalPlan().getAllOperators().entrySet()) {
        result += entry.getValue().stats.totalTuplesProcessed.get();
      }
      return result;
    }

    @Override
    public long getTuplesEmittedPSMA()
    {
      long result = 0;
      for (Map.Entry<Integer, PTOperator> entry : dnmgr.getPhysicalPlan().getAllOperators().entrySet()) {
        result += entry.getValue().stats.tuplesEmittedPSMA.get();
      }
      return result;
    }

    @Override
    public long getTotalTuplesEmitted()
    {
      long result = 0;
      for (Map.Entry<Integer, PTOperator> entry : dnmgr.getPhysicalPlan().getAllOperators().entrySet()) {
        result += entry.getValue().stats.totalTuplesEmitted.get();
      }
      return result;
    }

    @Override
    public long getTotalMemoryAllocated()
    {
      long result = 0;
      for (PTContainer c : dnmgr.getPhysicalPlan().getContainers()) {
        result += c.getAllocatedMemoryMB();
      }
      return result;
    }

    @Override
    public long getTotalBufferServerReadBytesPSMA()
    {
      long result = 0;
      for (Map.Entry<Integer, PTOperator> entry : dnmgr.getPhysicalPlan().getAllOperators().entrySet()) {
        for (Map.Entry<String, PortStatus> portEntry : entry.getValue().stats.inputPortStatusList.entrySet()) {
          result += portEntry.getValue().bufferServerBytesPMSMA.getAvg() * 1000;
        }
      }
      return result;
    }

    @Override
    public long getTotalBufferServerWriteBytesPSMA()
    {
      long result = 0;
      for (Map.Entry<Integer, PTOperator> entry : dnmgr.getPhysicalPlan().getAllOperators().entrySet()) {
        for (Map.Entry<String, PortStatus> portEntry : entry.getValue().stats.outputPortStatusList.entrySet()) {
          result += portEntry.getValue().bufferServerBytesPMSMA.getAvg() * 1000;
        }
      }
      return result;
    }

    @Override
    public List<Integer> getCriticalPath()
    {
      StreamingContainerManager.CriticalPathInfo criticalPathInfo = dnmgr.getCriticalPathInfo();
      return (criticalPathInfo == null) ? null : criticalPathInfo.path;
    }

    @Override
    public long getLatency()
    {
      StreamingContainerManager.CriticalPathInfo criticalPathInfo = dnmgr.getCriticalPathInfo();
      return (criticalPathInfo == null) ? 0 : criticalPathInfo.latency;
    }

  }

  private class ClusterAppContextImpl extends BaseContext implements StramAppContext
  {
    private ClusterAppContextImpl()
    {
      super(null, null);
    }

    ClusterAppContextImpl(AttributeMap attributes)
    {
      super(attributes, null);
    }

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
      return getValue(LogicalPlan.APPLICATION_NAME);
    }

    @Override
    public String getApplicationDocLink()
    {
      return getValue(LogicalPlan.APPLICATION_DOC_LINK);
    }

    @Override
    public long getStartTime()
    {
      return startTime;
    }

    @Override
    public String getApplicationPath()
    {
      return getValue(LogicalPlan.APPLICATION_PATH);
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
    public String getGatewayAddress()
    {
      return getValue(LogicalPlan.GATEWAY_CONNECT_ADDRESS);
    }

    @Override
    public String getLicenseId()
    {
      if (StreamingAppMasterService.this.licenseClient != null) {
        return StreamingAppMasterService.this.licenseClient.getLicenseId();
      }
      return "";
    }

    @Override
    public long getRemainingLicensedMB()
    {
      if (StreamingAppMasterService.this.licenseClient != null) {
        return StreamingAppMasterService.this.licenseClient.getRemainingLicensedMB();
      }
      return 0;
    }

    @Override
    public long getTotalLicensedMB()
    {
      if (StreamingAppMasterService.this.licenseClient != null) {
        return StreamingAppMasterService.this.licenseClient.getTotalLicensedMB();
      }
      return 0;
    }

    @Override
    public long getAllocatedMB()
    {
      if (StreamingAppMasterService.this.licenseClient != null) {
        return StreamingAppMasterService.this.licenseClient.getAllocatedMB();
      }
      return 0;
    }

    @Override
    public long getLicenseInfoLastUpdate()
    {
      if (StreamingAppMasterService.this.licenseClient != null) {
        return StreamingAppMasterService.this.licenseClient.getLicenseInfoLastUpdate();
      }
      return 0;
    }

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 201309112304L;
  }

  /**
   * Dump out contents of $CWD and the environment to stdout for debugging
   */
  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public void dumpOutDebugInfo()
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
      try {
        while ((line = buf.readLine()) != null) {
          LOG.info("System CWD content: " + line);
        }
        LOG.info("Dumping files in local dir: end");
      }
      finally {
        buf.close();
      }
    }
    catch (IOException e) {
      LOG.debug("Exception", e);
    }
    catch (InterruptedException e) {
      LOG.info("Interrupted", e);
    }

    LOG.info("Classpath: {}", System.getProperty("java.class.path"));
    LOG.info("Config resources: {}", getConfig().toString());
    try {
      // find a better way of logging this using the logger.
      Configuration.dumpConfiguration(getConfig(), new PrintWriter(System.out));
    }
    catch (Exception e) {
      LOG.error("Error dumping configuration.", e);
    }

  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception
  {
    LOG.info("Application master" + ", appId=" + appAttemptID.getApplicationId().getId() + ", clustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp() + ", attemptId=" + appAttemptID.getAttemptId());

    FileInputStream fis = new FileInputStream("./" + LogicalPlan.SER_FILE_NAME);
    try {
      this.dag = LogicalPlan.read(fis);
    }
    finally {
      fis.close();
    }
    // "debug" simply dumps all data using LOG.info
    if (dag.isDebug()) {
      dumpOutDebugInfo();
    }

    FSRecoveryHandler recoveryHandler = new FSRecoveryHandler(dag.assertAppPath(), conf);
    this.dnmgr = StreamingContainerManager.getInstance(recoveryHandler, dag, true);
    dag = this.dnmgr.getLogicalPlan();

    Map<Class<?>, Class<? extends StringCodec<?>>> codecs = dag.getAttributes().get(DAG.STRING_CODECS);
    if (codecs != null) {
      LOG.debug("LOADING CONVERTERS {}", Thread.currentThread().getId());
      StringCodecs.loadConverters(codecs);
    }

    LOG.info("Starting application with {} operators in {} containers", dnmgr.getPhysicalPlan().getAllOperators().size(), dnmgr.getPhysicalPlan().getContainers().size());

    if (UserGroupInformation.isSecurityEnabled()) {
      // TODO :- Need to perform token renewal
      delegationTokenManager = new StramDelegationTokenManager(DELEGATION_KEY_UPDATE_INTERVAL, DELEGATION_TOKEN_MAX_LIFETIME, DELEGATION_TOKEN_RENEW_INTERVAL, DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL);
    }
    this.nmClient = new NMClientAsyncImpl(new NMCallbackHandler());
    addService(nmClient);
    this.amRmClient = AMRMClient.createAMRMClient();
    addService(amRmClient);

    // start RPC server
    int rpcListenerCount = dag.getValue(DAGContext.HEARTBEAT_LISTENER_THREAD_COUNT);
    this.heartbeatListener = new StreamingContainerParent(this.getClass().getName(), dnmgr, delegationTokenManager, rpcListenerCount);
    addService(heartbeatListener);

    // get license and prepare for license agent interaction
    String licenseBase64 = dag.getValue(LogicalPlan.LICENSE);
    if (licenseBase64 != null) {
      byte[] licenseBytes = Base64.decodeBase64(licenseBase64);
      String licenseId = License.getLicenseID(licenseBytes);
      this.licenseClient = new LicensingAgentClient(appAttemptID.getApplicationId(), licenseId);
      addService(this.licenseClient);
    }

    // initialize all services added above
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception
  {
    super.serviceStart();
    if (delegationTokenManager != null) {
      delegationTokenManager.startThreads();
    }

    // write the connect address for containers to DFS
    InetSocketAddress connectAddress = NetUtils.getConnectAddress(this.heartbeatListener.getAddress());
    URI connectUri = new URI("stram", null, connectAddress.getHostName(), connectAddress.getPort(), null, null, null);
    FSRecoveryHandler recoveryHandler = new FSRecoveryHandler(dag.assertAppPath(), getConfig());
    recoveryHandler.writeConnectUri(connectUri.toString());

    // start web service
    StramAppContext appContext = new ClusterAppContextImpl(dag.getAttributes());
    try {
      org.mortbay.log.Log.setLog(null);
      Configuration config = getConfig();
      if (UserGroupInformation.isSecurityEnabled()) {
        config = new Configuration(config);
        config.set("hadoop.http.filter.initializers", StramWSFilterInitializer.class.getCanonicalName());
      }
      WebApp webApp = WebApps.$for("stram", StramAppContext.class, appContext, "ws").with(config).start(new StramWebApp(this.dnmgr));
      LOG.info("Started web service at port: " + webApp.port());
      this.appMasterTrackingUrl = NetUtils.getConnectAddress(webApp.getListenerAddress()).getHostName() + ":" + webApp.port();
      LOG.info("Setting tracking URL to: " + appMasterTrackingUrl);
    }
    catch (Exception e) {
      LOG.error("Webapps failed to start. Ignoring for now:", e);
    }
  }

  @Override
  protected void serviceStop() throws Exception
  {
    super.serviceStop();
    if (delegationTokenManager != null) {
      delegationTokenManager.stopThreads();
    }
    nmClient.stop();
    amRmClient.stop();
    dnmgr.teardown();
  }

  public boolean run() throws Exception
  {
    boolean status = true;
    try {
      StramChild.eventloop.start();
      execute();
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
  @SuppressWarnings("SleepWhileInLoop")
  private void execute() throws YarnException, IOException
  {
    LOG.info("Starting ApplicationMaster");

    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    LOG.info("number of tokens: {}", credentials.getAllTokens().size());
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      LOG.debug("token: " + token);
    }

    // Register self with ResourceManager
    RegisterApplicationMasterResponse response = amRmClient.registerApplicationMaster(appMasterHostname, 0, appMasterTrackingUrl);

    // Dump out information about cluster capability as seen by the resource manager
    int maxMem = response.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

    int containerMemory = dag.getContainerMemoryMB();
    if (containerMemory > maxMem) {
      LOG.info("Container memory specified above max threshold of cluster. Using max value." + ", specified=" + containerMemory + ", max=" + maxMem);
      containerMemory = maxMem;
    }

    // for locality relaxation fall back
    Map<StramChildAgent.ContainerStartRequest, Integer> requestedResources = Maps.newHashMap();

    // Setup heartbeat emitter
    // TODO poll RM every now and then with an empty request to let RM know that we are alive
    // The heartbeat interval after which an AM is timed out by the RM is defined by a config setting:
    // RM_AM_EXPIRY_INTERVAL_MS with default defined by DEFAULT_RM_AM_EXPIRY_INTERVAL_MS
    // The allocate calls to the RM count as heartbeat so, for now, this additional heartbeat emitter
    // is not required.

    int loopCounter = -1;
    List<ContainerId> releasedContainers = new ArrayList<ContainerId>();
    int numTotalContainers = 0;
    // keep track of already requested containers to not request them again while waiting for allocation
    int numRequestedContainers = 0;
    int numReleasedContainers = 0;
    int nextRequestPriority = 0;
    ResourceRequestHandler resourceRequestor = new ResourceRequestHandler();

    try {
      // YARN-435
      // we need getClusterNodes to populate the initial node list,
      // subsequent updates come through the heartbeat response
      YarnClient clientRMService = YarnClient.createYarnClient();
      clientRMService.init(getConfig());
      clientRMService.start();
      resourceRequestor.updateNodeReports(clientRMService.getNodeReports());
      clientRMService.stop();
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to retrieve cluster nodes report.", e);
    }

    // check for previously allocated containers
    // as of 2.2, containers won't survive AM restart, but this will change in the future - YARN-1490
    checkContainerStatus();
    FinalApplicationStatus finalStatus = FinalApplicationStatus.SUCCEEDED;
    int availableLicensedMemory = (licenseClient != null) ? 0 : Integer.MAX_VALUE;

    while (!appDone) {
      loopCounter++;

      Runnable r;
      while ((r = this.pendingTasks.poll()) != null) {
        r.run();
      }

      // log current state
      /*
       * LOG.info("Current application state: loop=" + loopCounter + ", appDone=" + appDone + ", total=" +
       * numTotalContainers + ", requested=" + numRequestedContainers + ", completed=" + numCompletedContainers +
       * ", failed=" + numFailedContainers + ", currentAllocated=" + this.allAllocatedContainers.size());
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
      List<ContainerRequest> containerRequests = new ArrayList<ContainerRequest>();
      // request containers for pending deploy requests
      if (!dnmgr.containerStartRequests.isEmpty()) {
        boolean requestResources = true;
        if (licenseClient != null) {
          // ensure enough memory is left to request new container
          licenseClient.reportAllocatedMemory((int) stats.getTotalMemoryAllocated());
          availableLicensedMemory = licenseClient.getRemainingEnforcementMB();
          int requiredMemory = dnmgr.containerStartRequests.size() * containerMemory;
          if (requiredMemory > availableLicensedMemory) {
            LOG.warn("Insufficient licensed memory to request resources required {}m available {}m", requiredMemory, availableLicensedMemory);
            requestResources = false;
          }
        }
        if (requestResources) {
          StramChildAgent.ContainerStartRequest csr;
          while ((csr = dnmgr.containerStartRequests.poll()) != null) {
            csr.container.setResourceRequestPriority(nextRequestPriority++);
            requestedResources.put(csr, loopCounter);
            containerRequests.add(resourceRequestor.createContainerRequest(csr, containerMemory, true));
          }
        }
      }

      if (!requestedResources.isEmpty()) {
        //resourceRequestor.clearNodeMapping();
        for (Map.Entry<StramChildAgent.ContainerStartRequest, Integer> entry : requestedResources.entrySet()) {
          if ((loopCounter - entry.getValue()) > NUMBER_MISSED_HEARTBEATS) {
            entry.setValue(loopCounter);
            StramChildAgent.ContainerStartRequest csr = entry.getKey();
            containerRequests.add(resourceRequestor.createContainerRequest(csr, containerMemory, false));
          }
        }
      }

      numTotalContainers += containerRequests.size();
      numRequestedContainers += containerRequests.size();
      AllocateResponse amResp = sendContainerAskToRM(containerRequests, releasedContainers);
      if (amResp.getAMCommand() != null) {
        LOG.info(" statement executed:{}", amResp.getAMCommand());
        switch (amResp.getAMCommand()) {
          case AM_RESYNC:
          case AM_SHUTDOWN:
            throw new YarnRuntimeException("Received the " + amResp.getAMCommand() + " command from RM");
          default:
            throw new YarnRuntimeException("Received the " + amResp.getAMCommand() + " command from RM");

        }
      }
      releasedContainers.clear();

      // CDH reporting incorrect resources, see SPOI-1846, YARN-1959. Workaround for now.
      //int availableMemory = Math.min(amResp.getAvailableResources().getMemory(), availableLicensedMemory);
      int availableMemory = availableLicensedMemory;
      dnmgr.getPhysicalPlan().setAvailableResources(availableMemory);

      // Retrieve list of allocated containers from the response
      List<Container> newAllocatedContainers = amResp.getAllocatedContainers();
      // LOG.info("Got response from RM for container ask, allocatedCnt=" + newAllocatedContainers.size());
      numRequestedContainers -= newAllocatedContainers.size();
      long timestamp = System.currentTimeMillis();
      for (Container allocatedContainer : newAllocatedContainers) {

        LOG.info("Got new container." + ", containerId=" + allocatedContainer.getId() + ", containerNode=" + allocatedContainer.getNodeId() + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress() + ", containerResourceMemory" + allocatedContainer.getResource().getMemory() + ", priority" + allocatedContainer.getPriority());
        // + ", containerToken" + allocatedContainer.getContainerToken().getIdentifier().toString());

        boolean alreadyAllocated = true;
        StramChildAgent.ContainerStartRequest csr = null;
        for (Map.Entry<StramChildAgent.ContainerStartRequest, Integer> entry : requestedResources.entrySet()) {
          if (entry.getKey().container.getResourceRequestPriority() == allocatedContainer.getPriority().getPriority()) {
            alreadyAllocated = false;
            csr = entry.getKey();
            break;
          }
        }

        if (alreadyAllocated) {
          LOG.info("Releasing {} as resource with priority {} was already assigned", allocatedContainer.getId(), allocatedContainer.getPriority());
          releasedContainers.add(allocatedContainer.getId());
          numReleasedContainers++;
          numRequestedContainers++;
          continue;
        }
        if (csr != null) {
          requestedResources.remove(csr);
        }

        // allocate resource to container
        ContainerResource resource = new ContainerResource(allocatedContainer.getPriority().getPriority(), allocatedContainer.getId().toString(), allocatedContainer.getNodeId().toString(), allocatedContainer.getResource().getMemory(), allocatedContainer.getNodeHttpAddress());
        StramChildAgent sca = dnmgr.assignContainer(resource, null);

        if (sca == null) {
          // allocated container no longer needed, add release request
          LOG.warn("Container {} allocated but nothing to deploy, going to release this container.", allocatedContainer.getId());
          releasedContainers.add(allocatedContainer.getId());
        }
        else {
          this.allocatedContainers.put(allocatedContainer.getId().toString(), new AllocatedContainer(allocatedContainer));
          ByteBuffer tokens = LaunchContainerRunnable.getTokens(delegationTokenManager, heartbeatListener.getAddress());
          LaunchContainerRunnable launchContainer = new LaunchContainerRunnable(allocatedContainer, nmClient, dag, tokens);
          // Thread launchThread = new Thread(runnableLaunchContainer);
          // launchThreads.add(launchThread);
          // launchThread.start();
          launchContainer.run(); // communication with NMs is now async

          // record container start event
          StramEvent ev = new StramEvent.StartContainerEvent(allocatedContainer.getId().toString(), allocatedContainer.getNodeId().toString());
          ev.setTimestamp(timestamp);
          dnmgr.recordEventAsync(ev);
        }
      }

      // track node updates for future locality constraint allocations
      // TODO: it seems 2.0.4-alpha doesn't give us any updates
      resourceRequestor.updateNodeReports(amResp.getUpdatedNodes());

      // Check the completed containers
      List<ContainerStatus> completedContainers = amResp.getCompletedContainersStatuses();
      // LOG.debug("Got response from RM for container ask, completedCnt=" + completedContainers.size());
      for (ContainerStatus containerStatus : completedContainers) {
        LOG.info("Completed containerId=" + containerStatus.getContainerId() + ", state=" + containerStatus.getState() + ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics=" + containerStatus.getDiagnostics());

        // non complete containers should not be here
        assert (containerStatus.getState() == ContainerState.COMPLETE);

        AllocatedContainer allocatedContainer = allocatedContainers.remove(containerStatus.getContainerId().toString());
        int exitStatus = containerStatus.getExitStatus();
        if (0 != exitStatus) {
          if (allocatedContainer != null) {
            numFailedContainers.incrementAndGet();
          }
//          if (exitStatus == 1) {
//            // non-recoverable StramChild failure
//            appDone = true;
//            finalStatus = FinalApplicationStatus.FAILED;
//            dnmgr.shutdownDiagnosticsMessage = "Unrecoverable failure " + containerStatus.getContainerId();
//            LOG.info("Exiting due to: {}", dnmgr.shutdownDiagnosticsMessage);
//          }
//          else {
          // Recoverable failure or process killed (externally or via stop request by AM)
          // also occurs when a container was released by the application but never assigned/launched
          LOG.debug("Container {} failed or killed.", containerStatus.getContainerId());
          dnmgr.scheduleContainerRestart(containerStatus.getContainerId().toString());
//          }
        }
        else {
          // container completed successfully
          numCompletedContainers.incrementAndGet();
          LOG.info("Container completed successfully." + ", containerId=" + containerStatus.getContainerId());
        }

        String containerIdStr = containerStatus.getContainerId().toString();
        dnmgr.removeContainerAgent(containerIdStr);

        // record container stop event
        StramEvent ev = new StramEvent.StopContainerEvent(containerIdStr, containerStatus.getExitStatus());
        ev.setReason(containerStatus.getDiagnostics());
        dnmgr.recordEventAsync(ev);
      }

      if (licenseClient != null) {
        if (!(amResp.getCompletedContainersStatuses().isEmpty() && amResp.getAllocatedContainers().isEmpty())) {
          // update license agent on allocated container changes
          licenseClient.reportAllocatedMemory((int) stats.getTotalMemoryAllocated());
        }
        availableLicensedMemory = licenseClient.getRemainingEnforcementMB();
      }

      if (dnmgr.forcedShutdown) {
        LOG.info("Forced shutdown due to {}", dnmgr.shutdownDiagnosticsMessage);
        finalStatus = FinalApplicationStatus.FAILED;
        appDone = true;
      }
      else if (allocatedContainers.isEmpty() && numRequestedContainers == 0 && dnmgr.containerStartRequests.isEmpty()) {
        LOG.debug("Exiting as no more containers are allocated or requested");
        finalStatus = FinalApplicationStatus.SUCCEEDED;
        appDone = true;
      }

      LOG.debug("Current application state: loop=" + loopCounter + ", appDone=" + appDone + ", total=" + numTotalContainers + ", requested=" + numRequestedContainers + ", released=" + numReleasedContainers + ", completed=" + numCompletedContainers + ", failed=" + numFailedContainers + ", currentAllocated=" + allocatedContainers.size());

      // monitor child containers
      dnmgr.monitorHeartbeat();
    }

    LOG.info("Application completed. Signalling finish to RM");
    FinishApplicationMasterRequest finishReq = Records.newRecord(FinishApplicationMasterRequest.class);
    finishReq.setFinalApplicationStatus(finalStatus);

    if (finalStatus != FinalApplicationStatus.SUCCEEDED) {
      String diagnostics = "Diagnostics." + ", total=" + numTotalContainers + ", completed=" + numCompletedContainers.get() + ", allocated=" + allocatedContainers.size() + ", failed=" + numFailedContainers.get();
      if (!StringUtils.isEmpty(dnmgr.shutdownDiagnosticsMessage)) {
        diagnostics += "\n";
        diagnostics += dnmgr.shutdownDiagnosticsMessage;
      }
      // YARN-208 - as of 2.0.1-alpha dropped by the RM
      finishReq.setDiagnostics(diagnostics);
      // expected termination of the master process
      // application status and diagnostics message are set above
    }
    LOG.info("diagnostics: " + finishReq.getDiagnostics());
    amRmClient.unregisterApplicationMaster(finishReq.getFinalApplicationStatus(), finishReq.getDiagnostics(), null);
  }

  /**
   * Check for containers that were allocated in a previous attempt.
   * If the containers are still alive, wait for them to check in via heartbeat.
   */
  private void checkContainerStatus()
  {
    Collection<StramChildAgent> containers = this.dnmgr.getContainerAgents();
    for (StramChildAgent ca : containers) {
      ContainerId containerId = ConverterUtils.toContainerId(ca.container.getExternalId());
      NodeId nodeId = ConverterUtils.toNodeId(ca.container.host);

      // put container back into the allocated list
      org.apache.hadoop.yarn.api.records.Token containerToken = null;
      Resource resource = Resource.newInstance(ca.container.getAllocatedMemoryMB(), 1);
      Priority priority = Priority.newInstance(ca.container.getResourceRequestPriority());
      Container yarnContainer = Container.newInstance(containerId, nodeId, ca.container.nodeHttpAddress, resource, priority, containerToken);
      this.allocatedContainers.put(containerId.toString(), new AllocatedContainer(yarnContainer));

      // check the status
      nmClient.getContainerStatusAsync(containerId, nodeId);
    }
  }

  /**
   * Ask RM to allocate given no. of containers to this Application Master
   *
   * @param requestedContainers Containers to ask for from RM
   * @return Response from RM to AM with allocated containers
   * @throws YarnRemoteException
   */
  private AllocateResponse sendContainerAskToRM(List<ContainerRequest> containerRequests, List<ContainerId> releasedContainers) throws YarnException, IOException
  {
    if (containerRequests.size() > 0) {
      LOG.info("Asking RM for containers: " + containerRequests);
      for (ContainerRequest cr : containerRequests) {
        LOG.info("Requested container: {}", cr.toString());
        amRmClient.addContainerRequest(cr);
      }
    }

    for (ContainerId containerId : releasedContainers) {
      LOG.info("Released container, id={}", containerId.getId());
      amRmClient.releaseAssignedContainer(containerId);
    }

    for (String containerIdStr : dnmgr.containerStopRequests.values()) {
      AllocatedContainer allocatedContainer = this.allocatedContainers.get(containerIdStr);
      if (allocatedContainer != null && !allocatedContainer.stopRequested) {
        nmClient.stopContainerAsync(allocatedContainer.container.getId(), allocatedContainer.container.getNodeId());
        LOG.info("Requested stop container {}", containerIdStr);
        allocatedContainer.stopRequested = true;
      }
      dnmgr.containerStopRequests.remove(containerIdStr);
    }

    return amRmClient.allocate(0);
  }

  private class NMCallbackHandler implements NMClientAsync.CallbackHandler
  {
    NMCallbackHandler()
    {
    }

    @Override
    public void onContainerStopped(ContainerId containerId)
    {
      LOG.debug("Succeeded to stop Container {}", containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus)
    {
      LOG.debug("Container Status: id={}, status={}", containerId, containerStatus);
      if (containerStatus.getState() != ContainerState.RUNNING) {
        recoverContainer(containerId);
      }
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse)
    {
      LOG.debug("Succeeded to start Container {}", containerId);
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t)
    {
      LOG.error("Start container failed for: containerId={}", containerId, t);
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t)
    {
      LOG.error("Failed to query the status of {}", containerId, t);
      // if the NM is not reachable, consider container lost and recover (occurs during AM recovery)
      recoverContainer(containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t)
    {
      LOG.warn("Failed to stop container {}", containerId, t);
      // container could not be stopped, we won't receive a stop event from AM heartbeat
      // short circuit and schedule recovery directly
      recoverContainer(containerId);
    }

    private void recoverContainer(final ContainerId containerId)
    {
      pendingTasks.add(new Runnable()
      {
        @Override
        public void run()
        {
          dnmgr.scheduleContainerRestart(containerId.toString());
          allocatedContainers.remove(containerId.toString());
        }

      });
    }

  }

  private class AllocatedContainer
  {
    final private Container container;
    private boolean stopRequested;

    private AllocatedContainer(Container c)
    {
      container = c;
    }
  }

}
