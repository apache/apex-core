/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import static java.lang.Thread.sleep;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAGContext;
import com.datatorrent.stram.debug.StdOutErrLog;
import com.datatorrent.stram.license.LicensingAgentProtocolImpl;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.security.StramDelegationTokenManager;
import com.datatorrent.stram.util.VersionInfo;

/**
 * Application master for licensing
 */
public class LicensingAppMaster extends CompositeService
{
  static {
    // set system properties so they can be used in logger configuration
    Map<String, String> envs = System.getenv();
    String containerIdString = envs.get(Environment.CONTAINER_ID.name());
    if (containerIdString == null) {
      // container id should always be set in the env by the framework
      throw new AssertionError("ContainerId not set in the environment");
    }
    System.setProperty("stram.cid", containerIdString);
  }

  private static final Logger LOG = LoggerFactory.getLogger(LicensingAppMaster.class);
  private static final long DELEGATION_KEY_UPDATE_INTERVAL = 24 * 60 * 60 * 1000;
  private static final long DELEGATION_TOKEN_MAX_LIFETIME = 7 * 24 * 60 * 60 * 1000;
  private static final long DELEGATION_TOKEN_RENEW_INTERVAL = 24 * 60 * 60 * 1000;
  private static final long DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL = 3600000;
  private AMRMClient<ContainerRequest> amRmClient;
  private NMClientAsync nmClient;
  private LogicalPlan dag;
  // Application Attempt Id ( combination of attemptId and fail count )
  private ApplicationAttemptId appAttemptID;
  // Hostname of the container
  private final String appMasterHostname = "";
  // Tracking url to which app master publishes info for clients to monitor
  private final String appMasterTrackingUrl = "";
  // Simple flag to denote whether all works is done
  private final boolean appDone = false;
  // Launch threads
  // child container callback
  private StramLocalCluster localCluster = null;
  private LicensingAgentProtocolImpl rpcListener;
  private StramDelegationTokenManager delegationTokenManager = null;

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
    LicensingAppMaster appMaster = null;

    try {
      appMaster = new LicensingAppMaster();
      LOG.info("Initializing ApplicationMaster");

      ContainerId containerId = ConverterUtils.toContainerId(System.getenv(Environment.CONTAINER_ID.name()));
      appMaster.appAttemptID = containerId.getApplicationAttemptId();

      Configuration conf = new YarnConfiguration();
      appMaster.init(conf);
      appMaster.start();
      result = appMaster.run();
    }
    catch (Throwable t) {
      LOG.error("Error running ApplicationMaster", t);
      System.exit(1);
    }
    finally {
      if (appMaster != null) {
        appMaster.stop();
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
    LOG.info("Config resources: {}", getConfig().toString());
    try {
      // find a better way of logging this using the logger.
      Configuration.dumpConfiguration(getConfig(), new PrintWriter(System.out));
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

  public LicensingAppMaster()
  {
    super(LicensingAppMaster.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception
  {
    LOG.info("Application master"
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

    this.localCluster = new StramLocalCluster(dag);

    if (UserGroupInformation.isSecurityEnabled()) {
      //TODO :- Need to perform token renewal
      delegationTokenManager = new StramDelegationTokenManager(DELEGATION_KEY_UPDATE_INTERVAL,
                                                               DELEGATION_TOKEN_MAX_LIFETIME,
                                                               DELEGATION_TOKEN_RENEW_INTERVAL,
                                                               DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL);
    }
    this.nmClient = new NMClientAsyncImpl(new NMCallbackHandler());
    addService(nmClient);
    this.amRmClient = AMRMClient.createAMRMClient();
    addService(amRmClient);

    // start RPC server
    int rpcListenerCount = dag.getValue(DAGContext.HEARTBEAT_LISTENER_THREAD_COUNT);
    this.rpcListener = new LicensingAgentProtocolImpl(this.getClass().getName(), delegationTokenManager, rpcListenerCount);
    addService(rpcListener);

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
  }

  private boolean run() throws YarnException
  {
    try {
      StramChild.eventloop.start();
      execute();
    }
    catch (Exception re) {
      status = false;
      LOG.error("Caught Exception in execute()", re);
      if (re.getCause() instanceof YarnException) {
        throw (YarnException)re.getCause();
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

  @SuppressWarnings("SleepWhileInLoop")
  public void execute() throws YarnException, IOException
  {
    LOG.info("Starting ApplicationMaster");

    Credentials credentials =
        UserGroupInformation.getCurrentUser().getCredentials();
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
      LOG.info("Container memory specified above max threshold of cluster. Using max value."
              + ", specified=" + containerMemory
              + ", max=" + maxMem);
      containerMemory = maxMem;
    }

    // Setup ask for containers from RM
    // Send request for containers to RM
    // Until we get our fully allocated quota, we keep on polling RM for containers
    // Keep looping until all containers finished processing
    // ( regardless of success/failure).

    this.localCluster.runAsync();
    int loopCounter = -1;

    while (!appDone) {
      loopCounter++;
      try {
        sleep(1000);
        if (loopCounter == 30) {
          break;
        }
      }
      catch (InterruptedException e) {
        LOG.info("Sleep interrupted " + e.getMessage());
      }
    }

    LOG.info("Application completed. Signalling finish to RM");
    FinishApplicationMasterRequest finishReq = Records.newRecord(FinishApplicationMasterRequest.class);
    if (true) {
      finishReq.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
    }
    else {
      finishReq.setFinalApplicationStatus(FinalApplicationStatus.FAILED);
      String diagnostics = "Diagnostics.";
      finishReq.setDiagnostics(diagnostics);
      // return true to indicates expected termination of the master process
      // application status and diagnostics message are set above
      status = true;
    }
    LOG.info("diagnostics: " + finishReq.getDiagnostics());
    amRmClient.unregisterApplicationMaster(finishReq.getFinalApplicationStatus(), finishReq.getDiagnostics(), null);

  }

  private class NMCallbackHandler
    implements NMClientAsync.CallbackHandler {

    NMCallbackHandler() {
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Succeeded to stop Container " + containerId);
      }
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId,
        ContainerStatus containerStatus) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Container Status: id=" + containerId + ", status=" +
            containerStatus);
      }
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Succeeded to start Container " + containerId);
      }
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Start container failed for: containerId={}" + containerId, t);
    }

    @Override
    public void onGetContainerStatusError(
        ContainerId containerId, Throwable t) {
      LOG.error("Failed to query the status of Container " + containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      LOG.warn("Failed to stop container {}", containerId);
    }
  }

}
