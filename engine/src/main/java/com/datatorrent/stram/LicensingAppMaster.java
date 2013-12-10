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
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAGContext;
import com.datatorrent.stram.debug.StdOutErrLog;
import com.datatorrent.stram.license.LicensingAgentProtocolImpl;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.security.StramDelegationTokenManager;
import com.datatorrent.stram.util.VersionInfo;
import com.google.common.collect.Sets;

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
  private LogicalPlan dag;
  private ApplicationAttemptId appAttemptID;
  private final String appMasterTrackingUrl = "";
  private FinalApplicationStatus finalStatus = null;
  private final String diagnosticsMessage = "";
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

    LicensingAppMaster appMaster = null;
    try {
      appMaster = new LicensingAppMaster();
      LOG.info("Initializing ApplicationMaster");

      ContainerId containerId = ConverterUtils.toContainerId(System.getenv(Environment.CONTAINER_ID.name()));
      appMaster.appAttemptID = containerId.getApplicationAttemptId();

      Configuration conf = new YarnConfiguration();
      appMaster.init(conf);
      appMaster.start();
      appMaster.run();
      LOG.debug("run complete");
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

    if (appMaster.finalStatus == FinalApplicationStatus.SUCCEEDED) {
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

    String licenseBase64 = dag.getValue(LogicalPlan.LICENSE);
    byte[] licenseBytes = Base64.decodeBase64(licenseBase64);
    LOG.info("License:\n{}", new String(licenseBytes));

    if (UserGroupInformation.isSecurityEnabled()) {
      //TODO :- Need to perform token renewal
      delegationTokenManager = new StramDelegationTokenManager(DELEGATION_KEY_UPDATE_INTERVAL,
                                                               DELEGATION_TOKEN_MAX_LIFETIME,
                                                               DELEGATION_TOKEN_RENEW_INTERVAL,
                                                               DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL);
    }
    this.amRmClient = AMRMClient.createAMRMClient();
    addService(amRmClient);

    // RPC server
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
    amRmClient.stop();
  }

  private void run() throws YarnException, IOException
  {
    try {
      StramChild.eventloop.start();

      LOG.info("Starting ApplicationMaster");
      Credentials credentials =
          UserGroupInformation.getCurrentUser().getCredentials();
      LOG.info("number of tokens: {}", credentials.getAllTokens().size());
      Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
      while (iter.hasNext()) {
        Token<?> token = iter.next();
        LOG.debug("token: " + token);
      }
      LOG.debug("Registering with RM {}", this.appAttemptID);
      // Register self with ResourceManager
      InetSocketAddress connectAddress = NetUtils.getConnectAddress(rpcListener.getAddress());
      amRmClient.registerApplicationMaster(connectAddress.getHostName(), connectAddress.getPort(), appMasterTrackingUrl);
      LOG.debug("Registered with RM as {}", connectAddress);

      mainLoop();

      FinishApplicationMasterRequest finishReq = Records.newRecord(FinishApplicationMasterRequest.class);
      finishReq.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
      if (this.finalStatus == FinalApplicationStatus.FAILED) {
        finishReq.setFinalApplicationStatus(finalStatus);
        finishReq.setDiagnostics(this.diagnosticsMessage);
        LOG.info("Diagnostics: " + finishReq.getDiagnostics());
      }
      LOG.info("Application completed with {}. Signalling finish to RM", finishReq.getFinalApplicationStatus());
      amRmClient.unregisterApplicationMaster(finishReq.getFinalApplicationStatus(), finishReq.getDiagnostics(), null);
    } finally {
      StramChild.eventloop.stop();
    }
  }

  /**
   * Main run function for the application master
   *
   * @throws YarnRemoteException
   */
  @SuppressWarnings("SleepWhileInLoop")
  public void mainLoop() throws YarnException, IOException
  {
    // ensure that we are the only instance for the given license key
    try {
      YarnClient clientRMService = YarnClient.createYarnClient();
      clientRMService.init(getConfig());
      clientRMService.start();
      List<ApplicationReport> apps = clientRMService.getApplications(Sets.newHashSet(StramClient.YARN_APPLICATION_TYPE_LICENSE),
          Sets.newEnumSet(Sets.newHashSet(YarnApplicationState.RUNNING), YarnApplicationState.class));
      LOG.debug("There are {} license agents registered", apps.size());
      for (ApplicationReport ar : apps) {
        ApplicationId otherAppId = ar.getApplicationId();
        if (otherAppId.compareTo(this.appAttemptID.getApplicationId()) != 0) {
          LOG.debug("Found another license agent {} {}", ar.getName(), otherAppId);
          if (otherAppId.getId() < this.appAttemptID.getApplicationId().getId()) {
            // another agent was started for the same id
            if (ar.getName().equals(this.dag.getValue(DAG.APPLICATION_NAME))) {
              String msg = String.format("Licensing agent %s already running as %s", ar.getName(), ar.getApplicationId());
              LOG.error(msg);
              this.finalStatus = FinalApplicationStatus.FAILED;
              clientRMService.stop();
              return;
            }
          }
        }
      }
      clientRMService.stop();
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to retrieve cluster nodes report.", e);
    }

    int loopCounter = -1;
    while (true) {
      loopCounter++;
      try {
        sleep(1000);
        amRmClient.allocate(0);
        //if (loopCounter == 60) {
        //  break;
        //}
      }
      catch (InterruptedException e) {
        LOG.info("Sleep interrupted " + e.getMessage());
      }
    }

  }

}
