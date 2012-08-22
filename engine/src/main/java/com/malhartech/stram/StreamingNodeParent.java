/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.app.security.authorize.MRAMPolicyProvider;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.service.CompositeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Stram side implementation of communication protocol with hadoop container<p>
 * <br>
 * 
 */

public class StreamingNodeParent extends CompositeService implements StreamingNodeUmbilicalProtocol {

  private static Logger LOG = LoggerFactory.getLogger(StreamingNodeParent.class);
  private Server server;
  private SecretManager<? extends TokenIdentifier> tokenSecretManager = null;
  private InetSocketAddress address;
  private DNodeManager dnodeManager;
  
  public StreamingNodeParent(String name, DNodeManager dnodeMgr) {
    super(name);
    this.dnodeManager = dnodeMgr;
  }

  @Override
  public void init(Configuration conf) {
   super.init(conf);
  }  
  
  @Override
  public void start() {
    startRpcServer();
    super.start();
  }

  @Override
  public void stop() {
    stopRpcServer();
    super.stop();
  }  

  protected void startRpcServer() {
    Configuration conf = getConfig();
    LOG.info("Config: " + conf);
    try {
      server =
          RPC.getServer(StreamingNodeUmbilicalProtocol.class, this, "0.0.0.0", 0, 
              conf.getInt(MRJobConfig.MR_AM_TASK_LISTENER_THREAD_COUNT, // TODO: config
                  MRJobConfig.DEFAULT_MR_AM_TASK_LISTENER_THREAD_COUNT),
              false, conf, tokenSecretManager);
      
      // Enable service authorization?
      if (conf.getBoolean(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
          false)) {
        refreshServiceAcls(conf, new MRAMPolicyProvider());
      }

      server.start();
      this.address = NetUtils.getConnectAddress(server);
    } catch (IOException e) {
      throw new YarnException(e);
    }
  }  

  protected void stopRpcServer() {
    server.stop();
  }

  public InetSocketAddress getAddress() {
    return address;
  }  
  
  void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }
    
  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
      return ProtocolSignature.getProtocolSignature(this, 
          protocol, clientVersion, clientMethodsHash);
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return StreamingNodeUmbilicalProtocol.versionID;
  }

  @Override
  public void log(String containerId, String msg) throws IOException {
    LOG.info("child msg: {} context: {}", msg, dnodeManager.getContainerAgent(containerId).container);
  }

  @Override
  public StreamingContainerContext getInitContext(String containerId)
      throws IOException {
    StramChildAgent sca = dnodeManager.getContainerAgent(containerId);
    return sca.getInitContext();
  }

  @Override
  public ContainerHeartbeatResponse processHeartbeat(ContainerHeartbeat msg) {
    return dnodeManager.processHeartbeat(msg);
  }

  @Override
  public ContainerHeartbeatResponse pollRequest(String containerId) {
    StramChildAgent sca = dnodeManager.getContainerAgent(containerId);
    return sca.pollRequest();
  }

  @Override
  public StramToNodeRequest processPartioningDetails() {
    throw new RuntimeException("processPartioningDetails not implemented");
  }
  
}
