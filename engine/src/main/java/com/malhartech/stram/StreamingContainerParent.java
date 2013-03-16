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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.service.CompositeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.DAGContext;

/**
 *
 * Stram side implementation of communication protocol with hadoop container<p>
 * <br>
 *
 */

public class StreamingContainerParent extends CompositeService implements StreamingContainerUmbilicalProtocol {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingContainerParent.class);
  private Server server;
  private final SecretManager<? extends TokenIdentifier> tokenSecretManager = null;
  private InetSocketAddress address;
  private final StreamingContainerManager dagManager;

  public StreamingContainerParent(String name, StreamingContainerManager dnodeMgr) {
    super(name);
    this.dagManager = dnodeMgr;
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
          RPC.getServer(StreamingContainerUmbilicalProtocol.class, this, "0.0.0.0", 0,
              DAGContext.DEFAULT_HEARTBEAT_LISTENER_THREAD_COUNT,
              false, conf, tokenSecretManager);

      // Enable service authorization?
      if (conf.getBoolean(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
          false)) {
        //refreshServiceAcls(conf, new MRAMPolicyProvider());
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
    return StreamingContainerUmbilicalProtocol.versionID;
  }

  @Override
  public void log(String containerId, String msg) throws IOException {
    LOG.info("child msg: {} context: {}", msg, dagManager.getContainerAgent(containerId).container);
  }

  @Override
  public StreamingContainerContext getInitContext(String containerId)
      throws IOException {
    StramChildAgent sca = dagManager.getContainerAgent(containerId);
    return sca.getInitContext();
  }

  @Override
  public ContainerHeartbeatResponse processHeartbeat(ContainerHeartbeat msg) {
    return dagManager.processHeartbeat(msg);
  }

  @Override
  public ContainerHeartbeatResponse pollRequest(String containerId) {
    StramChildAgent sca = dagManager.getContainerAgent(containerId);
    return sca.pollRequest();
  }

}
