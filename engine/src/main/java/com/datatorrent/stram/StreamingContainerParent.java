/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.stram.api.StramEvent.ContainerErrorEvent;
import com.datatorrent.stram.api.StramEvent.OperatorErrorEvent;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol;
import com.datatorrent.stram.util.SecureExecutor;
import com.datatorrent.stram.webapp.OperatorInfo;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Stram side implementation of communication protocol with hadoop container<p>
 * <br>
 *
 * @since 0.3.2
 */
public class StreamingContainerParent extends org.apache.hadoop.service.CompositeService implements StreamingContainerUmbilicalProtocol {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingContainerParent.class);
  private Server server;
  private SecretManager<? extends TokenIdentifier> tokenSecretManager = null;
  private InetSocketAddress address;
  private final StreamingContainerManager dagManager;
  private final int listenerThreadCount;

  public StreamingContainerParent(String name, StreamingContainerManager dnodeMgr, SecretManager<? extends TokenIdentifier> secretManager, int listenerThreadCount) {
    super(name);
    this.dagManager = dnodeMgr;
    this.tokenSecretManager = secretManager;
    this.listenerThreadCount = listenerThreadCount;
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
    LOG.info("Listener thread count " + listenerThreadCount);
    try {
      server = new RPC.Builder(conf).setProtocol(StreamingContainerUmbilicalProtocol.class).setInstance(this)
          .setBindAddress("0.0.0.0").setPort(0).setNumHandlers(listenerThreadCount).setSecretManager(tokenSecretManager).setVerbose(false).build();

      // Enable service authorization?
      if (conf.getBoolean(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
          false)) {
        //refreshServiceAcls(conf, new MRAMPolicyProvider());
        server.refreshServiceAcl(conf, new PolicyProvider() {

          @Override
          public Service[] getServices()
          {
            return (new Service[] {
              new Service(StreamingContainerUmbilicalProtocol.class.getName(), StreamingContainerUmbilicalProtocol.class)
            });
          }

        });
      }

      server.start();
      this.address = NetUtils.getConnectAddress(server);
      LOG.info("Container callback server listening at " + this.address);
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }

  protected void stopRpcServer() {
    server.stop();
  }

  public InetSocketAddress getAddress() {
    return address;
  }

  // Can be used to help test listener thread count
  public int getListenerThreadCount()
  {
    return listenerThreadCount;
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
  public void reportError(String containerId, int[] operators, String msg)
  {
    if (operators == null || operators.length == 0) {
      dagManager.recordEventAsync(new ContainerErrorEvent(containerId, msg));
    }
    else {
      for (int operator : operators) {
        OperatorInfo operatorInfo = dagManager.getOperatorInfo(operator);
        if (operatorInfo != null) {
          dagManager.recordEventAsync(new OperatorErrorEvent(operatorInfo.name, operator, containerId, msg));
        }
      }
    }
    try {
      log(containerId, msg);
    }
    catch (IOException ex) {
      // ignore
    }
  }

  @Override
  public StreamingContainerContext getInitContext(String containerId)
      throws IOException {
    StreamingContainerAgent sca = dagManager.getContainerAgent(containerId);

    return sca.getInitContext();
  }

  @Override
  public ContainerHeartbeatResponse processHeartbeat(ContainerHeartbeat msg) {
    // -- TODO
    // Change to use some sort of a annotation that developers can use to specify secure code
    // For now using SecureExecutor work load. Also change sig to throw Exception
    long now = System.currentTimeMillis();
    if (msg.sentTms - now > 50) {
      LOG.warn("Child container heartbeat sent time for {} ({}) is greater than the receive timestamp in AM ({}). Please make sure the clocks are in sync", msg.getContainerId(), msg.sentTms, now);
    }
    //LOG.debug("RPC latency from child container {} is {} ms (according to system clocks)", msg.getContainerId(), now - msg.sentTms);
    dagManager.updateRPCLatency(msg.getContainerId(), now - msg.sentTms);
    try {
      final ContainerHeartbeat fmsg = msg;
      return SecureExecutor.execute(new SecureExecutor.WorkLoad<ContainerHeartbeatResponse>() {
        @Override
        public ContainerHeartbeatResponse run()
        {
          return dagManager.processHeartbeat(fmsg);
        }
      });
    } catch (IOException ex) {
      LOG.error("Error processing heartbeat", ex);
      return null;
    }
  }

}
