/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.events.grouping.GroupingManager;
import org.apache.apex.engine.events.grouping.GroupingRequest;
import org.apache.apex.engine.events.grouping.GroupingRequest.EventGroupId;
import org.apache.apex.log.LogFileInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import com.datatorrent.stram.api.StramEvent.ContainerErrorEvent;
import com.datatorrent.stram.api.StramEvent.OperatorErrorEvent;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol;
import com.datatorrent.stram.util.SecureExecutor;
import com.datatorrent.stram.webapp.OperatorInfo;

/**
 *
 * Stram side implementation of communication protocol with hadoop container<p>
 * <br>
 *
 * @since 0.3.2
 */
public class StreamingContainerParent extends org.apache.hadoop.service.CompositeService implements StreamingContainerUmbilicalProtocol
{

  private static final Logger LOG = LoggerFactory.getLogger(StreamingContainerParent.class);
  private Server server;
  private SecretManager<? extends TokenIdentifier> tokenSecretManager = null;
  private InetSocketAddress address;
  private final StreamingContainerManager dagManager;
  private final int listenerThreadCount;

  public StreamingContainerParent(String name, StreamingContainerManager dnodeMgr, SecretManager<? extends TokenIdentifier> secretManager, int listenerThreadCount)
  {
    super(name);
    this.dagManager = dnodeMgr;
    this.tokenSecretManager = secretManager;
    this.listenerThreadCount = listenerThreadCount;
  }

  @Override
  public void init(Configuration conf)
  {
    super.init(conf);
  }

  @Override
  public void start()
  {
    startRpcServer();
    super.start();
  }

  @Override
  public void stop()
  {
    stopRpcServer();
    super.stop();
  }

  protected void startRpcServer()
  {
    Configuration conf = getConfig();
    LOG.info("Config: " + conf);
    LOG.info("Listener thread count " + listenerThreadCount);
    try {
      server = new RPC.Builder(conf).setProtocol(StreamingContainerUmbilicalProtocol.class).setInstance(this)
          .setBindAddress("0.0.0.0").setPort(0).setNumHandlers(listenerThreadCount).setSecretManager(tokenSecretManager)
          .setVerbose(false).build();

      // Enable service authorization?
      if (conf.getBoolean(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
          false)) {
        //refreshServiceAcls(conf, new MRAMPolicyProvider());
        server.refreshServiceAcl(conf, new PolicyProvider()
        {

          @Override
          public Service[] getServices()
          {
            return (new Service[]{
                new Service(StreamingContainerUmbilicalProtocol.class
                    .getName(), StreamingContainerUmbilicalProtocol.class)
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

  protected void stopRpcServer()
  {
    server.stop();
  }

  public InetSocketAddress getAddress()
  {
    return address;
  }

  // Can be used to help test listener thread count
  public int getListenerThreadCount()
  {
    return listenerThreadCount;
  }

  void refreshServiceAcls(Configuration configuration,
      PolicyProvider policyProvider)
  {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException
  {
    return ProtocolSignature.getProtocolSignature(this, protocol, clientVersion, clientMethodsHash);
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException
  {
    return StreamingContainerUmbilicalProtocol.versionID;
  }

  @Override
  public void log(String containerId, String msg) throws IOException
  {
    final StreamingContainerAgent sca = dagManager.getContainerAgent(containerId);
    if (sca != null) {
      LOG.info("child msg: {} context: {}", msg, sca.container);
    } else {
      LOG.info("unknown container {} msg: {}", containerId, msg);
    }
  }

  @Override
  public void reportError(String containerId, int[] operators, String msg, LogFileInformation logFileInfo) throws IOException
  {
    EventGroupId groupId = getGroupIdForNewGroupingRequest(containerId);
    if (operators == null || operators.length == 0) {
      dagManager.recordEventAsync(new ContainerErrorEvent(containerId, msg, logFileInfo, groupId));
    } else {
      for (int operator : operators) {
        OperatorInfo operatorInfo = dagManager.getOperatorInfo(operator);
        if (operatorInfo != null) {
          dagManager.recordEventAsync(
              new OperatorErrorEvent(operatorInfo.name, operator, containerId, msg, logFileInfo, groupId));
        }
      }
    }
    log(containerId, msg);
  }

  //create new group the deploy request, request data will be populated when sub-dag restart happens
  private EventGroupId getGroupIdForNewGroupingRequest(String containerId)
  {
    GroupingManager groupingManager = GroupingManager.getGroupingManagerInstance();
    GroupingRequest groupingRequest = groupingManager.addOrModifyGroupingRequest(containerId, Collections.EMPTY_SET);
    return groupingRequest.getEventGroupId();
  }

  @Override
  public StreamingContainerContext getInitContext(String containerId)
      throws IOException
  {
    StreamingContainerContext scc = null;
    StreamingContainerAgent sca = dagManager.getContainerAgent(containerId);
    if (sca != null) {
      scc = sca.getInitContext();
    }
    return scc;
  }

  @Override
  public ContainerHeartbeatResponse processHeartbeat(final ContainerHeartbeat msg) throws IOException
  {
    // -- TODO
    // Change to use some sort of a annotation that developers can use to specify secure code
    // For now using SecureExecutor work load. Also change sig to throw Exception
    long now = System.currentTimeMillis();
    if (msg.sentTms - now > 50) {
      LOG.warn("Child container heartbeat sent time for {} ({}) is greater than the receive timestamp in AM ({}). Please make sure the clocks are in sync", msg.getContainerId(), msg.sentTms, now);
    }
    //LOG.debug("RPC latency from child container {} is {} ms (according to system clocks)", msg.getContainerId(),
    // now - msg.sentTms);
    dagManager.updateRPCLatency(msg.getContainerId(), now - msg.sentTms);
    return SecureExecutor.execute(new SecureExecutor.WorkLoad<ContainerHeartbeatResponse>()
    {
      @Override
      public ContainerHeartbeatResponse run()
      {
        return dagManager.processHeartbeat(msg);
      }
    });
  }

}
