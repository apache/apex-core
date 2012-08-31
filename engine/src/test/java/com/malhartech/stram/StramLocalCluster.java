/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.bufferserver.Server;
import com.malhartech.stram.StramChildAgent.DeployRequest;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.TopologyDeployer.PTNode;
import com.malhartech.stram.conf.Topology;
import com.malhartech.stram.conf.Topology.NodeDecl;

/**
 * Launcher for topologies in local mode within a single process.
 * Child containers are mapped to threads.
 */
public class StramLocalCluster implements Runnable {

  private static Logger LOG = LoggerFactory.getLogger(StramLocalCluster.class);
  // assumes execution as unit test
  private static File CLUSTER_WORK_DIR = new File("target", StramLocalCluster.class.getName());

  final private DNodeManager dnmgr;
  final private UmbilicalProtocolLocalImpl umbilical;
  final private InetSocketAddress bufferServerAddress;
  private Server bufferServer = null;
  final private Map<String, LocalStramChild> childContainers = new ConcurrentHashMap<String, LocalStramChild>();
  private int containerSeq = 0;
  private boolean appDone = false;

  final private Map<String, StramChild> injectShutdown = new ConcurrentHashMap<String, StramChild>();

  private class UmbilicalProtocolLocalImpl implements StreamingNodeUmbilicalProtocol {

    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
        throws IOException {
      throw new UnsupportedOperationException("not implemented in local mode");
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
        long clientVersion, int clientMethodsHash) throws IOException {
      throw new UnsupportedOperationException("not implemented in local mode");
    }

    @Override
    public void log(String containerId, String msg) throws IOException {
      LOG.info("child msg: {} context: {}", msg, dnmgr.getContainerAgent(containerId).container);
    }

    @Override
    public StreamingContainerContext getInitContext(String containerId)
        throws IOException {
      StramChildAgent sca = dnmgr.getContainerAgent(containerId);
      return sca.getInitContext();
    }

    @Override
    public ContainerHeartbeatResponse processHeartbeat(ContainerHeartbeat msg) {
      if (injectShutdown.containsKey(msg.getContainerId())) {
        ContainerHeartbeatResponse r = new ContainerHeartbeatResponse();
        r.setShutdown(true);
        return r;
      }
      try {
        return dnmgr.processHeartbeat(msg);
      } finally {
        LocalStramChild c = childContainers.get(msg.getContainerId());
        synchronized (c.heartbeatCount) {
          c.heartbeatCount.incrementAndGet();
          c.heartbeatCount.notifyAll();
        }
      }
    }

    @Override
    public ContainerHeartbeatResponse pollRequest(String containerId) {
      StramChildAgent sca = dnmgr.getContainerAgent(containerId);
      return sca.pollRequest();
    }

    @Override
    public StramToNodeRequest processPartioningDetails() {
      throw new RuntimeException("processPartioningDetails not implemented");
    }

  }

  public static class LocalStramChild extends StramChild
  {
    /**
     * Count heartbeat from container and allow other threads to wait for it.
     */
    private AtomicInteger heartbeatCount = new AtomicInteger();

    public LocalStramChild(String containerId, StreamingNodeUmbilicalProtocol umbilical, WindowGenerator wgen)
    {
      super(containerId, new Configuration(), umbilical);
      super.setWindowGenerator(wgen);
    }

    @Override
    public void init(StreamingContainerContext ctx) throws IOException
    {
      super.init(ctx);
    }

    @Override
    public void shutdown()
    {
      super.shutdown();
    }

    public static void run(StramChild stramChild, StreamingContainerContext ctx) throws Exception {
      LOG.debug("Got context: " + ctx);
      stramChild.init(ctx);
      // main thread enters heartbeat loop
      stramChild.heartbeatLoop();
      // shutdown
      stramChild.shutdown();
    }

    public void waitForHeartbeat(int waitMillis) throws InterruptedException {
      synchronized (heartbeatCount) {
        heartbeatCount.wait(waitMillis);
      }
    }

  }

  /**
   * Starts the child "container" as thread.
   */
  private class LocalStramChildLauncher implements Runnable {
    final String containerId;
    final LocalStramChild child;

    private LocalStramChildLauncher(DeployRequest cdr) {
      this.containerId = "container-" + containerSeq++;
      this.child = new LocalStramChild(containerId, umbilical, null);
      dnmgr.assignContainer(cdr, containerId, NetUtils.getConnectAddress(bufferServerAddress));
      Thread launchThread = new Thread(this, containerId);
      launchThread.start();
      childContainers.put(containerId, child);
      LOG.info("Started container {}", containerId);
    }

    @Override
    public void run() {
      try {
        StreamingContainerContext ctx = umbilical.getInitContext(containerId);
        LocalStramChild.run(child, ctx);
      } catch (Exception e) {
        LOG.error("Container {} failed", containerId, e);
        throw new RuntimeException(e);
      } finally {
        childContainers.remove(containerId);
        LOG.info("Container {} terminating.", containerId);
      }
    }
  }

  public StramLocalCluster(Topology topology) throws Exception {

    try {
      FileContext.getLocalFSFileContext().delete(
          new Path(CLUSTER_WORK_DIR.getAbsolutePath()), true);
    } catch (Exception e) {
      throw new RuntimeException("could not cleanup test dir", e);
    }

    if (topology.getConf().get(Topology.STRAM_CHECKPOINT_DIR) == null) {
      topology.getConf().set(Topology.STRAM_CHECKPOINT_DIR, CLUSTER_WORK_DIR.getPath());
    }
    this.dnmgr = new DNodeManager(topology);
    this.umbilical = new UmbilicalProtocolLocalImpl();

    // start buffer server
    this.bufferServer = new Server(0);
    SocketAddress bindAddr = this.bufferServer.run();
    this.bufferServerAddress = ((InetSocketAddress) bindAddr);
    LOG.info("Buffer server started: {}", bufferServerAddress);
  }

  LocalStramChild getContainer(String id) {
    return this.childContainers.get(id);
  }

  /**
   * Simulate container failure for testing purposes.
   * @param c
   */
  void failContainer(StramChild c) {
    injectShutdown.put(c.getContainerId(), c);
    c.triggerHeartbeat();
    LOG.info("Container {} failed, launching new container.", c.getContainerId());
    dnmgr.scheduleContainerRestart(c.getContainerId());
    // simplify testing: remove immediately rather than waiting for thread to exit
    this.childContainers.remove(c.getContainerId());
  }

  PTNode findByLogicalNode(NodeDecl logicalNode) {
    List<PTNode> nodes = dnmgr.getTopologyDeployer().getNodes(logicalNode);
    if (nodes.isEmpty()) {
      return null;
    }
    return nodes.get(0);
  }

  StramChildAgent getContainerAgent(StramChild c) {
      return this.dnmgr.getContainerAgent(c.getContainerId());
  }

  public void runAsync() {
    new Thread(this, "master").start();
  }

  public void shutdown() {
    appDone = true;
  }

  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public void run() {
    while (!appDone) {

      for (String containerIdStr : dnmgr.containerStopRequests.values()) {
        // shutdown child thread
        StramChild c = childContainers.get(containerIdStr);
        if (c != null) {
          ContainerHeartbeatResponse r = new ContainerHeartbeatResponse();
          r.setShutdown(true);
          c.processHeartbeatResponse(r);
        }
        dnmgr.containerStopRequests.remove(containerIdStr);
      }

      // start containers
      while (!dnmgr.containerStartRequests.isEmpty()) {
        DeployRequest cdr = dnmgr.containerStartRequests.poll();
        if (cdr != null) {
          new LocalStramChildLauncher(cdr);
        }
      }

      // monitor child containers
      dnmgr.monitorHeartbeat();

      if (childContainers.isEmpty() && dnmgr.containerStartRequests.isEmpty()) {
        appDone = true;
      } else {
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException e) {
          LOG.info("Sleep interrupted " + e.getMessage());
        }
      }
    }

    LOG.info("Application finished.");
    bufferServer.shutdown();
  }

}
