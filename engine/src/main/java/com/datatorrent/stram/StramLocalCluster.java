/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.net.NetUtils;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode.Controller;
import com.datatorrent.api.Operator;

import com.datatorrent.bufferserver.server.Server;
import com.datatorrent.bufferserver.storage.DiskStorage;
import com.datatorrent.stram.StramChildAgent.ContainerStartRequest;
import com.datatorrent.stram.StreamingContainerManager.ContainerResource;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.StreamingContainerContext;
import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.physical.PTOperator;

/**
 * Launcher for topologies in local mode within a single process.
 * Child containers are mapped to threads.
 *
 * @since 0.3.2
 */
public class StramLocalCluster implements Runnable, Controller
{
  private static final Logger logger = LoggerFactory.getLogger(StramLocalCluster.class);
  // assumes execution as unit test
  private static File CLUSTER_WORK_DIR = new File("target", StramLocalCluster.class.getName());
  protected final StreamingContainerManager dnmgr;
  private final UmbilicalProtocolLocalImpl umbilical;
  private InetSocketAddress bufferServerAddress;
  private boolean perContainerBufferServer;
  private Server bufferServer = null;
  private final Map<String, LocalStramChild> childContainers = new ConcurrentHashMap<String, LocalStramChild>();
  private int containerSeq = 0;
  private boolean appDone = false;
  private final Map<String, StramChild> injectShutdown = new ConcurrentHashMap<String, StramChild>();
  private boolean heartbeatMonitoringEnabled = true;

  public interface MockComponentFactory
  {
    WindowGenerator setupWindowGenerator();
  }

  private MockComponentFactory mockComponentFactory;

  private class UmbilicalProtocolLocalImpl implements StreamingContainerUmbilicalProtocol
  {
    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
            throws IOException
    {
      throw new UnsupportedOperationException("not implemented in local mode");
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
            long clientVersion, int clientMethodsHash) throws IOException
    {
      throw new UnsupportedOperationException("not implemented in local mode");
    }

    @Override
    public void reportError(String containerId, int[] operators, String msg)
    {
      try {
        log(containerId, msg);
      }
      catch (IOException ex) {
        // ignore
      }
    }

    @Override
    public void log(String containerId, String msg) throws IOException
    {
      logger.info("{} msg: {}", containerId, msg);
    }

    @Override
    public StreamingContainerContext getInitContext(String containerId)
            throws IOException
    {
      StramChildAgent sca = dnmgr.getContainerAgent(containerId);
      StreamingContainerContext scc = sca.getInitContext();
      scc.deployBufferServer = perContainerBufferServer;
      return scc;
    }

    @Override
    public ContainerHeartbeatResponse processHeartbeat(ContainerHeartbeat msg)
    {
      if (injectShutdown.containsKey(msg.getContainerId())) {
        ContainerHeartbeatResponse r = new ContainerHeartbeatResponse();
        r.shutdown = true;
        return r;
      }
      try {
        //LOG.debug("processing heartbeat " + msg.getContainerId());
        return dnmgr.processHeartbeat(msg);
      }
      finally {
        LocalStramChild c = childContainers.get(msg.getContainerId());
        synchronized (c.heartbeatCount) {
          c.heartbeatCount.incrementAndGet();
          c.heartbeatCount.notifyAll();
        }
      }
    }

  }

  public static class LocalStramChild extends StramChild
  {
    /**
     * Count heartbeat from container and allow other threads to wait for it.
     */
    private final AtomicInteger heartbeatCount = new AtomicInteger();
    private final WindowGenerator windowGenerator;

    public LocalStramChild(String containerId, StreamingContainerUmbilicalProtocol umbilical, WindowGenerator winGen)
    {
      super(containerId, umbilical);
      this.windowGenerator = winGen;
    }

    public static void run(StramChild stramChild, StreamingContainerContext ctx) throws Exception
    {
      logger.debug("Got context: " + ctx);
      stramChild.setup(ctx);
      boolean hasError = true;
      try {
        // main thread enters heartbeat loop
        stramChild.heartbeatLoop();
        hasError = false;
      }
      finally {
        // teardown
        try {
          stramChild.teardown();
        }
        catch (Exception e) {
          if (!hasError) {
            throw e;
          }
        }
      }
    }

    public void waitForHeartbeat(int waitMillis) throws InterruptedException
    {
      synchronized (heartbeatCount) {
        heartbeatCount.wait(waitMillis);
      }
    }

    @Override
    public void teardown()
    {
      super.teardown();
    }

    @Override
    protected WindowGenerator setupWindowGenerator(long smallestWindowId)
    {
      if (windowGenerator != null) {
        return windowGenerator;
      }
      return super.setupWindowGenerator(smallestWindowId);
    }

    OperatorContext getNodeContext(int id)
    {
      return nodes.get(id).context;
    }

    Operator getOperator(int id)
    {
      return nodes.get(id).getOperator();
    }

    Map<Integer, Node<?>> getNodes()
    {
      return nodes;
    }

  }

  /**
   * Starts the child "container" as thread.
   */
  private class LocalStramChildLauncher implements Runnable
  {
    final String containerId;
    final LocalStramChild child;

    @SuppressWarnings("CallToThreadStartDuringObjectConstruction")
    private LocalStramChildLauncher(ContainerStartRequest cdr)
    {
      this.containerId = "container-" + containerSeq++;
      WindowGenerator wingen = null;
      if (mockComponentFactory != null) {
        wingen = mockComponentFactory.setupWindowGenerator();
      }
      this.child = new LocalStramChild(containerId, umbilical, wingen);
      ContainerResource cr = new ContainerResource(cdr.container.getResourceRequestPriority(), containerId, "localhost", cdr.container.getRequiredMemoryMB(), null);
      StramChildAgent sca = dnmgr.assignContainer(cr, perContainerBufferServer ? null : NetUtils.getConnectAddress(bufferServerAddress));
      if (sca != null) {
        Thread launchThread = new Thread(this, containerId);
        launchThread.start();
        childContainers.put(containerId, child);
        logger.info("Started container {}", containerId);
      }
    }

    @Override
    public void run()
    {
      try {
        StreamingContainerContext ctx = umbilical.getInitContext(containerId);
        LocalStramChild.run(child, ctx);
      }
      catch (Exception e) {
        logger.error("Container {} failed", containerId, e);
        throw new RuntimeException(e);
      }
      finally {
        childContainers.remove(containerId);
        logger.info("Container {} terminating.", containerId);
      }
    }

  }

  public StramLocalCluster(LogicalPlan dag) throws Exception
  {
    dag.validate();
    // convert to URI so we always write to local file system,
    // even when the environment has a default HDFS location.
    String pathUri = CLUSTER_WORK_DIR.toURI().toString();
    try {
      FileContext.getLocalFSFileContext().delete(new Path(pathUri/*CLUSTER_WORK_DIR.getAbsolutePath()*/), true);
    }
    catch (IllegalArgumentException e) {
      throw e;
    }
    catch (IOException e) {
      throw new RuntimeException("could not cleanup test dir", e);
    }

    dag.getAttributes().put(LogicalPlan.APPLICATION_ID, "app_local_" + System.currentTimeMillis());
    if (dag.getAttributes().get(LogicalPlan.APPLICATION_PATH) == null) {
      dag.getAttributes().put(LogicalPlan.APPLICATION_PATH, pathUri);
    }
    if (dag.getAttributes().get(OperatorContext.STORAGE_AGENT) == null) {
      dag.setAttribute(OperatorContext.STORAGE_AGENT, new FSStorageAgent(new Path(pathUri, LogicalPlan.SUBDIR_CHECKPOINTS).toString(), null));
    }
    this.dnmgr = new StreamingContainerManager(dag);
    this.umbilical = new UmbilicalProtocolLocalImpl();

    if (!perContainerBufferServer) {
      StramChild.eventloop.start();
      bufferServer = new Server(0, 1024 * 1024,8);
      bufferServer.setSpoolStorage(new DiskStorage());
      SocketAddress bindAddr = bufferServer.run(StramChild.eventloop);
      this.bufferServerAddress = ((InetSocketAddress)bindAddr);
      logger.info("Buffer server started: {}", bufferServerAddress);
    }
  }

  LocalStramChild getContainer(String id)
  {
    return this.childContainers.get(id);
  }

  public StreamingContainerManager getStreamingContainerManager()
  {
    return dnmgr;
  }

  public DAG getDAG()
  {
    return dnmgr.getPhysicalPlan().getLogicalPlan();
  }

  public StramLocalCluster(LogicalPlan dag, MockComponentFactory mcf) throws Exception
  {
    this(dag);
    this.mockComponentFactory = mcf;
  }

  /**
   * Simulate container failure for testing purposes.
   *
   * @param c
   */
  void failContainer(StramChild c)
  {
    injectShutdown.put(c.getContainerId(), c);
    c.triggerHeartbeat();
    logger.info("Container {} failed, launching new container.", c.getContainerId());
    dnmgr.scheduleContainerRestart(c.getContainerId());
    // simplify testing: remove immediately rather than waiting for thread to exit
    this.childContainers.remove(c.getContainerId());
  }

  public PTOperator findByLogicalNode(OperatorMeta logicalNode)
  {
    List<PTOperator> nodes = dnmgr.getPhysicalPlan().getOperators(logicalNode);
    if (nodes.isEmpty()) {
      return null;
    }
    return nodes.get(0);
  }

  List<PTOperator> getPlanOperators(OperatorMeta logicalNode)
  {
    return dnmgr.getPhysicalPlan().getOperators(logicalNode);
  }

  /**
   * Return the container that has the given operator deployed.
   * Returns null if the specified operator is not deployed.
   *
   * @param planOperator
   * @return
   */
  public LocalStramChild getContainer(PTOperator planOperator)
  {
    LocalStramChild container;
    String cid = planOperator.getContainer().getExternalId();
    if (cid != null) {
      if ((container = getContainer(cid)) != null) {
        if (container.getNodeContext(planOperator.getId()) != null) {
          return container;
        }
      }
    }
    return null;
  }

  StramChildAgent getContainerAgent(StramChild c)
  {
    return this.dnmgr.getContainerAgent(c.getContainerId());
  }

  @Override
  public void runAsync()
  {
    new Thread(this, "master").start();
  }

  @Override
  public void shutdown()
  {
    appDone = true;
  }

  @Override
  public void setHeartbeatMonitoringEnabled(boolean enabled)
  {
    this.heartbeatMonitoringEnabled = enabled;
  }

  public void setPerContainerBufferServer(boolean perContainerBufferServer)
  {
    this.perContainerBufferServer = perContainerBufferServer;
  }

  @Override
  public void run()
  {
    run(0);
  }

  @Override
  @SuppressWarnings({"SleepWhileInLoop", "ResultOfObjectAllocationIgnored"})
  public void run(long runMillis)
  {
    long endMillis = System.currentTimeMillis() + runMillis;

    while (!appDone) {

      for (String containerIdStr: dnmgr.containerStopRequests.values()) {
        // teardown child thread
        StramChild c = childContainers.get(containerIdStr);
        if (c != null) {
          ContainerHeartbeatResponse r = new ContainerHeartbeatResponse();
          r.shutdown = true;
          c.processHeartbeatResponse(r);
        }
        dnmgr.containerStopRequests.remove(containerIdStr);
        logger.info("Container {} restart.", containerIdStr);
        dnmgr.scheduleContainerRestart(containerIdStr);
        //dnmgr.removeContainerAgent(containerIdStr);
      }

      // start containers
      while (!dnmgr.containerStartRequests.isEmpty()) {
        ContainerStartRequest cdr = dnmgr.containerStartRequests.poll();
        if (cdr != null) {
          new LocalStramChildLauncher(cdr);
        }
      }

      if (heartbeatMonitoringEnabled) {
        // monitor child containers
        dnmgr.monitorHeartbeat();
      }

      if (childContainers.isEmpty() && dnmgr.containerStartRequests.isEmpty()) {
        appDone = true;
      }

      if (runMillis > 0 && System.currentTimeMillis() > endMillis) {
        appDone = true;
      }

      if (Thread.interrupted()) {
        break;
      }

      if (!appDone) {
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException e) {
          logger.info("Sleep interrupted " + e.getMessage());
          break;
        }
      }
    }

    for (LocalStramChild lsc: childContainers.values()) {
      injectShutdown.put(lsc.getContainerId(), lsc);
      lsc.triggerHeartbeat();
    }

    logger.info("Application finished.");
    if (!perContainerBufferServer) {
      StramChild.eventloop.stop(bufferServer);
      StramChild.eventloop.stop();
    }
  }

}
