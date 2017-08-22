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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.log.LogFileInformation;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.ProtocolSignature;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode.Controller;
import com.datatorrent.api.Operator;
import com.datatorrent.bufferserver.server.Server;
import com.datatorrent.bufferserver.storage.DiskStorage;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.stram.StreamingContainerAgent.ContainerStartRequest;
import com.datatorrent.stram.StreamingContainerManager.ContainerResource;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.StreamingContainerContext;
import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.engine.StreamingContainer;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.physical.PTOperator;

/**
 * Launcher for topologies in embedded mode within a single process.
 * Child containers are mapped to threads.
 *
 * @since 0.3.2
 */
public class StramLocalCluster implements Runnable, Controller
{
  private static final Logger LOG = LoggerFactory.getLogger(StramLocalCluster.class);
  // assumes execution as unit test
  private static final File DEFAULT_APP_DIR = new File("target", StramLocalCluster.class.getName());
  private static final String LOCALHOST_PROPERTY_KEY = "org.apache.apex.stram.StramLocalCluster.hostname";
  private static final String LOCALHOST = System.getProperty(LOCALHOST_PROPERTY_KEY, "localhost");
  protected final StreamingContainerManager dnmgr;
  private final UmbilicalProtocolLocalImpl umbilical;
  private InetSocketAddress bufferServerAddress;
  private boolean perContainerBufferServer;
  private Server bufferServer = null;
  private final Map<String, LocalStreamingContainer> childContainers = new ConcurrentHashMap<>();
  private int containerSeq = 0;
  private boolean appDone = false;
  private final Map<String, StreamingContainer> injectShutdown = new ConcurrentHashMap<>();
  private boolean heartbeatMonitoringEnabled = true;
  private Callable<Boolean> exitCondition;
  private Thread master;

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
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException
    {
      throw new UnsupportedOperationException("not implemented in local mode");
    }

    @Override
    public void reportError(String containerId, int[] operators, String msg, LogFileInformation logFileInfo) throws IOException
    {
      log(containerId, msg);
    }

    @Override
    public void log(String containerId, String msg) throws IOException
    {
      LOG.info("{} msg: {}", containerId, msg);
    }

    @Override
    public StreamingContainerContext getInitContext(String containerId)
            throws IOException
    {
      StreamingContainerAgent sca = dnmgr.getContainerAgent(containerId);
      StreamingContainerContext scc = sca.getInitContext();
      scc.deployBufferServer = perContainerBufferServer;
      return scc;
    }

    @Override
    public ContainerHeartbeatResponse processHeartbeat(ContainerHeartbeat msg) throws IOException
    {
      if (injectShutdown.containsKey(msg.getContainerId())) {
        ContainerHeartbeatResponse r = new ContainerHeartbeatResponse();
        r.shutdown = ShutdownType.ABORT;
        return r;
      }
      try {
        ContainerHeartbeatResponse rsp = dnmgr.processHeartbeat(msg);
        if (rsp != null) {
          // clone to not share attributes (stream codec etc.) between threads.
          rsp = SerializationUtils.clone(rsp);
        }
        return rsp;
      } finally {
        LocalStreamingContainer c = childContainers.get(msg.getContainerId());
        synchronized (c.heartbeatCount) {
          c.heartbeatCount.incrementAndGet();
          c.heartbeatCount.notifyAll();
        }
      }
    }

  }

  public static class LocalStreamingContainer extends StreamingContainer
  {
    /**
     * Count heartbeat from container and allow other threads to wait for it.
     */
    private final AtomicInteger heartbeatCount = new AtomicInteger();
    private final WindowGenerator windowGenerator;

    public LocalStreamingContainer(String containerId, StreamingContainerUmbilicalProtocol umbilical, WindowGenerator winGen)
    {
      super(containerId, umbilical);
      this.windowGenerator = winGen;
    }

    public void run(StreamingContainerContext ctx) throws Exception
    {
      LOG.debug("container {} context {}", getContainerId(), ctx);
      setup(ctx);
      if (bufferServerAddress != null && !bufferServerAddress.getAddress().isLoopbackAddress()) {
        bufferServerAddress = InetSocketAddress.createUnresolved(LOCALHOST, bufferServerAddress.getPort());
      }

      boolean hasError = true;
      try {
        // main thread enters heartbeat loop
        heartbeatLoop();
        hasError = false;
      } finally {
        // teardown
        try {
          teardown();
        } catch (Exception e) {
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
      return Collections.unmodifiableMap(nodes);
    }

  }

  /**
   * Starts the child "container" as thread.
   */
  private class LocalStreamingContainerLauncher implements Runnable
  {
    final String containerId;
    final LocalStreamingContainer child;

    @SuppressWarnings("CallToThreadStartDuringObjectConstruction")
    private LocalStreamingContainerLauncher(ContainerStartRequest cdr, List<Thread> containerThreads)
    {
      this.containerId = "container-" + containerSeq++;
      WindowGenerator wingen = null;
      if (mockComponentFactory != null) {
        wingen = mockComponentFactory.setupWindowGenerator();
      }
      this.child = new LocalStreamingContainer(containerId, umbilical, wingen);
      ContainerResource cr = new ContainerResource(cdr.container.getResourceRequestPriority(), containerId, "localhost", cdr.container.getRequiredMemoryMB(), cdr.container.getRequiredVCores(), null);
      StreamingContainerAgent sca = dnmgr.assignContainer(cr, perContainerBufferServer ? null : bufferServerAddress);
      if (sca != null) {
        childContainers.put(containerId, child);
        Thread launchThread = new Thread(this, containerId);
        containerThreads.add(launchThread);
        launchThread.start();
      }
    }

    @Override
    public void run()
    {
      try {
        StreamingContainerContext ctx = umbilical.getInitContext(containerId);
        LOG.info("Started container {}", containerId);
        child.run(ctx);
      } catch (Exception | Error e) {
        LOG.error("Fatal {} in container {}", e instanceof Error ? "error" : "exception", containerId, e);
      } finally {
        childContainers.remove(containerId);
        LOG.info("Container {} terminating.", containerId);
      }
    }

  }

  public StramLocalCluster(LogicalPlan dag) throws IOException, ClassNotFoundException
  {
    dag.validate();
    // ensure plan can be serialized
    cloneLogicalPlan(dag);
    final Path pathUri;
    String appPath = dag.getAttributes().get(LogicalPlan.APPLICATION_PATH);
    if (appPath == null) {
      // convert to URI so we always write to local file system,
      // even when the environment has a default HDFS location.
      pathUri = new Path(DEFAULT_APP_DIR.toURI());
      dag.getAttributes().put(LogicalPlan.APPLICATION_PATH, pathUri.toString());
    } else {
      // should accept any valid path URI (or relative path) provided by user
      Path tmp = new Path(appPath);
      if (!tmp.isAbsolute()) {
        pathUri = new Path(new File(appPath).toURI());
      } else {
        pathUri = tmp;
      }
    }
    try {
      FileContext.getLocalFSFileContext().delete(pathUri, true);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (IOException e) {
      throw new RuntimeException("could not cleanup test dir", e);
    }

    dag.getAttributes().put(LogicalPlan.APPLICATION_ID, "app_local_" + System.currentTimeMillis());
    if (dag.getAttributes().get(OperatorContext.STORAGE_AGENT) == null) {
      dag.setAttribute(OperatorContext.STORAGE_AGENT, new AsyncFSStorageAgent(new Path(pathUri, LogicalPlan.SUBDIR_CHECKPOINTS).toString(), null));
    }
    this.dnmgr = new StreamingContainerManager(dag);
    this.umbilical = new UmbilicalProtocolLocalImpl();
  }

  public static LogicalPlan cloneLogicalPlan(LogicalPlan lp) throws IOException, ClassNotFoundException
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    LogicalPlan.write(lp, bos);
    LOG.debug("serialized size: {}", bos.toByteArray().length);
    bos.flush();
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    return LogicalPlan.read(bis);
  }

  LocalStreamingContainer getContainer(String id)
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
  void failContainer(StreamingContainer c)
  {
    injectShutdown.put(c.getContainerId(), c);
    c.triggerHeartbeat();
    LOG.info("Container {} failed, launching new container.", c.getContainerId());
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
  public LocalStreamingContainer getContainer(PTOperator planOperator)
  {
    LocalStreamingContainer container;
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

  StreamingContainerAgent getContainerAgent(StreamingContainer c)
  {
    return this.dnmgr.getContainerAgent(c.getContainerId());
  }

  @Override
  public void runAsync()
  {
    master = new Thread(this, "master");
    master.start();
  }

  @Override
  public void shutdown()
  {
    appDone = true;
    awaitTermination(0);
  }

  private void awaitTermination(long millis)
  {
    if (master != null) {
      try {
        master.interrupt();
        master.join(millis);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        if (master.isAlive()) {
          LOG.warn("{} {} did not terminate.", this.getClass().getSimpleName(), master.getName());
        }
        master = null;
      }
    }
  }

  public boolean isFinished()
  {
    return appDone;
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

  public void setExitCondition(Callable<Boolean> exitCondition)
  {
    this.exitCondition = exitCondition;
  }

  public void run(Callable<Boolean> exitCondition)
  {
    run(exitCondition, 0);
  }

  public void run(Callable<Boolean> exitCondition, long runMillis)
  {
    setExitCondition(exitCondition);
    run(runMillis);
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
    Thread eventLoopThread = null;
    List<Thread> containerThreads = new LinkedList<>();
    try {
      if (!perContainerBufferServer) {
        eventLoopThread = StreamingContainer.eventloop.start();
        bufferServer = new Server(StreamingContainer.eventloop, 0, 1024 * 1024, 8);
        try {
          bufferServer.setSpoolStorage(new DiskStorage());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        bufferServerAddress = InetSocketAddress.createUnresolved(LOCALHOST, bufferServer.run().getPort());
        LOG.info("Buffer server started: {}", bufferServerAddress);
      }

      long endMillis = System.currentTimeMillis() + runMillis;

      while (!appDone) {

        for (String containerIdStr : dnmgr.containerStopRequests.values()) {
          // teardown child thread
          StreamingContainer c = childContainers.get(containerIdStr);
          if (c != null) {
            ContainerHeartbeatResponse r = new ContainerHeartbeatResponse();
            r.shutdown = StreamingContainerUmbilicalProtocol.ShutdownType.ABORT;
            c.processHeartbeatResponse(r);
          }
          dnmgr.containerStopRequests.remove(containerIdStr);
          LOG.info("Container {} restart.", containerIdStr);
          dnmgr.scheduleContainerRestart(containerIdStr);
          //dnmgr.removeContainerAgent(containerIdStr);
        }

        // start containers
        while (!dnmgr.containerStartRequests.isEmpty()) {
          ContainerStartRequest cdr = dnmgr.containerStartRequests.poll();
          if (cdr != null) {
            new LocalStreamingContainerLauncher(cdr, containerThreads);
          }
        }

        if (heartbeatMonitoringEnabled) {
          // monitor child containers
          dnmgr.monitorHeartbeat(false);
        }

        if (childContainers.isEmpty() && dnmgr.containerStartRequests.isEmpty()) {
          appDone = true;
        }

        if (runMillis > 0 && System.currentTimeMillis() > endMillis) {
          appDone = true;
        }

        try {
          if (exitCondition != null && exitCondition.call()) {
            LOG.info("Stopping on exit condition");
            appDone = true;
          }
        } catch (Exception ex) {
          break;
        }

        if (Thread.interrupted()) {
          break;
        }

        if (!appDone) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            LOG.debug("Sleep interrupted", e);
            break;
          }
        }
      }
    } finally {
      for (LocalStreamingContainer lsc : childContainers.values()) {
        injectShutdown.put(lsc.getContainerId(), lsc);
        lsc.triggerHeartbeat();
      }

      for (Thread thread : containerThreads) {
        try {
          thread.join(1000);
        } catch (InterruptedException e) {
          LOG.debug("Wait for {} to terminate interrupted", thread, e);
        }
        if (thread.isAlive()) {
          LOG.warn("Container thread {} is still alive", thread.getName());
        }
      }

      try {
        dnmgr.teardown();
      } catch (RuntimeException e) {
        LOG.warn("Exception during StreamingContainerManager teardown", e);
      }

      if (bufferServerAddress != null) {
        try {
          bufferServer.stop();
        } catch (RuntimeException e) {
          LOG.warn("Exception during BufferServer stop", e);
        }
      }

      if (eventLoopThread != null) {
        try {
          StreamingContainer.eventloop.stop();
          eventLoopThread.join(1000);
        } catch (InterruptedException ie) {
          LOG.debug("Wait for {} to terminate interrupted", eventLoopThread.getName(), ie);
        } catch (RuntimeException e) {
          LOG.warn("Exception during {} stop", StreamingContainer.eventloop, e);
        }
        if (StreamingContainer.eventloop.isActive()) {
          LOG.warn("Event loop {} is still active", StreamingContainer.eventloop);
        }
      }
    }
    LOG.info("Application finished.");
  }
}
