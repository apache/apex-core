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
package com.datatorrent.stram.engine;

import java.io.IOException;
import java.lang.Thread.State;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.security.TokenRenewer;
import org.apache.apex.log.LogFileInformation;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.LogManager;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StatsListener.OperatorRequest;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.StringCodec;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.bufferserver.server.Server;
import com.datatorrent.bufferserver.storage.DiskStorage;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.common.util.ScheduledThreadPoolExecutor;
import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.stram.ComponentContextPair;
import com.datatorrent.stram.RecoverableRpcProxy;
import com.datatorrent.stram.StramUtils;
import com.datatorrent.stram.StramUtils.YarnContainerMain;
import com.datatorrent.stram.StringCodecs;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.api.ContainerContext;
import com.datatorrent.stram.api.ContainerEvent;
import com.datatorrent.stram.api.ContainerEvent.ContainerStatsEvent;
import com.datatorrent.stram.api.ContainerEvent.NodeActivationEvent;
import com.datatorrent.stram.api.ContainerEvent.NodeDeactivationEvent;
import com.datatorrent.stram.api.ContainerEvent.StreamActivationEvent;
import com.datatorrent.stram.api.ContainerEvent.StreamDeactivationEvent;
import com.datatorrent.stram.api.OperatorDeployInfo;
import com.datatorrent.stram.api.OperatorDeployInfo.OperatorType;
import com.datatorrent.stram.api.OperatorDeployInfo.UnifierDeployInfo;
import com.datatorrent.stram.api.RequestFactory;
import com.datatorrent.stram.api.StramToNodeChangeLoggersRequest;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeat;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerStats;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat.DeployState;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.StreamingContainerContext;
import com.datatorrent.stram.debug.StdOutErrLog;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.Operators.PortContextPair;
import com.datatorrent.stram.plan.logical.Operators.PortMappingDescriptor;
import com.datatorrent.stram.plan.logical.StreamCodecWrapperForPersistance;
import com.datatorrent.stram.stream.BufferServerPublisher;
import com.datatorrent.stram.stream.BufferServerSubscriber;
import com.datatorrent.stram.stream.FastPublisher;
import com.datatorrent.stram.stream.FastSubscriber;
import com.datatorrent.stram.stream.InlineStream;
import com.datatorrent.stram.stream.MuxStream;
import com.datatorrent.stram.stream.OiOStream;
import com.datatorrent.stram.stream.PartitionAwareSink;
import com.datatorrent.stram.stream.PartitionAwareSinkForPersistence;
import com.datatorrent.stram.util.LoggerUtil;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.bus.config.BusConfiguration;

/**
 * Object which controls the container process launched by {@link com.datatorrent.stram.StreamingAppMaster}.
 *
 * @since 0.3.2
 */
public class StreamingContainer extends YarnContainerMain
{
  public static final String PROP_APP_PATH = StreamingApplication.DT_PREFIX + Context.DAGContext.APPLICATION_PATH.getName();
  private final transient String jvmName;
  private final String containerId;
  private final transient StreamingContainerUmbilicalProtocol umbilical;
  protected final Map<Integer, Node<?>> nodes = new ConcurrentHashMap<>();
  protected final Set<Integer> failedNodes = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
  private final Map<String, ComponentContextPair<Stream, StreamContext>> streams = new ConcurrentHashMap<>();
  protected final Map<Integer, WindowGenerator> generators = new ConcurrentHashMap<>();
  private transient List<GarbageCollectorMXBean> garbageCollectorMXBeans;
  /**
   * OIO groups map
   * key: operator id of oio owning thread node
   * value: list of nodes which are in oio with oio owning thread node
   */
  protected final Map<Integer, ArrayList<Integer>> oioGroups = new ConcurrentHashMap<>();
  private final Map<Stream, StreamContext> activeStreams = new ConcurrentHashMap<>();
  private final Map<WindowGenerator, Object> activeGenerators = new ConcurrentHashMap<>();
  private int heartbeatIntervalMillis = 1000;
  private volatile boolean exitHeartbeatLoop = false;
  private final Object heartbeatTrigger = new Object();
  public static DefaultEventLoop eventloop;
  /**
   * List of listeners interested in listening into the status change of the nodes.
   */
  private long firstWindowMillis;
  private int windowWidthMillis;
  protected InetSocketAddress bufferServerAddress;
  protected Server bufferServer;
  private int checkpointWindowCount;
  private boolean fastPublisherSubscriber;
  private StreamingContainerContext containerContext;
  private List<StramToNodeRequest> nodeRequests;
  private final HashMap<String, Object> singletons;
  private final MBassador<ContainerEvent> eventBus; // event bus for publishing container events
  HashSet<Component<ContainerContext>> components;
  private RequestFactory requestFactory;
  private TokenRenewer tokenRenewer;

  private Map<Integer, Node<?>> reuseOpNodes = new HashMap<>();

  static {
    try {
      eventloop = DefaultEventLoop.createEventLoop("ProcessWideEventLoop");
    } catch (IOException io) {
      throw new RuntimeException(io);
    }
  }

  protected StreamingContainer(String containerId, StreamingContainerUmbilicalProtocol umbilical)
  {
    this.jvmName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
    this.components = new HashSet<>();
    this.eventBus = new MBassador<>(BusConfiguration.Default(1, 1, 1));
    this.singletons = new HashMap<>();
    this.nodeRequests = new ArrayList<>();

    logger.debug("instantiated StramChild {}", containerId);
    this.umbilical = umbilical;
    this.containerId = containerId;
  }

  @SuppressWarnings("unchecked")
  public void setup(StreamingContainerContext ctx)
  {
    containerContext = ctx;

    /* add a request factory local to this container */
    this.requestFactory = new RequestFactory();
    ctx.attributes.put(ContainerContext.REQUEST_FACTORY, requestFactory);

    heartbeatIntervalMillis = ctx.getValue(Context.DAGContext.HEARTBEAT_INTERVAL_MILLIS);
    firstWindowMillis = ctx.startWindowMillis;
    windowWidthMillis = ctx.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS);
    checkpointWindowCount = ctx.getValue(Context.DAGContext.CHECKPOINT_WINDOW_COUNT);

    fastPublisherSubscriber = ctx.getValue(LogicalPlan.FAST_PUBLISHER_SUBSCRIBER);

    Map<Class<?>, Class<? extends StringCodec<?>>> codecs = ctx.getValue(Context.DAGContext.STRING_CODECS);
    StringCodecs.loadConverters(codecs);

    try {
      if (ctx.deployBufferServer) {
        eventloop.start();

        int bufferServerRAM = ctx.getValue(ContainerContext.BUFFER_SERVER_MB);
        logger.debug("buffer server memory {}", bufferServerRAM);
        int blockCount;
        int blocksize;
        if (bufferServerRAM < ContainerContext.BUFFER_SERVER_MB.defaultValue) {
          blockCount = 8;
          blocksize = bufferServerRAM / blockCount;
          if (blocksize < 1) {
            blocksize = 1;
          }
        } else {
          blocksize = 64;
          blockCount = bufferServerRAM / blocksize;
        }
        // start buffer server, if it was not set externally
        bufferServer = new Server(eventloop, 0, blocksize * 1024 * 1024, blockCount);
        bufferServer.setAuthToken(ctx.getValue(StreamingContainerContext.BUFFER_SERVER_TOKEN));
        if (ctx.getValue(Context.DAGContext.BUFFER_SPOOLING)) {
          bufferServer.setSpoolStorage(new DiskStorage());
        }
        bufferServerAddress = NetUtils.getConnectAddress(bufferServer.run());
        logger.debug("Buffer server started: {}", bufferServerAddress);
      }
    } catch (IOException ex) {
      logger.warn("deploy request failed due to {}", ex);
      throw new IllegalStateException("Failed to deploy buffer server", ex);
    }

    for (Class<?> clazz : ContainerEvent.CONTAINER_EVENTS_LISTENERS) {
      try {
        Object newInstance = clazz.newInstance();
        singletons.put(clazz.getName(), newInstance);

        if (newInstance instanceof Component) {
          components.add((Component<ContainerContext>)newInstance);
        }

        eventBus.subscribe(newInstance);
      } catch (InstantiationException ex) {
        logger.warn("Container Event Listener Instantiation", ex);
      } catch (IllegalAccessException ex) {
        logger.warn("Container Event Listener Instantiation", ex);
      }
    }

    operateListeners(ctx, true);
  }

  public String getContainerId()
  {
    return this.containerId;
  }

  /**
   * This method is introduced as replacement for getTupleRecorder method which was cluttering the code.
   *
   * @param classname
   * @return
   */
  public Object getInstance(String classname)
  {
    return singletons.get(classname);
  }

  /**
   * Initialize container. Establishes heartbeat connection to the master
   * distribute through the callback address provided on the command line. Deploys
   * initial modules, then enters the heartbeat loop, which will only terminate
   * once container receives shutdown request from the master. On shutdown,
   * after exiting heartbeat loop, shutdown all modules and terminate
   * processing threads.
   *
   * @param args
   * @throws Throwable
   */
  public static void main(String[] args) throws Throwable
  {
    LoggerUtil.setupMDC("worker");
    StdOutErrLog.tieSystemOutAndErrToLog();
    logger.debug("PID: " + System.getenv().get("JVM_PID"));
    logger.info("Child starting with classpath: {}", System.getProperty("java.class.path"));

    String appPath = System.getProperty(PROP_APP_PATH);
    if (appPath == null) {
      logger.error("{} not set in container environment.", PROP_APP_PATH);
      System.exit(1);
    }

    int exitStatus = 1; // interpreted as unrecoverable container failure

    RecoverableRpcProxy rpcProxy = null;
    StreamingContainerUmbilicalProtocol umbilical = null;
    final String childId = System.getProperty(StreamingApplication.DT_PREFIX + "cid");
    try {
      rpcProxy = new RecoverableRpcProxy(appPath, new Configuration());
      umbilical = rpcProxy.getProxy();
      StreamingContainerContext ctx = umbilical.getInitContext(childId);
      StreamingContainer stramChild = new StreamingContainer(childId, umbilical);
      logger.debug("Container Context = {}", ctx);
      stramChild.setup(ctx);
      try {
        /* main thread enters heartbeat loop */
        stramChild.heartbeatLoop();
        exitStatus = 0;
      } finally {
        stramChild.teardown();
      }
    } catch (Error | Exception e) {
      LogFileInformation logFileInfo = LoggerUtil.getLogFileInformation();
      logger.error("Fatal {} in container!", (e instanceof Error) ? "Error" : "Exception", e);
      /* Report back any failures, for diagnostic purposes */
      try {
        umbilical.reportError(childId, null, ExceptionUtils.getStackTrace(e), logFileInfo);
      } catch (Exception ex) {
        logger.debug("Fail to log", ex);
      }
    } finally {
      if (rpcProxy != null) {
        rpcProxy.close();
      }
      DefaultMetricsSystem.shutdown();
      logger.info("Exit status for container: {}", exitStatus);
      LogManager.shutdown();
      if (exitStatus != 0) {
        System.exit(exitStatus);
      }
    }
  }

  public synchronized void deactivate()
  {
    ArrayList<Thread> activeThreads = new ArrayList<>();
    ArrayList<Integer> activeOperators = new ArrayList<>();

    for (Map.Entry<Integer, Node<?>> e : nodes.entrySet()) {
      Thread t = e.getValue().context.getThread();
      if (t == null || !t.isAlive()) {
        disconnectNode(e.getKey());
      } else {
        activeThreads.add(t);
        activeOperators.add(e.getKey());
        e.getValue().shutdown();
      }
    }

    try {
      Iterator<Integer> iterator = activeOperators.iterator();
      for (Thread t : activeThreads) {
        t.join(1000);
        if (!t.getState().equals(State.TERMINATED)) {
          t.interrupt();
        }
        disconnectNode(iterator.next());
      }
    } catch (InterruptedException ex) {
      logger.warn("Aborting wait for operators to get deactivated!", ex);
    }

    for (WindowGenerator wg : activeGenerators.keySet()) {
      wg.deactivate();
    }
    activeGenerators.clear();

    for (Stream stream : activeStreams.keySet()) {
      stream.deactivate();
    }
    activeStreams.clear();
  }

  private void disconnectNode(int nodeid)
  {
    Node<?> node = nodes.get(nodeid);
    disconnectWindowGenerator(nodeid, node);

    PortMappingDescriptor portMappingDescriptor = node.getPortMappingDescriptor();
    Iterator<String> outputPorts = portMappingDescriptor.outputPorts.keySet().iterator();
    while (outputPorts.hasNext()) {
      String sourceIdentifier = String.valueOf(nodeid).concat(Component.CONCAT_SEPARATOR).concat(outputPorts.next());
      ComponentContextPair<Stream, StreamContext> pair = streams.remove(sourceIdentifier);
      if (pair != null) {
        if (activeStreams.remove(pair.component) != null) {
          pair.component.deactivate();
          eventBus.publish(new StreamDeactivationEvent(pair));
        }

        if (pair.component instanceof Stream.MultiSinkCapableStream) {
          String sinks = pair.context.getSinkId();
          if (sinks == null) {
            logger.error("mux sinks found connected at {} with sink id null", sourceIdentifier);
          } else {
            String[] split = sinks.split(MuxStream.MULTI_SINK_ID_CONCAT_SEPARATOR);
            for (int i = split.length; i-- > 0; ) {
              ComponentContextPair<Stream, StreamContext> spair = streams.remove(split[i]);
              if (spair == null) {
                logger.error("mux is missing the stream for sink {}", split[i]);
              } else {
                if (activeStreams.remove(spair.component) != null) {
                  spair.component.deactivate();
                  eventBus.publish(new StreamDeactivationEvent(spair));
                }

                spair.component.teardown();
              }
            }
          }
        } else {
          // it's either inline stream or it's bufferserver publisher.
        }

        pair.component.teardown();
      }
    }

    Iterator<String> inputPorts = portMappingDescriptor.inputPorts.keySet().iterator();
    while (inputPorts.hasNext()) {
      String sinkIdentifier = String.valueOf(nodeid).concat(Component.CONCAT_SEPARATOR).concat(inputPorts.next());
      ComponentContextPair<Stream, StreamContext> pair = streams.remove(sinkIdentifier);
      if (pair != null) {
        if (activeStreams.remove(pair.component) != null) {
          pair.component.deactivate();
          eventBus.publish(new StreamDeactivationEvent(pair));
        }

        pair.component.teardown();
        /**
         * we should also make sure that if this stream is connected to mux stream,
         * we deregister it from the mux stream to avoid clogged sink problem.
         */
        ComponentContextPair<Stream, StreamContext> sourcePair = streams.get(pair.context.getSourceId());
        if (sourcePair != null) {
          if (sourcePair == pair) {
            /* for some reason we had the stream stored against both source and sink identifiers */
            streams.remove(pair.context.getSourceId());
          } else {
            /* the stream was one of the many streams sourced by a muxstream */
            unregisterSinkFromMux(sourcePair, sinkIdentifier);
          }
        }
      }
    }
  }

  private boolean unregisterSinkFromMux(ComponentContextPair<Stream, StreamContext> muxpair, String sinkIdentifier)
  {
    String[] sinks = muxpair.context.getSinkId().split(MuxStream.MULTI_SINK_ID_CONCAT_SEPARATOR);
    boolean found = false;
    for (int i = sinks.length; i-- > 0; ) {
      if (sinks[i].equals(sinkIdentifier)) {
        sinks[i] = null;
        found = true;
        break;
      }
    }

    if (found) {
      ((Stream.MultiSinkCapableStream)muxpair.component).setSink(sinkIdentifier, null);

      if (sinks.length == 1) {
        muxpair.context.setSinkId(null);
        streams.remove(muxpair.context.getSourceId());
        if (activeStreams.remove(muxpair.component) != null) {
          muxpair.component.deactivate();
          eventBus.publish(new StreamDeactivationEvent(muxpair));
        }
        muxpair.component.teardown();
      } else {
        StringBuilder builder = new StringBuilder(muxpair.context.getSinkId().length() - MuxStream.MULTI_SINK_ID_CONCAT_SEPARATOR.length() - sinkIdentifier.length());

        found = false;
        for (int i = sinks.length; i-- > 0; ) {
          if (sinks[i] != null) {
            if (found) {
              builder.append(MuxStream.MULTI_SINK_ID_CONCAT_SEPARATOR).append(sinks[i]);
            } else {
              builder.append(sinks[i]);
              found = true;
            }
          }
        }

        muxpair.context.setSinkId(builder.toString());
      }
    } else {
      logger.error("{} was not connected to stream connected to {}", sinkIdentifier, muxpair.context.getSourceId());
    }

    return found;
  }

  private void disconnectWindowGenerator(int nodeid, Node<?> node)
  {
    WindowGenerator chosen1 = generators.remove(nodeid);
    if (chosen1 != null) {
      chosen1.releaseReservoir(Integer.toString(nodeid).concat(Component.CONCAT_SEPARATOR).concat(Node.INPUT));
      // should we send the closure of the port to the node?

      int count = 0;
      for (WindowGenerator wg : generators.values()) {
        if (chosen1 == wg) {
          count++;
        }
      }

      if (count == 0) {
        activeGenerators.remove(chosen1);
        chosen1.deactivate();
        chosen1.teardown();
      }
    }
  }

  private synchronized Map<Integer, Node<?>> undeploy(List<Integer> nodeList)
  {
    /**
     * make sure that all the operators which we are asked to undeploy are in this container.
     */
    HashMap<Integer, Node<?>> toUndeploy = new HashMap<>();
    for (Integer operatorId : nodeList) {
      Node<?> node = nodes.get(operatorId);
      if (node == null) {
        throw new IllegalArgumentException("Node " + operatorId + " is not hosted in this container!");
      } else if (toUndeploy.containsKey(operatorId)) {
        throw new IllegalArgumentException("Node " + operatorId + " is requested to be undeployed more than once");
      } else {
        toUndeploy.put(operatorId, node);
      }
    }

    ArrayList<Thread> joinList = new ArrayList<>();
    ArrayList<Integer> discoList = new ArrayList<>();
    for (Integer operatorId : nodeList) {
      Thread t = nodes.get(operatorId).context.getThread();
      if (t == null || !t.isAlive()) {
        disconnectNode(operatorId);
      } else {
        joinList.add(t);
        discoList.add(operatorId);
        nodes.get(operatorId).shutdown();
      }
    }

    try {
      Iterator<Integer> iterator = discoList.iterator();
      for (Thread t : joinList) {
        t.join(1000);
        if (!t.getState().equals(State.TERMINATED)) {
          t.interrupt();
        }
        disconnectNode(iterator.next());
      }
      logger.info("Undeploy complete.");
    } catch (InterruptedException ex) {
      logger.warn("Aborting wait for operators to get deactivated!", ex);
    }

    for (Integer operatorId : nodeList) {
      nodes.remove(operatorId);
    }

    return toUndeploy;
  }

  public void teardown()
  {
    operateListeners(containerContext, false);

    deactivate();

    assert (streams.isEmpty());

    eventBus.shutdown();

    nodes.clear();

    HashSet<WindowGenerator> gens = new HashSet<>();
    gens.addAll(generators.values());
    generators.clear();
    for (WindowGenerator wg : gens) {
      wg.teardown();
    }

    if (bufferServer != null) {
      bufferServer.stop();
      eventloop.stop();
    }

    gens.clear();
  }

  public void triggerHeartbeat()
  {
    synchronized (heartbeatTrigger) {
      heartbeatTrigger.notifyAll();
    }
  }

  public void heartbeatLoop() throws Exception
  {
    logger.debug("Entering heartbeat loop (interval is {} ms)", this.heartbeatIntervalMillis);
    umbilical.log(containerId, "[" + containerId + "] Entering heartbeat loop..");
    final YarnConfiguration conf = new YarnConfiguration();
    if (UserGroupInformation.isSecurityEnabled()) {
      tokenRenewer = new TokenRenewer(containerContext, false, conf, containerId);
    }
    String stackTrace = null;
    while (!exitHeartbeatLoop) {

      if (tokenRenewer != null) {
        tokenRenewer.checkAndRenew();
      }

      synchronized (this.heartbeatTrigger) {
        try {
          this.heartbeatTrigger.wait(heartbeatIntervalMillis);
        } catch (InterruptedException e1) {
          logger.warn("Interrupted in heartbeat loop, exiting..");
          break;
        }
      }

      long currentTime = System.currentTimeMillis();
      ContainerHeartbeat msg = new ContainerHeartbeat();
      msg.jvmName = jvmName;
      if (this.bufferServerAddress != null) {
        msg.bufferServerHost = this.bufferServerAddress.getHostName();
        msg.bufferServerPort = this.bufferServerAddress.getPort();
        if (bufferServer != null && !eventloop.isActive()) {
          logger.warn("Requesting restart due to terminated event loop");
          msg.restartRequested = true;
        }
      }
      msg.memoryMBFree = ((int)(Runtime.getRuntime().freeMemory() / (1024 * 1024)));
      garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
      for (GarbageCollectorMXBean bean : garbageCollectorMXBeans) {
        msg.gcCollectionTime += bean.getCollectionTime();
        msg.gcCollectionCount += bean.getCollectionCount();
      }

      ContainerHeartbeatResponse rsp;
      do {

        ContainerStats stats = new ContainerStats(containerId);
        // gather heartbeat info for all operators
        for (Map.Entry<Integer, Node<?>> e : nodes.entrySet()) {
          OperatorHeartbeat hb = new OperatorHeartbeat();
          hb.setNodeId(e.getKey());
          hb.setGeneratedTms(currentTime);
          hb.setIntervalMs(heartbeatIntervalMillis);
          if (e.getValue().commandResponse.size() > 0) {
            BlockingQueue<StatsListener.OperatorResponse> commandResponse = e.getValue().commandResponse;
            ArrayList<StatsListener.OperatorResponse> response = new ArrayList<>();
            for (int i = 0; i < commandResponse.size(); i++) {
              response.add(commandResponse.poll());
            }
            hb.requestResponse = response;
          }
          OperatorContext context = e.getValue().context;
          context.drainStats(hb.getOperatorStatsContainer());

          if (context.getThread() == null || context.getThread().getState() != Thread.State.TERMINATED) {
            hb.setState(DeployState.ACTIVE);
          } else if (failedNodes.contains(hb.nodeId)) {
            hb.setState(DeployState.FAILED);
          } else {
            logger.debug("Reporting SHUTDOWN state because thread is {} and failedNodes is {}", context.getThread(), failedNodes);
            hb.setState(DeployState.SHUTDOWN);
          }

          stats.addNodeStats(hb);
        }

        /**
         * Container stats published for whoever is interested in listening.
         * Currently interested candidates are TupleRecorderCollection and BufferServerStatsSubscriber
         */
        eventBus.publish(new ContainerStatsEvent(stats));

        msg.setContainerStats(stats);

        // heartbeat call and follow-up processing
        //logger.debug("Sending heartbeat for {} operators.", msg.getContainerStats().size());
        msg.sentTms = System.currentTimeMillis();

        msg.stackTrace = stackTrace;

        rsp = umbilical.processHeartbeat(msg);

        if (rsp.stackTraceRequired) {
          stackTrace = StramUtils.getStackTrace().toString();
        } else {
          stackTrace = null;
        }

        processHeartbeatResponse(rsp);

        if (rsp.hasPendingRequests) {
          logger.info("Waiting for pending request.");
          synchronized (this.heartbeatTrigger) {
            try {
              this.heartbeatTrigger.wait(500);
            } catch (InterruptedException ie) {
              logger.warn("Interrupted in heartbeat loop", ie);
              break;
            }
          }
        }
      } while (rsp.hasPendingRequests);

    }
    logger.debug("[{}] Exiting heartbeat loop", containerId);
    umbilical.log(containerId, "[" + containerId + "] Exiting heartbeat loop..");
  }

  private long lastCommittedWindowId = WindowGenerator.MIN_WINDOW_ID - 1;

  private void processNodeRequests(boolean flagInvalid)
  {
    for (StramToNodeRequest req : nodeRequests) {
      if (req.isDeleted()) {
        continue;
      }
      if (req instanceof StramToNodeChangeLoggersRequest) {
        handleChangeLoggersRequest((StramToNodeChangeLoggersRequest)req);
        continue;
      }

      Node<?> node = nodes.get(req.getOperatorId());
      if (node == null) {
        logger.warn("Node for operator {} is not found, probably not deployed yet", req.getOperatorId());
        continue;
      }

      Thread thread = node.context.getThread();
      if (thread == null || !thread.isAlive()) {
        if (flagInvalid) {
          logger.warn("Received request with invalid operator id {} ({})", req.getOperatorId(), req);
          req.setDeleted(true);
        }
      } else {
        logger.debug("request received: {}", req);
        OperatorRequest requestExecutor = requestFactory.getRequestExecutor(nodes.get(req.operatorId), req);
        if (requestExecutor != null) {
          node.context.request(requestExecutor);
        } else {
          logger.warn("No executor identified for the request {}", req);
        }
        req.setDeleted(true);
      }
    }
  }

  public void processHeartbeatResponse(ContainerHeartbeatResponse rsp)
  {
    if (rsp.nodeRequests != null) {
      nodeRequests = rsp.nodeRequests;
    }

    if (rsp.committedWindowId != lastCommittedWindowId) {

      lastCommittedWindowId = rsp.committedWindowId;

      if (bufferServer != null) {
        //  One Window before the committed Window is kept in the Buffer Server, for historical reasons.
        // Jira for that issue is APEXCORE-479
        bufferServer.purge(lastCommittedWindowId - 1);
      }

      OperatorRequest nr = null;
      for (Entry<Integer, Node<?>> e : nodes.entrySet()) {
        final Thread thread = e.getValue().context.getThread();
        if (thread == null || !thread.isAlive()) {
          continue;
        }

        if (e.getValue().getOperator() instanceof Operator.CheckpointListener) {
          if (nr == null) {
            nr = new OperatorRequest()
            {
              @Override
              public StatsListener.OperatorResponse execute(Operator operator, int operatorId, long windowId) throws IOException
              {
                ((Operator.CheckpointListener)operator).committed(lastCommittedWindowId);
                return null;
              }

            };
          }
          e.getValue().context.request(nr);
        }
      }
    }

    if (rsp.undeployRequest != null) {
      logger.info("Undeploy request: {}", rsp.undeployRequest);
      processNodeRequests(false);
      Map<Integer, Node<?>> undeployNodes = undeploy(rsp.undeployRequest);
      undeployNodes.entrySet().removeIf((entry) -> !isReuseOperator(entry.getValue()));
      reuseOpNodes.putAll(undeployNodes);
    }

    if (rsp.shutdown != null) {
      logger.info("Received shutdown request type {}", rsp.shutdown);
      if (rsp.shutdown == StreamingContainerUmbilicalProtocol.ShutdownType.ABORT) {
        processNodeRequests(false);
        this.exitHeartbeatLoop = true;
        return;
      } else if (rsp.shutdown == StreamingContainerUmbilicalProtocol.ShutdownType.WAIT_TERMINATE) {
        stopInputNodes();
      }
    }

    if (rsp.deployRequest != null) {
      logger.info("Deploy request: {}", rsp.deployRequest);
      try {
        deploy(rsp.deployRequest);
      } catch (Exception e) {
        logger.error("deploy request failed", e);
        try {
          umbilical.log(this.containerId, "deploy request failed: " + rsp.deployRequest + " " + ExceptionUtils.getStackTrace(e));
        } catch (IOException ioe) {
          logger.debug("Fail to log", ioe);
        }
        this.exitHeartbeatLoop = true;
        throw new IllegalStateException("Deploy request failed: " + rsp.deployRequest, e);
      }
    }

    processNodeRequests(true);
  }

  private boolean isReuseOperator(Node<?> node)
  {
    if (node.context.getAttributes().contains(OperatorContext.RECOVERY_MODE)) {
      return node.context.getValue(OperatorContext.RECOVERY_MODE) == Operator.RecoveryMode.REUSE_INSTANCE;
    } else {
      if (node.operator.getClass().isAnnotationPresent(OperatorAnnotation.class)) {
        return node.operator.getClass().getAnnotation(OperatorAnnotation.class).recoveryMode() == Operator.RecoveryMode.REUSE_INSTANCE;
      }
    }
    logger.debug("Is reuse operator {} {}", node, false);
    return false;
  }

  private void stopInputNodes()
  {
    for (Entry<Integer, Node<?>> e : nodes.entrySet()) {
      Node<?> node = e.getValue();
      if (node instanceof InputNode) {
        final Thread thread = e.getValue().context.getThread();
        if (thread == null || !thread.isAlive()) {
          continue;
        }
        node.shutdown(true);
      }
    }
  }

  private int getOutputQueueCapacity(List<OperatorDeployInfo> operatorList, int sourceOperatorId, String sourcePortName)
  {
    for (OperatorDeployInfo odi : operatorList) {
      if (odi.id == sourceOperatorId) {
        for (OperatorDeployInfo.OutputDeployInfo odiodi : odi.outputs) {
          if (odiodi.portName.equals(sourcePortName)) {
            return getValue(PortContext.QUEUE_CAPACITY, odiodi, odi);
          }
        }
      }
    }

    return PortContext.QUEUE_CAPACITY.defaultValue;
  }

  private synchronized void deploy(List<OperatorDeployInfo> nodeList) throws Exception
  {
    /*
     * A little bit of up front sanity check would reduce the percentage of deploy failures later.
     */
    for (OperatorDeployInfo ndi : nodeList) {
      if (nodes.containsKey(ndi.id)) {
        throw new IllegalStateException("Node with id: " + ndi.id + " already present in container " + containerId + "!");
      }
    }

    deployNodes(nodeList);

    HashMap<String, ArrayList<String>> groupedInputStreams = new HashMap<>();
    for (OperatorDeployInfo ndi : nodeList) {
      groupInputStreams(groupedInputStreams, ndi);
    }

    HashMap<String, ComponentContextPair<Stream, StreamContext>> newStreams = deployOutputStreams(nodeList, groupedInputStreams);
    deployInputStreams(nodeList, newStreams);
    for (ComponentContextPair<Stream, StreamContext> pair : newStreams.values()) {
      pair.component.setup(pair.context);
    }
    streams.putAll(newStreams);

    HashMap<Integer, OperatorDeployInfo> operatorMap = new HashMap<>(nodeList.size());
    for (OperatorDeployInfo o : nodeList) {
      operatorMap.put(o.id, o);
    }
    activate(operatorMap, newStreams);
  }

  public static String getUnifierInputPortName(String portName, int sourceNodeId, String sourcePortName)
  {
    return portName + "(" + sourceNodeId + Component.CONCAT_SEPARATOR + sourcePortName + ")";
  }

  private void massageUnifierDeployInfo(OperatorDeployInfo odi)
  {
    for (OperatorDeployInfo.InputDeployInfo idi : odi.inputs) {
      idi.portName = getUnifierInputPortName(idi.portName, idi.sourceNodeId, idi.sourcePortName);
    }
  }

  private void deployNodes(List<OperatorDeployInfo> nodeList) throws IOException
  {
    for (OperatorDeployInfo ndi : nodeList) {
      StorageAgent backupAgent = getValue(OperatorContext.STORAGE_AGENT, ndi);
      assert (backupAgent != null);

      Context parentContext;
      if (ndi instanceof UnifierDeployInfo) {
        OperatorContext unifiedOperatorContext = new OperatorContext(0, ndi.name,
            ((UnifierDeployInfo)ndi).operatorAttributes, containerContext);
        parentContext = new PortContext(ndi.inputs.get(0).contextAttributes, unifiedOperatorContext);
        massageUnifierDeployInfo(ndi);
      } else {
        parentContext = containerContext;
      }

      OperatorContext ctx = new OperatorContext(ndi.id, ndi.name, ndi.contextAttributes, parentContext);
      ctx.attributes.put(OperatorContext.ACTIVATION_WINDOW_ID, ndi.checkpoint.windowId);
      Node<?> node = reuseOpNodes.get(ndi.id);
      if (node == null) {
        logger.info("Restoring operator {} to checkpoint {} stateless={}.", ndi.id, Codec.getStringWindowId(ndi.checkpoint.windowId), ctx.stateless);
        node = Node.retrieveNode(backupAgent.load(ndi.id, ctx.stateless ? Stateless.WINDOW_ID : ndi.checkpoint.windowId), ctx, ndi.type);
      } else {
        logger.info("Reusing previous operator instance {}", ndi.id);
        node = Node.retrieveNode(node.operator, ctx, ndi.type);
        node.setReuseOperator(true);
        reuseOpNodes.remove(ndi.id);
      }
      node.currentWindowId = ndi.checkpoint.windowId;
      node.applicationWindowCount = ndi.checkpoint.applicationWindowCount;
      node.firstWindowMillis = firstWindowMillis;
      node.windowWidthMillis = windowWidthMillis;

      node.setId(ndi.id);
      nodes.put(ndi.id, node);
      logger.debug("Marking operator {} as deployed.", node);
    }
  }

  private HashMap.SimpleEntry<String, ComponentContextPair<Stream, StreamContext>> deployBufferServerPublisher(
      String connIdentifier, StreamCodec<?> streamCodec, long finishedWindowId, int queueCapacity,
      OperatorDeployInfo.OutputDeployInfo nodi)
      throws UnknownHostException
  {
    String sinkIdentifier = "tcp://".concat(nodi.bufferServerHost).concat(":").concat(String.valueOf(nodi.bufferServerPort)).concat("/").concat(connIdentifier);

    StreamContext bssc = new StreamContext(nodi.declaredStreamId);
    bssc.setPortId(nodi.portName);
    bssc.setSourceId(connIdentifier);
    bssc.setSinkId(sinkIdentifier);
    bssc.setFinishedWindowId(finishedWindowId);
    bssc.put(StreamContext.CODEC, streamCodec);
    bssc.put(StreamContext.EVENT_LOOP, eventloop);
    bssc.setBufferServerAddress(InetSocketAddress.createUnresolved(nodi.bufferServerHost, nodi.bufferServerPort));
    bssc.put(StreamContext.BUFFER_SERVER_TOKEN, nodi.bufferServerToken);
    InetAddress inetAddress = bssc.getBufferServerAddress().getAddress();
    if (inetAddress != null && NetUtils.isLocalAddress(inetAddress)) {
      bssc.setBufferServerAddress(new InetSocketAddress(InetAddress.getByName(null), nodi.bufferServerPort));
    }

    Stream publisher = fastPublisherSubscriber ? new FastPublisher(connIdentifier, queueCapacity * 256) : new BufferServerPublisher(connIdentifier, queueCapacity);
    return new HashMap.SimpleEntry<>(sinkIdentifier, new ComponentContextPair<>(publisher, bssc));
  }

  private HashMap<String, ComponentContextPair<Stream, StreamContext>> deployOutputStreams(
      List<OperatorDeployInfo> nodeList, HashMap<String, ArrayList<String>> groupedInputStreams)
      throws Exception
  {
    HashMap<String, ComponentContextPair<Stream, StreamContext>> newStreams = new HashMap<>();
    /*
     * We proceed to deploy all the output streams. At the end of this block, our streams collection
     * will contain all the streams which originate at the output port of the operators. The streams
     * are generally mapped against the "nodename.portname" string. But the BufferServerPublishers which
     * share the output port with other inline streams are mapped against the Buffer Server port to
     * avoid collision and at the same time keep track of these buffer streams.
     */
    for (OperatorDeployInfo ndi : nodeList) {
      Node<?> node = nodes.get(ndi.id);
      long checkpointWindowId = ndi.checkpoint.windowId;

      for (OperatorDeployInfo.OutputDeployInfo nodi : ndi.outputs) {
        String sourceIdentifier = Integer.toString(ndi.id).concat(Component.CONCAT_SEPARATOR).concat(nodi.portName);
        int queueCapacity = getValue(PortContext.QUEUE_CAPACITY, nodi, ndi);
        logger.debug("for stream {} the queue capacity is {}", sourceIdentifier, queueCapacity);

        ArrayList<String> collection = groupedInputStreams.get(sourceIdentifier);
        Map<Integer, StreamCodec<?>> streamCodecs = nodi.streamCodecs;
        if ((collection == null) && (streamCodecs.size() == 1)) {
          assert (nodi.bufferServerHost != null) : "resulting stream cannot be inline: " + nodi;
          /*
           * Let's create a stream to carry the data to the Buffer Server.
           * Nobody in this container is interested in the output placed on this stream, but
           * this stream exists. That means someone outside of this container must be interested.
           */
          Map.Entry<Integer, StreamCodec<?>> entry = streamCodecs.entrySet().iterator().next();
          StreamCodec<?> streamCodec = entry.getValue();
          Integer streamCodecIdentifier = entry.getKey();
          String connIdentifier = sourceIdentifier + Component.CONCAT_SEPARATOR + streamCodecIdentifier;

          SimpleEntry<String, ComponentContextPair<Stream, StreamContext>> deployBufferServerPublisher =
              deployBufferServerPublisher(connIdentifier, streamCodec, checkpointWindowId, queueCapacity, nodi);
          newStreams.put(sourceIdentifier, deployBufferServerPublisher.getValue());
          node.connectOutputPort(nodi.portName, deployBufferServerPublisher.getValue().component);
        } else {
          /*
           * In this case we have 2 possibilities, either we have 1 inline or multiple streams.
           * Since we cannot tell at this point, we assume that we will have multiple streams and
           * plan accordingly. we possibly will come to this code block multiple times. We create
           * the MuxStream only the first time and use it for subsequent calls of this block.
           *
           * There is also the possibility that we have a stream with multiple sinks having distinct codecs
           */
          ComponentContextPair<Stream, StreamContext> pair = newStreams.get(sourceIdentifier);
          if (pair == null) {
            /**
             * Let's multiplex the output placed on this stream.
             * This container itself contains more than one parties interested.
             */
            StreamContext context = new StreamContext(nodi.declaredStreamId);
            context.setSourceId(sourceIdentifier);
            context.setFinishedWindowId(checkpointWindowId);
            Stream stream = new MuxStream();

            newStreams.put(sourceIdentifier, pair = new ComponentContextPair<>(stream, context));
            node.connectOutputPort(nodi.portName, stream);
          }

          if (nodi.bufferServerHost != null) {
            /*
             * Although there is a node in this container interested in output placed on this stream, there
             * seems to at least one more party interested but placed in a container other than this one.
             */
            for (Map.Entry<Integer, StreamCodec<?>> entry : streamCodecs.entrySet()) {
              Integer streamCodecIdentifier = entry.getKey();
              StreamCodec<?> streamCodec = entry.getValue();

              String connIdentifier = sourceIdentifier + Component.CONCAT_SEPARATOR + streamCodecIdentifier;

              SimpleEntry<String, ComponentContextPair<Stream, StreamContext>> deployBufferServerPublisher =
                  deployBufferServerPublisher(connIdentifier, streamCodec, checkpointWindowId, queueCapacity, nodi);
              newStreams.put(deployBufferServerPublisher.getKey(), deployBufferServerPublisher.getValue());

              String sinkIdentifier = pair.context.getSinkId();
              if (sinkIdentifier == null) {
                pair.context.setSinkId(deployBufferServerPublisher.getKey());
              } else {
                pair.context.setSinkId(sinkIdentifier.concat(", ").concat(deployBufferServerPublisher.getKey()));
              }

              ((Stream.MultiSinkCapableStream)pair.component).setSink(deployBufferServerPublisher.getKey(), deployBufferServerPublisher.getValue().component);
            }
          }
        }
      }
    }

    return newStreams;
  }

  /**
   * If the port is connected, return the declared stream Id.
   *
   * @param operatorId id of the operator to which the port belongs.
   * @param portname   name of port to which the stream is connected.
   * @return Stream Id if connected, null otherwise.
   */
  public final String getDeclaredStreamId(int operatorId, String portname)
  {
    String identifier = String.valueOf(operatorId).concat(Component.CONCAT_SEPARATOR).concat(portname);
    ComponentContextPair<Stream, StreamContext> spair = streams.get(identifier);
    if (spair == null) {
      return null;
    }

    return spair.context.getId();
  }

  @SuppressWarnings("unchecked")
  private void deployInputStreams(List<OperatorDeployInfo> operatorList, HashMap<String, ComponentContextPair<Stream, StreamContext>> newStreams) throws UnknownHostException
  {
    /*
     * collect any input operators along with their smallest window id,
     * those are subsequently used to setup the window generator
     */
    ArrayList<OperatorDeployInfo> inputNodes = new ArrayList<>();
    long smallestCheckpointedWindowId = Long.MAX_VALUE;
    //a simple map which maps the oio node to it's the node which owns the thread.
    Map<Integer, Integer> oioNodes = new ConcurrentHashMap<>();

    /*
     * Hook up all the downstream ports. There are 2 places where we deal with more than 1
     * downstream ports. The first one follows immediately for WindowGenerator. The second
     * case is when source for the input port of some node in this container is in another
     * container. So we need to create the stream. We need to track this stream along with
     * other streams,and many such streams may exist, we hash them against buffer server
     * info as we did for outputs but throw in the sinkid in the mix as well.
     */
    for (OperatorDeployInfo ndi : operatorList) {
      if (ndi.inputs == null || ndi.inputs.isEmpty()) {
        /*
         * This has to be InputNode, so let's hook the WindowGenerator to it.
         * A node which does not take any input cannot exist in the DAG since it would be completely
         * unaware of the windows. So for that reason, AbstractInputNode allows Component.INPUT port.
         */
        inputNodes.add(ndi);
        /*
         * When we activate the window Generator, we plan to activate it only from required windowId.
         */
        ndi.checkpoint = getFinishedCheckpoint(ndi);
        if (ndi.checkpoint.windowId < smallestCheckpointedWindowId) {
          smallestCheckpointedWindowId = ndi.checkpoint.windowId;
        }
      } else {
        Node<?> node = nodes.get(ndi.id);

        for (OperatorDeployInfo.InputDeployInfo nidi : ndi.inputs) {
          if (nidi.streamCodecs.size() != 1) {
            throw new IllegalStateException("Only one input codec configuration should be present");
          }
          Map.Entry<Integer, StreamCodec<?>> entry = nidi.streamCodecs.entrySet().iterator().next();
          Integer streamCodecIdentifier = entry.getKey();
          StreamCodec<?> streamCodec = entry.getValue();
          String sourceIdentifier = Integer.toString(nidi.sourceNodeId).concat(Component.CONCAT_SEPARATOR).concat(nidi.sourcePortName);
          String sinkIdentifier = Integer.toString(ndi.id).concat(Component.CONCAT_SEPARATOR).concat(nidi.portName);

          int queueCapacity = getValue(PortContext.QUEUE_CAPACITY, nidi, ndi);

          Checkpoint checkpoint = getFinishedCheckpoint(ndi);
          ComponentContextPair<Stream, StreamContext> pair = streams.get(sourceIdentifier);
          if (pair == null) {
            pair = newStreams.get(sourceIdentifier);
          }

          if (pair == null) {
            /*
             * We connect to the buffer server for the input on this port.
             * We have already placed all the output streams for all the operators in this container.
             * Yet, there is no stream which can source this port so it has to come from the buffer
             * server, so let's make a connection to it.
             */
            assert (nidi.locality != Locality.CONTAINER_LOCAL && nidi.locality != Locality.THREAD_LOCAL);

            StreamContext context = new StreamContext(nidi.declaredStreamId);
            context.setBufferServerAddress(InetSocketAddress.createUnresolved(nidi.bufferServerHost, nidi.bufferServerPort));
            InetAddress inetAddress = context.getBufferServerAddress().getAddress();
            if (inetAddress != null && NetUtils.isLocalAddress(inetAddress)) {
              context.setBufferServerAddress(new InetSocketAddress(InetAddress.getByName(null), nidi.bufferServerPort));
            }
            context.put(StreamContext.BUFFER_SERVER_TOKEN, nidi.bufferServerToken);
            String connIdentifier = sourceIdentifier + Component.CONCAT_SEPARATOR + streamCodecIdentifier;
            context.setPortId(nidi.portName);
            context.put(StreamContext.CODEC, streamCodec);
            context.put(StreamContext.EVENT_LOOP, eventloop);
            context.setPartitions(nidi.partitionMask, nidi.partitionKeys);
            //context.setSourceId(sourceIdentifier);
            context.setSourceId(connIdentifier);
            context.setSinkId(sinkIdentifier);
            context.setFinishedWindowId(checkpoint.windowId);

            BufferServerSubscriber subscriber = fastPublisherSubscriber
                ? new FastSubscriber("tcp://".concat(nidi.bufferServerHost).concat(":").concat(String.valueOf(nidi.bufferServerPort)).concat("/").concat(connIdentifier), queueCapacity)
                : new BufferServerSubscriber("tcp://".concat(nidi.bufferServerHost).concat(":").concat(String.valueOf(nidi.bufferServerPort)).concat("/").concat(connIdentifier), queueCapacity);
            if (streamCodec instanceof StreamCodecWrapperForPersistance) {
              subscriber.acquireReservoirForPersistStream(sinkIdentifier, queueCapacity, streamCodec);
            }
            SweepableReservoir reservoir = subscriber.acquireReservoir(sinkIdentifier, queueCapacity);
            if (checkpoint.windowId >= 0) {
              node.connectInputPort(nidi.portName, new WindowIdActivatedReservoir(sinkIdentifier, reservoir, checkpoint.windowId));
            }
            node.connectInputPort(nidi.portName, reservoir);

            newStreams.put(sinkIdentifier, new ComponentContextPair<Stream, StreamContext>(subscriber, context));
            logger.debug("put input stream {} against key {}", subscriber, sinkIdentifier);
          } else {
            assert (nidi.locality == Locality.CONTAINER_LOCAL || nidi.locality == Locality.THREAD_LOCAL);
            /* we are still dealing with the MuxStream originating at the output of the source port */
            StreamContext inlineContext = new StreamContext(nidi.declaredStreamId);
            inlineContext.setSourceId(sourceIdentifier);
            inlineContext.setSinkId(sinkIdentifier);

            Stream stream;
            SweepableReservoir reservoir;
            switch (nidi.locality) {
              case CONTAINER_LOCAL:
                int outputQueueCapacity = getOutputQueueCapacity(operatorList, nidi.sourceNodeId, nidi.sourcePortName);
                if (outputQueueCapacity > queueCapacity) {
                  queueCapacity = outputQueueCapacity;
                }

                stream = new InlineStream(queueCapacity);
                reservoir = ((InlineStream)stream).getReservoir();
                if (checkpoint.windowId >= 0) {
                  node.connectInputPort(nidi.portName, new WindowIdActivatedReservoir(sinkIdentifier, reservoir,
                      checkpoint.windowId));
                }
                break;

              case THREAD_LOCAL:
                stream = new OiOStream();
                reservoir = ((OiOStream)stream).getReservoir();
                ((OiOStream.OiOReservoir)reservoir).setControlSink(((OiONode)node).getControlSink(reservoir));
                oioNodes.put(ndi.id, nidi.sourceNodeId);
                break;

              default:
                throw new IllegalStateException("Locality can be either ContainerLocal or ThreadLocal");
            }

            node.connectInputPort(nidi.portName, reservoir);
            newStreams.put(sinkIdentifier, new ComponentContextPair<>(stream, inlineContext));

            if (!(pair.component instanceof Stream.MultiSinkCapableStream)) {
              String originalSinkId = pair.context.getSinkId();

              /* we come here only if we are trying to augment the dag */
              StreamContext muxContext = new StreamContext(nidi.declaredStreamId);
              muxContext.setSourceId(sourceIdentifier);
              muxContext.setFinishedWindowId(checkpoint.windowId);
              muxContext.setSinkId(originalSinkId);

              MuxStream muxStream = new MuxStream();
              muxStream.setSink(originalSinkId, pair.component);
              streams.put(originalSinkId, pair);

              Node<?> sourceNode = nodes.get(nidi.sourceNodeId);
              sourceNode.connectOutputPort(nidi.sourcePortName, muxStream);
              newStreams.put(sourceIdentifier, pair = new ComponentContextPair<Stream, StreamContext>(muxStream, muxContext));
            }

            /* here everything should be multisink capable */
            if (streamCodec instanceof StreamCodecWrapperForPersistance) {
              PartitionAwareSinkForPersistence pas;
              if (nidi.partitionKeys == null) {
                pas = new PartitionAwareSinkForPersistence((StreamCodecWrapperForPersistance<Object>)streamCodec, nidi.partitionMask, stream);
              } else {
                pas = new PartitionAwareSinkForPersistence((StreamCodecWrapperForPersistance<Object>)streamCodec, nidi.partitionKeys, nidi.partitionMask, stream);
              }
              ((Stream.MultiSinkCapableStream)pair.component).setSink(sinkIdentifier, pas);
            } else if (nidi.partitionKeys == null || nidi.partitionKeys.isEmpty()) {
              ((Stream.MultiSinkCapableStream)pair.component).setSink(sinkIdentifier, stream);
            } else {
              /*
               * generally speaking we do not have partitions on the inline streams so the control should not
               * come here but if it comes, then we are ready to handle it using the partition aware streams.
               */
              PartitionAwareSink<Object> pas = new PartitionAwareSink<>(streamCodec == null ? nonSerializingStreamCodec : (StreamCodec<Object>)streamCodec, nidi.partitionKeys, nidi.partitionMask, stream);
              ((Stream.MultiSinkCapableStream)pair.component).setSink(sinkIdentifier, pas);
            }

            String streamSinkId = pair.context.getSinkId();
            if (streamSinkId == null) {
              pair.context.setSinkId(sinkIdentifier);
            } else {
              pair.context.setSinkId(streamSinkId.concat(", ").concat(sinkIdentifier));
            }
          }
        }
      }
    }

    setupOiOGroups(oioNodes);

    if (!inputNodes.isEmpty()) {
      WindowGenerator windowGenerator = setupWindowGenerator(smallestCheckpointedWindowId);
      for (OperatorDeployInfo ndi : inputNodes) {
        generators.put(ndi.id, windowGenerator);

        Node<?> node = nodes.get(ndi.id);
        SweepableReservoir reservoir = windowGenerator.acquireReservoir(String.valueOf(ndi.id), 1024);
        if (ndi.checkpoint.windowId >= 0) {
          node.connectInputPort(Node.INPUT, new WindowIdActivatedReservoir(Integer.toString(ndi.id), reservoir, ndi.checkpoint.windowId));
        }
        node.connectInputPort(Node.INPUT, reservoir);
      }
    }

  }

  /**
   * Populates oioGroups with owner OIO Node as key and list of corresponding OIO nodes which will run in its thread as value
   * This method assumes that the DAG is valid as per OIO constraints
   */
  private void setupOiOGroups(Map<Integer, Integer> oioNodes)
  {
    for (Integer child : oioNodes.keySet()) {
      Integer oioParent = oioNodes.get(child);

      Integer temp;
      while ((temp = oioNodes.get(oioParent)) != null) {
        oioParent = temp;
      }

      ArrayList<Integer> children = oioGroups.get(oioParent);
      if (children == null) {
        oioGroups.put(oioParent, children = new ArrayList<>());
      }
      children.add(child);
    }
  }

  /**
   * Create the window generator for the given start window id.
   * This is a hook for tests to control the window generation.
   *
   * @param finishedWindowId
   * @return WindowGenerator
   */
  protected WindowGenerator setupWindowGenerator(long finishedWindowId)
  {
    WindowGenerator windowGenerator = new WindowGenerator(new ScheduledThreadPoolExecutor(1, "WindowGenerator"), 1024);
    /**
     * let's make sure that we send the same window Ids with the same reset windows.
     */
    windowGenerator.setResetWindow(firstWindowMillis);

    long millisAtFirstWindow = WindowGenerator.getNextWindowMillis(finishedWindowId, firstWindowMillis, windowWidthMillis);
    windowGenerator.setFirstWindow(millisAtFirstWindow);
    windowGenerator.setWindowWidth(windowWidthMillis);

    long windowCount = WindowGenerator.getWindowCount(millisAtFirstWindow, firstWindowMillis, windowWidthMillis);
    windowGenerator.setCheckpointCount(checkpointWindowCount, (int)(windowCount % checkpointWindowCount));
    return windowGenerator;
  }

  private void setupNode(OperatorDeployInfo ndi)
  {
    failedNodes.remove(ndi.id);
    final Node<?> node = nodes.get(ndi.id);

    node.setup(node.context);

    /* setup context for all the input ports */
    LinkedHashMap<String, PortContextPair<InputPort<?>>> inputPorts = node.getPortMappingDescriptor().inputPorts;
    LinkedHashMap<String, PortContextPair<InputPort<?>>> newInputPorts = new LinkedHashMap<>(inputPorts.size());
    for (OperatorDeployInfo.InputDeployInfo idi : ndi.inputs) {
      InputPort<?> port = inputPorts.get(idi.portName).component;
      PortContext context = new PortContext(idi.contextAttributes, node.context);
      newInputPorts.put(idi.portName, new PortContextPair<InputPort<?>>(port, context));
      port.setup(context);
    }
    inputPorts.putAll(newInputPorts);

    /* setup context for all the output ports */
    LinkedHashMap<String, PortContextPair<OutputPort<?>>> outputPorts = node.getPortMappingDescriptor().outputPorts;
    LinkedHashMap<String, PortContextPair<OutputPort<?>>> newOutputPorts = new LinkedHashMap<>(outputPorts.size());
    for (OperatorDeployInfo.OutputDeployInfo odi : ndi.outputs) {
      OutputPort<?> port = outputPorts.get(odi.portName).component;
      PortContext context = new PortContext(odi.contextAttributes, node.context);
      newOutputPorts.put(odi.portName, new PortContextPair<OutputPort<?>>(port, context));
      port.setup(context);
    }
    outputPorts.putAll(newOutputPorts);

    logger.debug("activating {} in container {}", node, containerId);
    /* This introduces need for synchronization on processNodeRequest which was solved by adding deleted field in StramToNodeRequest  */
    processNodeRequests(false);
    node.activate();
    eventBus.publish(new NodeActivationEvent(node));
  }

  private void teardownNode(OperatorDeployInfo ndi)
  {
    final Node<?> node = nodes.get(ndi.id);
    if (node == null) {
      logger.warn("node {}/{} took longer to exit, resulting in unclean undeploy!", ndi.id, ndi.name);
    } else {
      eventBus.publish(new NodeDeactivationEvent(node));
      node.deactivate();
      node.teardown();
      logger.debug("deactivated {}", node.getId());
    }
  }

  public synchronized void activate(final Map<Integer, OperatorDeployInfo> nodeMap, Map<String, ComponentContextPair<Stream, StreamContext>> newStreams)
  {
    for (ComponentContextPair<Stream, StreamContext> pair : newStreams.values()) {
      activeStreams.put(pair.component, pair.context);
      pair.component.activate(pair.context);
      eventBus.publish(new StreamActivationEvent(pair));
    }

    for (final OperatorDeployInfo ndi : nodeMap.values()) {
      /*
       * OiO nodes get activated with their primary nodes.
       */
      if (ndi.type == OperatorType.OIO) {
        continue;
      }

      final Node<?> node = nodes.get(ndi.id);
      final String name = new StringBuilder(Integer.toString(ndi.id))
          .append('/')
          .append(ndi.name)
          .append(':')
          .append(node.getOperator().getClass().getSimpleName())
          .toString();
      final Thread thread = new Thread(name)
      {
        @Override
        public void run()
        {
          HashSet<OperatorDeployInfo> setOperators = new HashSet<>();
          OperatorDeployInfo currentdi = ndi;
          try {
            /* primary operator initialization */
            setupNode(currentdi);
            setOperators.add(currentdi);

            /* lets go for OiO operator initialization */
            List<Integer> oioNodeIdList = oioGroups.get(ndi.id);
            if (oioNodeIdList != null) {
              for (Integer oioNodeId : oioNodeIdList) {
                currentdi = nodeMap.get(oioNodeId);
                setupNode(currentdi);
                setOperators.add(currentdi);
              }
            }

            currentdi = null;

            node.run(); /* this is a blocking call */
          } catch (Error error) {
            int[] operators;
            //fetch logFileInfo before logging exception, to get offset before exception
            LogFileInformation logFileInfo = LoggerUtil.getLogFileInformation();
            if (currentdi == null) {
              logger.error("Voluntary container termination due to an error in operator set {}.", setOperators, error);
              operators = new int[setOperators.size()];
              int i = 0;
              for (Iterator<OperatorDeployInfo> it = setOperators.iterator(); it.hasNext(); i++) {
                operators[i] = it.next().id;
              }
            } else {
              logger.error("Voluntary container termination due to an error in operator {}.", currentdi, error);
              operators = new int[]{currentdi.id};
            }
            try {
              umbilical.reportError(containerId, operators, "Voluntary container termination due to an error. " + ExceptionUtils.getStackTrace(error), logFileInfo);
            } catch (Exception e) {
              logger.debug("Fail to log", e);
            } finally {
              System.exit(1);
            }
          } catch (Exception ex) {
            //fetch logFileInfo before logging exception, to get offset before exception
            LogFileInformation logFileInfo = LoggerUtil.getLogFileInformation();
            if (currentdi == null) {
              failedNodes.add(ndi.id);
              logger.error("Operator set {} stopped running due to an exception.", setOperators, ex);
              int[] operators = new int[]{ndi.id};
              try {
                umbilical.reportError(containerId, operators, "Stopped running due to an exception. " + ExceptionUtils.getStackTrace(ex), logFileInfo);
              } catch (Exception e) {
                logger.debug("Fail to log", e);
              }
            } else {
              failedNodes.add(currentdi.id);
              logger.error("Abandoning deployment of operator {} due to setup failure.", currentdi, ex);
              int[] operators = new int[]{currentdi.id};
              try {
                umbilical.reportError(containerId, operators, "Abandoning deployment due to setup failure. " + ExceptionUtils.getStackTrace(ex), logFileInfo);
              } catch (Exception e) {
                logger.debug("Fail to log", e);
              }
            }
          } finally {
            if (setOperators.contains(ndi)) {
              try {
                teardownNode(ndi);
              } catch (Exception ex) {
                failedNodes.add(ndi.id);
                logger.error("Shutdown of operator {} failed due to an exception.", ndi, ex);
              }
            }

            List<Integer> oioNodeIdList = oioGroups.get(ndi.id);
            if (oioNodeIdList != null) {
              for (Integer oioNodeId : oioNodeIdList) {
                OperatorDeployInfo oiodi = nodeMap.get(oioNodeId);
                if (setOperators.contains(oiodi)) {
                  try {
                    teardownNode(oiodi);
                  } catch (Exception ex) {
                    failedNodes.add(oiodi.id);
                    logger.error("Shutdown of operator {} failed due to an exception.", oiodi, ex);
                  }
                }
              }
            }
          }
        }
      };
      node.context.setThread(thread);
      List<Integer> oioNodeIdList = oioGroups.get(ndi.id);
      if (oioNodeIdList != null) {
        for (Integer oioNodeId : oioNodeIdList) {
          Node<?> oioNode = nodes.get(oioNodeId);
          oioNode.context.setThread(thread);
        }
      }
      thread.start();
    }

    for (WindowGenerator wg : generators.values()) {
      if (!activeGenerators.containsKey(wg)) {
        activeGenerators.put(wg, generators);
        wg.activate(null);
      }
    }
  }

  private void groupInputStreams(HashMap<String, ArrayList<String>> groupedInputStreams, OperatorDeployInfo ndi)
  {
    for (OperatorDeployInfo.InputDeployInfo nidi : ndi.inputs) {
      String source = Integer.toString(nidi.sourceNodeId).concat(Component.CONCAT_SEPARATOR).concat(nidi.sourcePortName);

      /*
       * if we do not want to combine multiple streams with different partitions from the
       * same upstream node, we could also use the partition to group the streams together.
       * This logic comes with the danger that the performance of the group which shares
       * the same stream is bounded on the higher side by the performance of the lowest
       * performer upstream port. May be combining the streams is not such a good thing
       * but let's see if we allow this as an option to the user, what they end up choosing
       * the most.
       */
      ArrayList<String> collection = groupedInputStreams.get(source);
      if (collection == null) {
        collection = new ArrayList<>();
        groupedInputStreams.put(source, collection);
      }
      collection.add(Integer.toString(ndi.id).concat(Component.CONCAT_SEPARATOR).concat(nidi.portName));
    }
  }

  protected Checkpoint getFinishedCheckpoint(OperatorDeployInfo ndi)
  {
    Checkpoint checkpoint;
    if (ndi.contextAttributes != null
        && ndi.contextAttributes.get(OperatorContext.PROCESSING_MODE) == ProcessingMode.AT_MOST_ONCE) {
      long now = System.currentTimeMillis();
      long windowCount = WindowGenerator.getWindowCount(now, firstWindowMillis, firstWindowMillis);

      Integer temp = ndi.contextAttributes.get(OperatorContext.APPLICATION_WINDOW_COUNT);
      if (temp == null) {
        temp = containerContext.getValue(OperatorContext.APPLICATION_WINDOW_COUNT);
      }
      int appWindowCount = (int)(windowCount % temp);

      temp = ndi.contextAttributes.get(OperatorContext.CHECKPOINT_WINDOW_COUNT);
      if (temp == null) {
        temp = containerContext.getValue(OperatorContext.CHECKPOINT_WINDOW_COUNT);
      }
      int lCheckpointWindowCount = (int)(windowCount % temp);
      checkpoint = new Checkpoint(WindowGenerator.getWindowId(now, firstWindowMillis, windowWidthMillis), appWindowCount, lCheckpointWindowCount);
      logger.debug("using {} on {} at {}", ProcessingMode.AT_MOST_ONCE, ndi.name, checkpoint);
    } else {
      checkpoint = ndi.checkpoint;
      logger.debug("using {} on {} at {}",
          ndi.contextAttributes == null ? ProcessingMode.AT_LEAST_ONCE : (ndi.contextAttributes.get(OperatorContext.PROCESSING_MODE) == null ? ProcessingMode.AT_LEAST_ONCE : ndi.contextAttributes.get(OperatorContext.PROCESSING_MODE)),
          ndi.name, checkpoint);
    }

    return checkpoint;
  }

  public void operateListeners(StreamingContainerContext ctx, boolean setup)
  {
    if (setup) {
      for (Component<ContainerContext> c : components) {
        c.setup(ctx);
      }
    } else {
      for (Component<ContainerContext> c : components) {
        c.teardown();
      }
    }
  }

  /**
   * Traverse the components to find out the default value set at the top level.
   * In OO this class should be shielded from using internal implementation. When
   * OperatorDeployInfo starts implementing proper semantics of Context, this
   * would not be needed.
   *
   * @param <T>         Type of the attribute.
   * @param key         Name of the attribute.
   * @param portContext Port context if applicable otherwise null.
   * @param deployInfo  Operator context if applicable otherwise null.
   * @return Value of the operator
   */
  private <T> T getValue(Attribute<T> key, com.datatorrent.api.Context.PortContext portContext, OperatorDeployInfo deployInfo)
  {
    if (portContext != null) {
      Attribute.AttributeMap attributes = portContext.getAttributes();
      if (attributes != null) {
        T attr = attributes.get(key);
        if (attr != null) {
          return attr;
        }
      }
    }

    if (deployInfo != null) {
      Attribute.AttributeMap attributes = deployInfo.contextAttributes;
      if (attributes != null) {
        T attr = attributes.get(key);
        if (attr != null) {
          return attr;
        }
      }
    }

    return containerContext.getValue(key);
  }

  private <T> T getValue(Attribute<T> key, OperatorDeployInfo deployInfo)
  {
    if (deployInfo != null) {
      Attribute.AttributeMap attributes = deployInfo.contextAttributes;
      if (attributes != null) {
        T attr = attributes.get(key);
        if (attr != null) {
          return attr;
        }
      }
    }

    return containerContext.getValue(key);
  }

  private void handleChangeLoggersRequest(StramToNodeChangeLoggersRequest request)
  {
    logger.debug("handle change logger request");
    LoggerUtil.changeLoggersLevel(request.getTargetChanges());
  }

  private final StreamCodec<Object> nonSerializingStreamCodec = new StreamCodec<Object>()
  {
    @Override
    public Object fromByteArray(Slice fragment)
    {
      return null;
    }

    @Override
    public Slice toByteArray(Object o)
    {
      return null;
    }

    @Override
    public int getPartition(Object o)
    {
      return o.hashCode();
    }
  };

  private static final Logger logger = LoggerFactory.getLogger(StreamingContainer.class);
}
