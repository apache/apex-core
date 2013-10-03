/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.Thread.State;
import java.net.*;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.LogManager;

import com.datatorrent.api.*;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Operator.ProcessingMode;

import com.datatorrent.bufferserver.server.Server;
import com.datatorrent.bufferserver.storage.DiskStorage;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.stram.OperatorDeployInfo.OperatorType;
import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.*;
import com.datatorrent.stram.api.ContainerContext;
import com.datatorrent.stram.api.NodeActivationListener;
import com.datatorrent.stram.api.NodeRequest;
import com.datatorrent.stram.api.RequestFactory;
import com.datatorrent.stram.api.StatsListener.ContainerStatsListener;
import com.datatorrent.stram.debug.StdOutErrLog;
import com.datatorrent.stram.engine.ByteCounterStream;
import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.engine.PortContext;
import com.datatorrent.stram.engine.Stats.ContainerStats;
import com.datatorrent.stram.engine.Stream;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.engine.SweepableReservoir;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.engine.WindowIdActivatedReservoir;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.Operators.PortContextPair;
import com.datatorrent.stram.plan.logical.Operators.PortMappingDescriptor;
import com.datatorrent.stram.stream.BufferServerPublisher;
import com.datatorrent.stram.stream.BufferServerSubscriber;
import com.datatorrent.stram.stream.FastPublisher;
import com.datatorrent.stram.stream.FastSubscriber;
import com.datatorrent.stram.stream.InlineStream;
import com.datatorrent.stram.stream.MuxStream;
import com.datatorrent.stram.stream.OiOStream;
import com.datatorrent.stram.stream.PartitionAwareSink;
import com.datatorrent.stram.util.ScheduledThreadPoolExecutor;
import java.util.Map.Entry;

/**
 * Object which controls the container process launched by {@link com.datatorrent.stram.StramAppMaster}.
 *
 * @since 0.3.2
 */
public class StramChild
{
  public static final int PORT_QUEUE_CAPACITY = 1024;
  private static final int SPIN_MILLIS = 20;
  private final String containerId;
  private final Configuration conf;
  private final StreamingContainerUmbilicalProtocol umbilical;
  protected final Map<Integer, Node<?>> nodes = new ConcurrentHashMap<Integer, Node<?>>();
  protected final Set<Integer> failedNodes = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
  private final Map<String, ComponentContextPair<Stream, StreamContext>> streams = new ConcurrentHashMap<String, ComponentContextPair<Stream, StreamContext>>();
  protected final Map<Integer, WindowGenerator> generators = new ConcurrentHashMap<Integer, WindowGenerator>();
  /**
   * It's a simple map which maps the oio node to it's the node which owns the thread.
   */
  protected final Map<Integer, Integer> oioNodes = new ConcurrentHashMap<Integer, Integer>();
  protected final Map<Integer, OperatorContext> activeNodes = new ConcurrentHashMap<Integer, OperatorContext>();
  private final Map<Stream, StreamContext> activeStreams = new ConcurrentHashMap<Stream, StreamContext>();
  private final Map<WindowGenerator, Object> activeGenerators = new ConcurrentHashMap<WindowGenerator, Object>();
  private int heartbeatIntervalMillis = 1000;
  private volatile boolean exitHeartbeatLoop = false;
  private final Object heartbeatTrigger = new Object();
  private String checkpointFsPath;
  public static DefaultEventLoop eventloop;
  /**
   * List of listeners interested in listening into the status change of the nodes.
   */
  private long firstWindowMillis;
  private int windowWidthMillis;
  private InetSocketAddress bufferServerAddress;
  private com.datatorrent.bufferserver.server.Server bufferServer;
  private int checkpointWindowCount;
  private boolean fastPublisherSubscriber;
  private StreamingContainerContext containerContext;
  private List<StramToNodeRequest> nodeRequests;
  // possibly should combine all the guys below into one type of listeners - containereventlistener!
  private ArrayList<ContainerStatsListener> statsListener;
  private HashSet<NodeActivationListener> nodeListener;
  private HashMap<String, Object> singletons;
  private RequestFactory requestFactory;

  static {
    try {
      eventloop = new DefaultEventLoop("ProcessWideEventLoop");
    }
    catch (IOException io) {
      throw new RuntimeException(io);
    }
  }

  protected StramChild(String containerId, Configuration conf, StreamingContainerUmbilicalProtocol umbilical)
  {
    this.nodeListener = new HashSet<NodeActivationListener>();
    this.statsListener = new ArrayList<ContainerStatsListener>();
    this.singletons = new HashMap<String, Object>();
    this.nodeRequests = new ArrayList<StramToNodeRequest>();

    logger.debug("instantiated StramChild {}", containerId);
    this.umbilical = umbilical;
    this.containerId = containerId;
    this.conf = conf;
  }

  public void setup(StreamingContainerContext ctx)
  {
    containerContext = ctx;

    /* add a request factory local to this container */
    this.requestFactory = new RequestFactory();
    ctx.attributes.addTransientAttribute(ContainerContext.REQUEST_FACTORY).set(requestFactory);

    heartbeatIntervalMillis = ctx.attrValue(DAGContext.HEARTBEAT_INTERVAL_MILLIS, 1000);
    firstWindowMillis = ctx.startWindowMillis;
    windowWidthMillis = ctx.attrValue(DAGContext.STREAMING_WINDOW_SIZE_MILLIS, 500);
    checkpointWindowCount = ctx.attrValue(DAGContext.CHECKPOINT_WINDOW_COUNT, 60);

    checkpointFsPath = ctx.attrValue(DAGContext.APPLICATION_PATH, "app-dfs-path-not-configured") + "/" + DAGContext.SUBDIR_CHECKPOINTS;
    fastPublisherSubscriber = ctx.attrValue(LogicalPlan.FAST_PUBLISHER_SUBSCRIBER, false);

    try {
      if (ctx.deployBufferServer) {
        eventloop.start();
        // start buffer server, if it was not set externally
        bufferServer = new Server(0, 64 * 1024 * 1024);
        bufferServer.setSpoolStorage(new DiskStorage());
        SocketAddress bindAddr = bufferServer.run(eventloop);
        logger.info("Buffer server started: {}", bindAddr);
        this.bufferServerAddress = NetUtils.getConnectAddress(((InetSocketAddress)bindAddr));
      }
    }
    catch (Exception ex) {
      logger.warn("deploy request failed due to {}", ex);
      throw new IllegalStateException("Failed to deploy buffer server", ex);
    }

    // hack: for now seed the TupleRecorder as both NodeListener and ContainerStatsListener
    // but this code has to move out of here soon. If you feel like doing it, do it now!!!!
    String classname = "com.datatorrent.stram.debug.TupleRecorderCollection";
    try {
      @SuppressWarnings("unchecked")
      Class<?> cl = Class.forName(classname);
      Object newInstance = cl.newInstance();
      singletons.put(classname, newInstance);
      //logger.debug("putting {} in {}", System.identityHashCode(newInstance), this);

      if (newInstance instanceof ContainerStatsListener) {
        logger.info("adding container stats listener {}", classname);
        addStatsListener((ContainerStatsListener)newInstance);
      }
      if (newInstance instanceof NodeActivationListener) {
        logger.info("adding node activation listener {}", classname);
        addNodeListener((NodeActivationListener)newInstance);
      }
    }
    catch (Exception ex) {
      logger.error("Exception while adding listener", ex);
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
   * after exiting heartbeat loop, deactivate all modules and terminate
   * processing threads.
   *
   * @param args
   * @throws Throwable
   */
  public static void main(String[] args) throws Throwable
  {
    StdOutErrLog.tieSystemOutAndErrToLog();
    logger.debug("PID: " + System.getenv().get("JVM_PID"));
    logger.info("Child starting with classpath: {}", System.getProperty("java.class.path"));

    final Configuration defaultConf = new Configuration();

    String host = args[0];
    int port = Integer.parseInt(args[1]);
    final InetSocketAddress address = NetUtils.createSocketAddrForHost(host, port);
    final StreamingContainerUmbilicalProtocol umbilical = RPC.getProxy(StreamingContainerUmbilicalProtocol.class,
                                                                       StreamingContainerUmbilicalProtocol.versionID, address, defaultConf);
    int exitStatus = 1;

    final String childId = System.getProperty("stram.cid");
    try {
      StreamingContainerContext ctx = umbilical.getInitContext(childId);
      StramChild stramChild = new StramChild(childId, defaultConf, umbilical);
      logger.debug("Got context: " + ctx);
      stramChild.setup(ctx);
      try {
        /* main thread enters heartbeat loop */
        stramChild.heartbeatLoop();
      }
      finally {
        stramChild.teardown();
      }
      exitStatus = 0;
    }
    catch (Exception exception) {
      logger.warn("Exception running child : " + exception);
      /* Report back any failures, for diagnostic purposes */
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      exception.printStackTrace(new PrintStream(baos));
      umbilical.log(childId, "FATAL: " + baos.toString());
    }
    catch (Throwable throwable) {
      logger.error("Error running child : "
              + StringUtils.stringifyException(throwable));
      Throwable tCause = throwable.getCause();
      String cause = tCause == null
                     ? throwable.getMessage()
                     : StringUtils.stringifyException(tCause);
      umbilical.log(childId, cause);
    }
    finally {
      RPC.stopProxy(umbilical);
      DefaultMetricsSystem.shutdown();
      // Shutting down log4j of the child-vm...
      // This assumes that on return from Task.activate()
      // there is no more logging done.
      LogManager.shutdown();
    }

    if (exitStatus != 0) {
      System.exit(exitStatus);
    }

  }

  public synchronized void deactivate()
  {
    ArrayList<Thread> activeThreads = new ArrayList<Thread>();
    ArrayList<Integer> activeOperators = new ArrayList<Integer>();

    for (Map.Entry<Integer, Node<?>> e : nodes.entrySet()) {
      OperatorContext oc = activeNodes.get(e.getKey());
      if (oc == null) {
        disconnectNode(e.getKey());
      }
      else {
        activeThreads.add(oc.getThread());
        activeOperators.add(e.getKey());
        e.getValue().deactivate();
      }
    }

    try {
      Iterator<Integer> iterator = activeOperators.iterator();
      for (Thread t : activeThreads) {
        t.join();
        disconnectNode(iterator.next());
      }
      assert (activeNodes.isEmpty());
    }
    catch (InterruptedException ex) {
      logger.info("Aborting wait for for operators to get deactivated as got interrupted with {}", ex);
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
        }

        if (pair.component instanceof Stream.MultiSinkCapableStream) {
          String sinks = pair.context.getSinkId();
          if (sinks == null) {
            logger.error("mux sinks found connected at {} with sink id null", sourceIdentifier);
          }
          else {
            String[] split = sinks.split(", ");
            for (int i = split.length; i-- > 0;) {
              ComponentContextPair<Stream, StreamContext> spair = streams.remove(split[i]);
              if (spair == null) {
                logger.error("mux is missing the stream for sink {}", split[i]);
              }
              else {
                if (activeStreams.remove(spair.component) != null) {
                  spair.component.deactivate();
                }

                spair.component.teardown();
              }
            }
          }
        }
        else {
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
        }

        pair.component.teardown();
      }
    }
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

  private synchronized void undeploy(List<OperatorDeployInfo> nodeList)
  {
    logger.info("got undeploy request {}", nodeList);
    /**
     * make sure that all the operators which we are asked to undeploy are in this container.
     */
    HashMap<Integer, Node<?>> toUndeploy = new HashMap<Integer, Node<?>>();
    for (OperatorDeployInfo ndi : nodeList) {
      Node<?> node = nodes.get(ndi.id);
      if (node == null) {
        throw new IllegalArgumentException("Node " + ndi.id + " is not hosted in this container!");
      }
      else if (toUndeploy.containsKey(ndi.id)) {
        throw new IllegalArgumentException("Node " + ndi.id + " is requested to be undeployed more than once");
      }
      else {
        toUndeploy.put(ndi.id, node);
      }
    }

    ArrayList<Thread> joinList = new ArrayList<Thread>();
    ArrayList<Integer> discoList = new ArrayList<Integer>();
    for (OperatorDeployInfo ndi : nodeList) {
      OperatorContext oc = activeNodes.get(ndi.id);
      if (oc == null) {
        disconnectNode(ndi.id);
      }
      else {
        joinList.add(oc.getThread());
        discoList.add(ndi.id);
        nodes.get(ndi.id).deactivate();
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
      logger.info("undeploy complete");
    }
    catch (InterruptedException ex) {
      logger.warn("Aborted waiting for the deactivate to finish!");
    }

    for (OperatorDeployInfo ndi : nodeList) {
      nodes.remove(ndi.id);
    }
  }

  public void teardown()
  {
    operateListeners(containerContext, false);

    deactivate();

    assert (streams.isEmpty());

    nodes.clear();

    HashSet<WindowGenerator> gens = new HashSet<WindowGenerator>();
    gens.addAll(generators.values());
    generators.clear();
    for (WindowGenerator wg : gens) {
      wg.teardown();
    }

    if (bufferServer != null) {
      eventloop.stop(bufferServer);
      eventloop.stop();
    }

    gens.clear();
  }

  protected void triggerHeartbeat()
  {
    synchronized (heartbeatTrigger) {
      heartbeatTrigger.notifyAll();
    }
  }

  protected void heartbeatLoop() throws IOException
  {
    umbilical.log(containerId, "[" + containerId + "] Entering heartbeat loop..");
    logger.debug("Entering heartbeat loop (interval is {} ms)", this.heartbeatIntervalMillis);
    while (!exitHeartbeatLoop) {

      synchronized (this.heartbeatTrigger) {
        try {
          this.heartbeatTrigger.wait(heartbeatIntervalMillis);
        }
        catch (InterruptedException e1) {
          logger.warn("Interrupted in heartbeat loop, exiting..");
          break;
        }
      }

      long currentTime = System.currentTimeMillis();
      ContainerHeartbeat msg = new ContainerHeartbeat();
      msg.setContainerId(this.containerId);
      msg.jvmName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
      if (this.bufferServerAddress != null) {
        msg.bufferServerHost = this.bufferServerAddress.getHostName();
        msg.bufferServerPort = this.bufferServerAddress.getPort();
        if (bufferServer != null && !eventloop.isActive()) {
          logger.warn("Requesting restart due to terminated event loop");
          msg.restartRequested = true;
        }
      }
      msg.memoryMBFree = ((int)(Runtime.getRuntime().freeMemory() / (1024 * 1024)));
      ContainerStats stats = new ContainerStats(containerId);

      // gather heartbeat info for all operators
      for (Map.Entry<Integer, Node<?>> e : nodes.entrySet()) {
        StreamingNodeHeartbeat hb = new StreamingNodeHeartbeat();
        hb.setNodeId(e.getKey());
        hb.setGeneratedTms(currentTime);
        hb.setIntervalMs(heartbeatIntervalMillis);
        OperatorContext ctx = activeNodes.get(e.getKey());
        if (ctx != null) {
          ctx.drainStats(hb.getOperatorStatsContainer());
          hb.setState(StreamingNodeHeartbeat.DNodeState.ACTIVE.toString());
        }
        else {
          hb.setState(failedNodes.contains(e.getKey()) ? StreamingNodeHeartbeat.DNodeState.FAILED.toString() : StreamingNodeHeartbeat.DNodeState.IDLE.toString());
        }

        PortMappingDescriptor portMappingDescriptor = e.getValue().getPortMappingDescriptor();
        for (String portName : portMappingDescriptor.inputPorts.keySet()) {
          // the following code should belong to stats collections method.
          if (bufferServerAddress != null) {
            String streamId = e.getKey().toString().concat(Component.CONCAT_SEPARATOR).concat(portName);
            ComponentContextPair<Stream, StreamContext> stream = streams.get(streamId);
            if (stream != null && (stream.component instanceof ByteCounterStream)) {
              hb.setBufferServerBytes(portName, ((ByteCounterStream)stream.component).getByteCount(true));
            }
          }
        }
        for (String portName : portMappingDescriptor.outputPorts.keySet()) {
          // the following code should belong to stats collections method.
          if (bufferServerAddress != null) {
            String streamId = e.getKey().toString().concat(Component.CONCAT_SEPARATOR).concat(portName);
            ComponentContextPair<Stream, StreamContext> stream = streams.get(streamId);
            if (stream != null && (stream.component instanceof ByteCounterStream)) {
              hb.setBufferServerBytes(portName, ((ByteCounterStream)stream.component).getByteCount(true));
            }
          }
        }
        stats.addNodeStats(hb);
      }

      for (ContainerStatsListener csl : statsListener) {
        csl.collected(stats);
      }

      msg.setContainerStats(stats);

      // heartbeat call and follow-up processing
      //logger.debug("Sending heartbeat for {} operators.", msg.getContainerStats().size());
      try {
        ContainerHeartbeatResponse rsp = umbilical.processHeartbeat(msg);
        if (rsp != null) {
          processHeartbeatResponse(rsp);
          // keep polling at smaller interval if work is pending
          while (rsp != null && rsp.hasPendingRequests) {
            logger.info("Waiting for pending request.");
            synchronized (this.heartbeatTrigger) {
              try {
                this.heartbeatTrigger.wait(500);
              }
              catch (InterruptedException e1) {
                logger.warn("Interrupted in heartbeat loop, exiting..");
                break;
              }
            }
            rsp = umbilical.pollRequest(this.containerId);
            if (rsp != null) {
              processHeartbeatResponse(rsp);
            }
          }
        }
      }
      catch (Exception e) {
        logger.warn("Exception received (may be during shutdown?)", e);
      }
    }
    logger.debug("Exiting hearbeat loop");
    umbilical.log(containerId, "[" + containerId + "] Exiting heartbeat loop..");
  }

  private long lastCommittedWindowId = WindowGenerator.MIN_WINDOW_ID - 1;

  private void processNodeRequests(boolean flagInvalid)
  {
    for (Iterator<StramToNodeRequest> it = nodeRequests.iterator(); it.hasNext();) {
      StramToNodeRequest req = it.next();
      OperatorContext oc = activeNodes.get(req.getOperatorId());
      if (oc == null) {
        if (flagInvalid) {
          logger.warn("Received request with invalid operator id {} ({})", req.getOperatorId(), req);
          it.remove();
        }
      }
      else {
        logger.debug("request received: {}", req);
        NodeRequest requestExecutor = requestFactory.getRequestExecutor(nodes.get(req.operatorId), req);
        if (requestExecutor != null) {
          oc.request(requestExecutor);
        }
        else {
          logger.warn("No executor identified for the request {}", req);
        }

        it.remove();
      }
    }
  }

  @SuppressWarnings("unchecked")
  protected void processHeartbeatResponse(ContainerHeartbeatResponse rsp)
  {
    if (rsp.nodeRequests != null) {
      nodeRequests = rsp.nodeRequests;
    }

    if (rsp.committedWindowId != lastCommittedWindowId) {
      lastCommittedWindowId = rsp.committedWindowId;
      NodeRequest nr = null;
      for (Map.Entry<Integer, OperatorContext> e : activeNodes.entrySet()) {
        if (nodes.get(e.getKey()).getOperator() instanceof CheckpointListener) {
          if (nr == null) {
            nr = new NodeRequest()
            {
              @Override
              public void execute(Operator operator, int id, long windowId) throws IOException
              {
                ((CheckpointListener)operator).committed(lastCommittedWindowId);
              }

            };
          }
          e.getValue().request(nr);
        }
      }
    }

    if (rsp.shutdown) {
      logger.info("Received shutdown request");
      processNodeRequests(false);
      this.exitHeartbeatLoop = true;
      return;
    }

    if (rsp.undeployRequest != null) {
      logger.info("Undeploy request: {}", rsp.undeployRequest);
      processNodeRequests(false);
      undeploy(rsp.undeployRequest);
    }

    if (rsp.deployRequest != null) {
      logger.info("Deploy request: {}", rsp.deployRequest);
      try {
        deploy(rsp.deployRequest);
      }
      catch (Exception e) {
        logger.error("deploy request failed due to {}", e);
        // TODO: report it to stram?
        try {
          umbilical.log(this.containerId, "deploy request failed: " + rsp.deployRequest + " " + ExceptionUtils.getStackTrace(e));
        }
        catch (IOException ioe) {
          // ignore
        }
        this.exitHeartbeatLoop = true;
        throw new IllegalStateException("Deploy request failed: " + rsp.deployRequest, e);
      }
    }

    processNodeRequests(true);
  }

  private int getOutputQueueCapacity(List<OperatorDeployInfo> operatorList, int sourceOperatorId, String sourcePortName)
  {
    for (OperatorDeployInfo odi : operatorList) {
      if (odi.id == sourceOperatorId) {
        for (OperatorDeployInfo.OutputDeployInfo odiodi : odi.outputs) {
          if (odiodi.portName.equals(sourcePortName)) {
            return odiodi.attrValue(PortContext.QUEUE_CAPACITY, PORT_QUEUE_CAPACITY);
          }
        }
      }
    }

    return PORT_QUEUE_CAPACITY;
  }

  private synchronized void deploy(List<OperatorDeployInfo> nodeList) throws Exception
  {
    /*
     * A little bit of up front sanity check would reduce the percentage of deploy failures later.
     */
    for (OperatorDeployInfo ndi : nodeList) {
      if (nodes.containsKey(ndi.id)) {
        throw new IllegalStateException("Node with id: " + ndi.id + " already present in the container");
      }
    }

    deployNodes(nodeList);

    HashMap<String, ArrayList<String>> groupedInputStreams = new HashMap<String, ArrayList<String>>();
    for (OperatorDeployInfo ndi : nodeList) {
      groupInputStreams(groupedInputStreams, ndi);
    }

    HashMap<String, ComponentContextPair<Stream, StreamContext>> newStreams = deployOutputStreams(nodeList, groupedInputStreams);
    deployInputStreams(nodeList, newStreams);
    for (ComponentContextPair<Stream, StreamContext> pair : newStreams.values()) {
      pair.component.setup(pair.context);
    }
    streams.putAll(newStreams);

    HashMap<Integer, OperatorDeployInfo> operatorMap = new HashMap<Integer, OperatorDeployInfo>(nodeList.size());
    for (OperatorDeployInfo o : nodeList) {
      operatorMap.put(o.id, o);
    }
    activate(operatorMap, newStreams);
  }

  private void massageUnifierDeployInfo(OperatorDeployInfo odi)
  {
    for (OperatorDeployInfo.InputDeployInfo idi : odi.inputs) {
      idi.portName += "(" + idi.sourceNodeId + Component.CONCAT_SEPARATOR + idi.sourcePortName + ")";
    }
  }

  private void deployNodes(List<OperatorDeployInfo> nodeList) throws Exception
  {
    for (OperatorDeployInfo ndi : nodeList) {
      StorageAgent backupAgent;
      if (ndi.contextAttributes == null) {
        backupAgent = new HdfsStorageAgent(this.conf, this.checkpointFsPath);
      }
      else {
        backupAgent = ndi.contextAttributes.attr(OperatorContext.STORAGE_AGENT).get();
        if (backupAgent == null) {
          backupAgent = new HdfsStorageAgent(this.conf, this.checkpointFsPath);
          ndi.contextAttributes.attr(OperatorContext.STORAGE_AGENT).set(backupAgent);
        }
      }

      try {
        logger.debug("Restoring node {} to checkpoint {}", ndi.id, Codec.getStringWindowId(ndi.checkpointWindowId));
        InputStream stream = backupAgent.getLoadStream(ndi.id, ndi.checkpointWindowId);
        Node<?> node = Node.retrieveNode(stream, ndi.type);
        stream.close();
        node.currentWindowId = ndi.checkpointWindowId;
        if (ndi.type == OperatorDeployInfo.OperatorType.UNIFIER) {
          massageUnifierDeployInfo(ndi);
        }
        node.setId(ndi.id);
        nodes.put(ndi.id, node);
        logger.debug("Marking deployed {}", node);
      }
      catch (Exception e) {
        logger.error("Deploy error", e);
        throw e;
      }
    }
  }

  private HashMap.SimpleEntry<String, ComponentContextPair<Stream, StreamContext>> deployBufferServerPublisher(
          String sourceIdentifier, long startingWindowId, int queueCapacity, OperatorDeployInfo.OutputDeployInfo nodi)
          throws UnknownHostException
  {
    String sinkIdentifier = "tcp://".concat(nodi.bufferServerHost).concat(":").concat(String.valueOf(nodi.bufferServerPort)).concat("/").concat(sourceIdentifier);

    StreamContext bssc = new StreamContext(nodi.declaredStreamId);
    bssc.setSourceId(sourceIdentifier);
    bssc.setSinkId(sinkIdentifier);
    bssc.setFinishedWindowId(startingWindowId);
    bssc.attr(StreamContext.CODEC).set(StramUtils.getSerdeInstance(nodi.serDeClassName));
    bssc.attr(StreamContext.EVENT_LOOP).set(eventloop);
    bssc.setBufferServerAddress(InetSocketAddress.createUnresolved(nodi.bufferServerHost, nodi.bufferServerPort));
    if (NetUtils.isLocalAddress(bssc.getBufferServerAddress().getAddress())) {
      bssc.setBufferServerAddress(new InetSocketAddress(InetAddress.getByName(null), nodi.bufferServerPort));
    }

    Stream publisher = fastPublisherSubscriber ? new FastPublisher(sourceIdentifier, queueCapacity * 256) : new BufferServerPublisher(sourceIdentifier, queueCapacity);
    return new HashMap.SimpleEntry<String, ComponentContextPair<Stream, StreamContext>>(sinkIdentifier, new ComponentContextPair<Stream, StreamContext>(publisher, bssc));
  }

  private HashMap<String, ComponentContextPair<Stream, StreamContext>> deployOutputStreams(
          List<OperatorDeployInfo> nodeList, HashMap<String, ArrayList<String>> groupedInputStreams)
          throws Exception
  {
    HashMap<String, ComponentContextPair<Stream, StreamContext>> newStreams = new HashMap<String, ComponentContextPair<Stream, StreamContext>>();
    /*
     * We proceed to deploy all the output streams. At the end of this block, our streams collection
     * will contain all the streams which originate at the output port of the operators. The streams
     * are generally mapped against the "nodename.portname" string. But the BufferServerPublishers which
     * share the output port with other inline streams are mapped against the Buffer Server port to
     * avoid collision and at the same time keep track of these buffer streams.
     */
    for (OperatorDeployInfo ndi : nodeList) {
      Node<?> node = nodes.get(ndi.id);
      long finishedWindowId = ndi.checkpointWindowId > 0 ? ndi.checkpointWindowId : 0;

      for (OperatorDeployInfo.OutputDeployInfo nodi : ndi.outputs) {
        String sourceIdentifier = Integer.toString(ndi.id).concat(Component.CONCAT_SEPARATOR).concat(nodi.portName);
        int queueCapacity = nodi.attrValue(PortContext.QUEUE_CAPACITY, PORT_QUEUE_CAPACITY);
        logger.debug("for stream {} the queue capacity is {}", sourceIdentifier, queueCapacity);

        ArrayList<String> collection = groupedInputStreams.get(sourceIdentifier);
        if (collection == null) {
          assert (nodi.bufferServerHost != null): "resulting stream cannot be inline: " + nodi;
          /*
           * Let's create a stream to carry the data to the Buffer Server.
           * Nobody in this container is interested in the output placed on this stream, but
           * this stream exists. That means someone outside of this container must be interested.
           */
          SimpleEntry<String, ComponentContextPair<Stream, StreamContext>> deployBufferServerPublisher =
                  deployBufferServerPublisher(sourceIdentifier, finishedWindowId, queueCapacity, nodi);
          newStreams.put(sourceIdentifier, deployBufferServerPublisher.getValue());
          node.connectOutputPort(nodi.portName, deployBufferServerPublisher.getValue().component);
        }
        else {
          /*
           * In this case we have 2 possibilities, either we have 1 inline or multiple streams.
           * Since we cannot tell at this point, we assume that we will have multiple streams and
           * plan accordingly. we possibly will come to this code block multiple times. We create
           * the MuxStream only the first time and use it for subsequent calls of this block.
           */
          ComponentContextPair<Stream, StreamContext> pair = newStreams.get(sourceIdentifier);
          if (pair == null) {
            /**
             * Let's multiplex the output placed on this stream.
             * This container itself contains more than one parties interested.
             */
            StreamContext context = new StreamContext(nodi.declaredStreamId);
            context.setSourceId(sourceIdentifier);
            context.setFinishedWindowId(finishedWindowId);
            Stream stream = new MuxStream();

            newStreams.put(sourceIdentifier, pair = new ComponentContextPair<Stream, StreamContext>(stream, context));
            node.connectOutputPort(nodi.portName, stream);
          }

          if (nodi.bufferServerHost != null) {
            /*
             * Although there is a node in this container interested in output placed on this stream, there
             * seems to at least one more party interested but placed in a container other than this one.
             */
            SimpleEntry<String, ComponentContextPair<Stream, StreamContext>> deployBufferServerPublisher =
                    deployBufferServerPublisher(sourceIdentifier, finishedWindowId, queueCapacity, nodi);
            newStreams.put(deployBufferServerPublisher.getKey(), deployBufferServerPublisher.getValue());

            String sinkIdentifier = pair.context.getSinkId();
            if (sinkIdentifier == null) {
              pair.context.setSinkId(deployBufferServerPublisher.getKey());
            }
            else {
              pair.context.setSinkId(sinkIdentifier.concat(", ").concat(deployBufferServerPublisher.getKey()));
            }

            ((Stream.MultiSinkCapableStream)pair.component).setSink(deployBufferServerPublisher.getKey(), deployBufferServerPublisher.getValue().component);
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
   * @param portname name of port to which the stream is connected.
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

  private void deployInputStreams(List<OperatorDeployInfo> operatorList, HashMap<String, ComponentContextPair<Stream, StreamContext>> newStreams) throws UnknownHostException
  {
    /*
     * collect any input operators along with their smallest window id,
     * those are subsequently used to setup the window generator
     */
    ArrayList<OperatorDeployInfo> inputNodes = new ArrayList<OperatorDeployInfo>();
    long smallestCheckpointedWindowId = Long.MAX_VALUE;

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
        ndi.checkpointWindowId = getFinishedWindowId(ndi);
        if (ndi.checkpointWindowId < smallestCheckpointedWindowId) {
          smallestCheckpointedWindowId = ndi.checkpointWindowId;
        }
      }
      else {
        Node<?> node = nodes.get(ndi.id);

        for (OperatorDeployInfo.InputDeployInfo nidi : ndi.inputs) {
          String sourceIdentifier = Integer.toString(nidi.sourceNodeId).concat(Component.CONCAT_SEPARATOR).concat(nidi.sourcePortName);
          String sinkIdentifier = Integer.toString(ndi.id).concat(Component.CONCAT_SEPARATOR).concat(nidi.portName);

          int queueCapacity = nidi.contextAttributes == null ? PORT_QUEUE_CAPACITY : nidi.attrValue(PortContext.QUEUE_CAPACITY, PORT_QUEUE_CAPACITY);

          long finishedWindowId = getFinishedWindowId(ndi);
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
            if (NetUtils.isLocalAddress(context.getBufferServerAddress().getAddress())) {
              context.setBufferServerAddress(new InetSocketAddress(InetAddress.getByName(null), nidi.bufferServerPort));
            }
            context.attr(StreamContext.CODEC).set(StramUtils.getSerdeInstance(nidi.serDeClassName));
            context.attr(StreamContext.EVENT_LOOP).set(eventloop);
            context.setPartitions(nidi.partitionMask, nidi.partitionKeys);
            context.setSourceId(sourceIdentifier);
            context.setSinkId(sinkIdentifier);
            context.setFinishedWindowId(finishedWindowId);

            BufferServerSubscriber subscriber = fastPublisherSubscriber
                                                ? new FastSubscriber("tcp://".concat(nidi.bufferServerHost).concat(":").concat(String.valueOf(nidi.bufferServerPort)).concat("/").concat(sourceIdentifier), queueCapacity)
                                                : new BufferServerSubscriber("tcp://".concat(nidi.bufferServerHost).concat(":").concat(String.valueOf(nidi.bufferServerPort)).concat("/").concat(sourceIdentifier), queueCapacity);
            SweepableReservoir reservoir = subscriber.acquireReservoir(sinkIdentifier, queueCapacity);
            if (finishedWindowId > 0) {
              node.connectInputPort(nidi.portName, new WindowIdActivatedReservoir(sinkIdentifier, reservoir, finishedWindowId));
            }
            node.connectInputPort(nidi.portName, reservoir);

            newStreams.put(sinkIdentifier, new ComponentContextPair<Stream, StreamContext>(subscriber, context));
            logger.debug("put input stream {} against key {}", subscriber, sinkIdentifier);
          }
          else {
            assert (nidi.locality == Locality.CONTAINER_LOCAL || nidi.locality == Locality.THREAD_LOCAL);
            /* we are still dealing with the MuxStream originating at the output of the source port */
            StreamContext inlineContext = new StreamContext(nidi.declaredStreamId);
            inlineContext.setSourceId(sourceIdentifier);
            inlineContext.setSinkId(sinkIdentifier);

            Stream stream;
            switch (nidi.locality) {
              case CONTAINER_LOCAL:
                int outputQueueCapacity = getOutputQueueCapacity(operatorList, nidi.sourceNodeId, nidi.sourcePortName);
                if (outputQueueCapacity > queueCapacity) {
                  queueCapacity = outputQueueCapacity;
                }

                stream = new InlineStream(queueCapacity);
                if (finishedWindowId > 0) {
                  node.connectInputPort(nidi.portName, new WindowIdActivatedReservoir(sinkIdentifier, (SweepableReservoir)stream, finishedWindowId));
                }
                break;

              case THREAD_LOCAL:
                stream = new OiOStream();
                oioNodes.put(ndi.id, nidi.sourceNodeId);
                break;

              default:
                throw new IllegalStateException("Locality can be either ContainerLocal or ThreadLocal");
            }

            node.connectInputPort(nidi.portName, (SweepableReservoir)stream);
            newStreams.put(sinkIdentifier, new ComponentContextPair<Stream, StreamContext>(stream, inlineContext));

            if (!(pair.component instanceof Stream.MultiSinkCapableStream)) {
              String originalSinkId = pair.context.getSinkId();

              /* we come here only if we are trying to augment the dag */
              StreamContext muxContext = new StreamContext(nidi.declaredStreamId);
              muxContext.setSourceId(sourceIdentifier);
              muxContext.setFinishedWindowId(finishedWindowId);
              muxContext.setSinkId(originalSinkId);

              MuxStream muxStream = new MuxStream();
              muxStream.setSink(originalSinkId, pair.component);
              streams.put(originalSinkId, pair);

              Node<?> sourceNode = nodes.get(nidi.sourceNodeId);
              sourceNode.connectOutputPort(nidi.sourcePortName, muxStream);
              newStreams.put(sourceIdentifier, pair = new ComponentContextPair<Stream, StreamContext>(muxStream, muxContext));
            }

            /* here everything should be multisink capable */
            if (nidi.partitionKeys == null || nidi.partitionKeys.isEmpty()) {
              ((Stream.MultiSinkCapableStream)pair.component).setSink(sinkIdentifier, stream);
            }
            else {
              /*
               * generally speaking we do not have partitions on the inline streams so the control should not
               * come here but if it comes, then we are ready to handle it using the partition aware streams.
               */
              PartitionAwareSink<Object> pas = new PartitionAwareSink<Object>(StramUtils.getSerdeInstance(nidi.serDeClassName), nidi.partitionKeys, nidi.partitionMask, stream);
              ((Stream.MultiSinkCapableStream)pair.component).setSink(sinkIdentifier, pas);
            }

            String streamSinkId = pair.context.getSinkId();
            if (streamSinkId == null) {
              pair.context.setSinkId(sinkIdentifier);
            }
            else {
              pair.context.setSinkId(streamSinkId.concat(", ").concat(sinkIdentifier));
            }
          }
        }
      }
    }

    if (!inputNodes.isEmpty()) {
      WindowGenerator windowGenerator = setupWindowGenerator(smallestCheckpointedWindowId);
      for (OperatorDeployInfo ndi : inputNodes) {
        generators.put(ndi.id, windowGenerator);

        Node<?> node = nodes.get(ndi.id);
        SweepableReservoir reservoir = windowGenerator.acquireReservoir(String.valueOf(ndi.id), 1024);
        if (ndi.checkpointWindowId > 0) {
          node.connectInputPort(Node.INPUT, new WindowIdActivatedReservoir(Integer.toString(ndi.id), reservoir, ndi.checkpointWindowId));
        }
        node.connectInputPort(Node.INPUT, reservoir);
      }
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

    long millisAtFirstWindow = (finishedWindowId >> 32) * 1000 + windowWidthMillis * (finishedWindowId & WindowGenerator.MAX_WINDOW_ID) + windowWidthMillis;
    windowGenerator.setFirstWindow(millisAtFirstWindow > firstWindowMillis ? millisAtFirstWindow : firstWindowMillis);

    windowGenerator.setWindowWidth(windowWidthMillis);
    windowGenerator.setCheckpointCount(checkpointWindowCount);
    return windowGenerator;
  }

  private OperatorContext setupNode(OperatorDeployInfo ndi, Thread thread)
  {
    failedNodes.remove(ndi.id);
    final Node<?> node = nodes.get(ndi.id);

    OperatorContext operatorContext = new OperatorContext(new Integer(ndi.id), thread, ndi.contextAttributes, containerContext);
    node.setup(operatorContext);
    /* setup context for all the input ports */
    LinkedHashMap<String, PortContextPair<InputPort<?>>> inputPorts = node.getPortMappingDescriptor().inputPorts;
    LinkedHashMap<String, PortContextPair<InputPort<?>>> newInputPorts = new LinkedHashMap<String, PortContextPair<InputPort<?>>>(inputPorts.size());
    for (OperatorDeployInfo.InputDeployInfo idi : ndi.inputs) {
      InputPort<?> port = inputPorts.get(idi.portName).component;
      PortContext context = new PortContext(idi.contextAttributes, operatorContext);
      newInputPorts.put(idi.portName, new PortContextPair<InputPort<?>>(port, context));
      port.setup(context);
    }
    inputPorts.putAll(newInputPorts);
    /* setup context for all the output ports */
    LinkedHashMap<String, PortContextPair<OutputPort<?>>> outputPorts = node.getPortMappingDescriptor().outputPorts;
    LinkedHashMap<String, PortContextPair<OutputPort<?>>> newOutputPorts = new LinkedHashMap<String, PortContextPair<OutputPort<?>>>(outputPorts.size());
    for (OperatorDeployInfo.OutputDeployInfo odi : ndi.outputs) {
      OutputPort<?> port = outputPorts.get(odi.portName).component;
      PortContext context = new PortContext(odi.contextAttributes, operatorContext);
      newOutputPorts.put(odi.portName, new PortContextPair<OutputPort<?>>(port, context));
      port.setup(context);
    }
    outputPorts.putAll(newOutputPorts);
    activeNodes.put(ndi.id, operatorContext);
    logger.info("activating {} in container {}", node, containerId);
    processNodeRequests(false);
    for (NodeActivationListener l : nodeListener) {
      l.activated(node);
    }
    return operatorContext;
  }

  private void teardownNode(OperatorDeployInfo ndi)
  {
    activeNodes.remove(ndi.id);
    final Node<?> node = nodes.get(ndi.id);

    for (NodeActivationListener l : nodeListener) {
      l.deactivated(node);
    }
    node.teardown();
    logger.info("deactivated {}", node.getId());
  }

  @SuppressWarnings({"SleepWhileInLoop", "SleepWhileHoldingLock"})
  public synchronized void activate(final Map<Integer, OperatorDeployInfo> nodeMap, Map<String, ComponentContextPair<Stream, StreamContext>> newStreams)
  {
    for (ComponentContextPair<Stream, StreamContext> pair : newStreams.values()) {
      if (!(pair.component instanceof BufferServerSubscriber)) {
        activeStreams.put(pair.component, pair.context);
        pair.component.activate(pair.context);
      }
    }

    final ConcurrentHashMap<OperatorDeployInfo, OperatorDeployInfo> activatedOrFailed = new ConcurrentHashMap<OperatorDeployInfo, OperatorDeployInfo>();
    for (final OperatorDeployInfo ndi : nodeMap.values()) {
      /*
       * OiO nodes get activated with their primary nodes.
       */
      if (ndi.type == OperatorType.OIO) {
        continue;
      }
      assert (!activeNodes.containsKey(ndi.id));

      final Node<?> node = nodes.get(ndi.id);
      new Thread(Integer.toString(ndi.id).concat("/").concat(ndi.declaredId).concat(":").concat(node.getOperator().getClass().getSimpleName()))
      {
        @Override
        public void run()
        {
          try {
            /* primary operator initialization */
            OperatorContext operatorContext = setupNode(ndi, this);
            activatedOrFailed.put(ndi, ndi);

            /* lets go for OiO operator initializtion */
            for (Entry<Integer, Integer> e : oioNodes.entrySet()) {
              if (e.getValue() == ndi.id) {
                OperatorDeployInfo oiodi = nodeMap.get(e.getKey());
                setupNode(oiodi, this);
                activatedOrFailed.put(oiodi, oiodi);
              }
            }
            node.activate(operatorContext); /* this is a blocking call */
          }
          catch (Throwable ex) {
            logger.error("Node stopped abnormally because of exception", ex);
            failedNodes.add(ndi.id);
          }
          finally {
            for (Entry<Integer, Integer> e : oioNodes.entrySet()) {
              if (e.getValue() == ndi.id) {
                OperatorDeployInfo oiodi = nodeMap.get(e.getKey());
                activatedOrFailed.put(oiodi, oiodi);
                teardownNode(oiodi);
              }
            }
            activatedOrFailed.put(ndi, ndi);
            teardownNode(ndi);
          }
        }

      }.start();

    }

    /**
     * we need to make sure that before any of the operators gets the first message, it's activate.
     */
    try {
      do {
        Thread.sleep(SPIN_MILLIS);
      }
      while (activatedOrFailed.size() < nodeMap.size());
    }
    catch (InterruptedException ex) {
      logger.debug("Activation of Operators interruped", ex);
    }

    for (ComponentContextPair<Stream, StreamContext> pair : newStreams.values()) {
      if (pair.component instanceof BufferServerSubscriber) {
        activeStreams.put(pair.component, pair.context);
        pair.component.activate(pair.context);
      }
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
        collection = new ArrayList<String>();
        groupedInputStreams.put(source, collection);
      }
      collection.add(Integer.toString(ndi.id).concat(Component.CONCAT_SEPARATOR).concat(nidi.portName));
    }
  }

  protected long getFinishedWindowId(OperatorDeployInfo ndi)
  {
    long finishedWindowId;
    if (ndi.contextAttributes != null
            && ndi.contextAttributes.attr(OperatorContext.PROCESSING_MODE) != null
            && ndi.contextAttributes.attr(OperatorContext.PROCESSING_MODE).get() == ProcessingMode.AT_MOST_ONCE) {
      /* this is really not a valid window Id, but it works since the valid window id will be numerically bigger */
      long currentMillis = System.currentTimeMillis();
      long diff = currentMillis - firstWindowMillis;
      long remainder = diff % (windowWidthMillis * (WindowGenerator.MAX_WINDOW_ID + 1));
      long baseSeconds = (currentMillis - remainder) / 1000;
      long windowId = remainder / windowWidthMillis;
      finishedWindowId = baseSeconds << 32 | windowId;
      logger.debug("using at most once on {} at {}", ndi.declaredId, Codec.getStringWindowId(finishedWindowId));
    }
    else {
      finishedWindowId = ndi.checkpointWindowId;
      logger.debug("using at least once on {} at {}", ndi.declaredId, Codec.getStringWindowId(finishedWindowId));
    }
    return finishedWindowId;
  }

  public synchronized void addNodeListener(NodeActivationListener l)
  {
    nodeListener.add(l);
  }

  public synchronized void removeNodeListeneser(NodeActivationListener l)
  {
    nodeListener.remove(l);
  }

  public void addStatsListener(ContainerStatsListener listener)
  {
    if (statsListener.contains(listener)) {
      throw new IllegalStateException("Listener " + listener + " not present!");
    }
    else {
      statsListener.add(listener);
    }
  }

  public void removeStatsListener(ContainerStatsListener listener)
  {
    if (statsListener.contains(listener)) {
      statsListener.remove(listener);
    }
    else {
      throw new IllegalStateException("Listener " + listener + " already present!");
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(StramChild.class);

  @SuppressWarnings("unchecked")
  public void operateListeners(StreamingContainerContext ctx, boolean setup)
  {
    HashSet<Component<ContainerContext>> components = new HashSet<Component<ContainerContext>>();
    try {
      for (NodeActivationListener l : nodeListener) {
        if (l instanceof Component) {
          components.add((Component<ContainerContext>)l);
        }
      }
      for (ContainerStatsListener l : statsListener) {
        if (l instanceof Component) {
          components.add((Component<ContainerContext>)l);
        }
      }
    }
    catch (Exception ex) {
      logger.debug("Exception while adding listeners to components", ex);
    }

    if (setup) {
      for (Component<ContainerContext> c : components) {
        c.setup(ctx);
      }
    }
    else {
      for (Component<ContainerContext> c : components) {
        c.teardown();
      }
    }
  }

}
