/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.api.Context.PortContext;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.Operator.OutputPort;
import com.malhartech.api.Operator.Unifier;
import com.malhartech.api.*;
import com.malhartech.bufferserver.server.Server;
import com.malhartech.bufferserver.storage.DiskStorage;
import com.malhartech.bufferserver.util.Codec;
import com.malhartech.debug.StdOutErrLog;
import com.malhartech.engine.OperatorContext.NodeRequest;
import com.malhartech.engine.Operators.PortMappingDescriptor;
import com.malhartech.engine.*;
import com.malhartech.netlet.DefaultEventLoop;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.ContainerHeartbeat;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingNodeHeartbeat;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingNodeHeartbeat.DNodeState;
import com.malhartech.stream.*;
import com.malhartech.util.AttributeMap;
import com.malhartech.util.ScheduledThreadPoolExecutor;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.Thread.State;
import java.net.*;
import java.security.PrivilegedExceptionAction;
import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * The main() for streaming container processes launched by {@link com.malhartech.stram.StramAppMaster}.<p>
 * <br>
 *
 */
public class StramChild
{
  public static final String NODE_PORT_CONCAT_SEPARATOR = ".";
  public static final int PORT_QUEUE_CAPACITY = 1024;
  private static final int SPIN_MILLIS = 20;
  private final String containerId;
  private final Configuration conf;
  private final StreamingContainerUmbilicalProtocol umbilical;
  protected final Map<Integer, Node<?>> nodes = new ConcurrentHashMap<Integer, Node<?>>();
  private final Map<String, ComponentContextPair<Stream, StreamContext>> streams = new ConcurrentHashMap<String, ComponentContextPair<Stream, StreamContext>>();
  protected final Map<Integer, WindowGenerator> generators = new ConcurrentHashMap<Integer, WindowGenerator>();
  protected final Map<Integer, OperatorContext> activeNodes = new ConcurrentHashMap<Integer, OperatorContext>();
  private final Map<Stream, StreamContext> activeStreams = new ConcurrentHashMap<Stream, StreamContext>();
  private final Map<WindowGenerator, Object> activeGenerators = new ConcurrentHashMap<WindowGenerator, Object>();
  private int heartbeatIntervalMillis = 1000;
  private volatile boolean exitHeartbeatLoop = false;
  private final Object heartbeatTrigger = new Object();
  private String checkpointFsPath;
  private String appPath;
  public static DefaultEventLoop eventloop;

  static {
    try {
      eventloop = new DefaultEventLoop("ProcessWideEventLoop");
    }
    catch (IOException io) {
      throw new RuntimeException(io);
    }
  }

  /**
   * Map of last backup window id that is used to communicate checkpoint state back to Stram. TODO: Consider adding this to the node context instead.
   */
  private long firstWindowMillis;
  private int windowWidthMillis;
  private InetSocketAddress bufferServerAddress;
  private com.malhartech.bufferserver.server.Server bufferServer;
  private AttributeMap<DAGContext> applicationAttributes;
  protected HashMap<String, TupleRecorder> tupleRecorders = new HashMap<String, TupleRecorder>();
  private int tupleRecordingPartFileSize;
  private String daemonAddress;
  private long tupleRecordingPartFileTimeMillis;
  private int checkpointWindowCount;

  protected StramChild(String containerId, Configuration conf, StreamingContainerUmbilicalProtocol umbilical)
  {
    logger.debug("instantiated StramChild {}", containerId);
    this.umbilical = umbilical;
    this.containerId = containerId;
    this.conf = conf;
  }

  public void setup(StreamingContainerContext ctx)
  {
    this.applicationAttributes = ctx.applicationAttributes;
    heartbeatIntervalMillis = ctx.applicationAttributes.attrValue(DAG.STRAM_HEARTBEAT_INTERVAL_MILLIS, 1000);
    firstWindowMillis = ctx.startWindowMillis;
    windowWidthMillis = ctx.applicationAttributes.attrValue(DAG.STRAM_WINDOW_SIZE_MILLIS, 500);
    checkpointWindowCount = ctx.applicationAttributes.attrValue(DAG.STRAM_CHECKPOINT_WINDOW_COUNT, 60);

    this.appPath = ctx.applicationAttributes.attrValue(DAG.STRAM_APP_PATH, "app-dfs-path-not-configured");
    this.checkpointFsPath = this.appPath + "/" + DAG.SUBDIR_CHECKPOINTS;
    this.tupleRecordingPartFileSize = ctx.applicationAttributes.attrValue(DAG.STRAM_TUPLE_RECORDING_PART_FILE_SIZE, 100 * 1024);
    this.tupleRecordingPartFileTimeMillis = ctx.applicationAttributes.attrValue(DAG.STRAM_TUPLE_RECORDING_PART_FILE_TIME_MILLIS, 30 * 60 * 60 * 1000);
    this.daemonAddress = ctx.applicationAttributes.attrValue(DAG.STRAM_DAEMON_ADDRESS, null);

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
  }

  public String getContainerId()
  {
    return this.containerId;
  }

  public TupleRecorder getTupleRecorder(int operId, String portName)
  {
    return tupleRecorders.get(getRecorderKey(operId, portName));
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

    logger.info("Child starting with classpath: {}", System.getProperty("java.class.path"));

    final Configuration defaultConf = new Configuration();
    //defaultConf.addResource(MRJobConfig.JOB_CONF_FILE);
    UserGroupInformation.setConfiguration(defaultConf);

    String host = args[0];
    int port = Integer.parseInt(args[1]);
    final InetSocketAddress address =
            NetUtils.createSocketAddrForHost(host, port);

    final String childId = System.getProperty("stram.cid");

    //Token<JobTokenIdentifier> jt = loadCredentials(defaultConf, address);

    UserGroupInformation taskOwner;
    if (!UserGroupInformation.isSecurityEnabled()) {
      // Communicate with parent as actual task owner.
      taskOwner =
              UserGroupInformation.createRemoteUser(StramChild.class.getName());
    }
    else {
      taskOwner = UserGroupInformation.getCurrentUser();
    }
    logger.info("Task owner is " + taskOwner.getUserName());

    //taskOwner.addToken(jt);
    final StreamingContainerUmbilicalProtocol umbilical =
            taskOwner.doAs(new PrivilegedExceptionAction<StreamingContainerUmbilicalProtocol>()
    {
      @Override
      public StreamingContainerUmbilicalProtocol run() throws Exception
      {
        return RPC.getProxy(StreamingContainerUmbilicalProtocol.class,
                            StreamingContainerUmbilicalProtocol.versionID, address, defaultConf);
      }

    });

    logger.debug("PID: " + System.getenv().get("JVM_PID"));
    UserGroupInformation childUGI;
    int exitStatus = 1;

    try {
      if (!UserGroupInformation.isSecurityEnabled()) {
        childUGI = UserGroupInformation.createRemoteUser(System.getenv(ApplicationConstants.Environment.USER.toString()));
        // Add tokens to new user so that it may execute its task correctly.
        for (Token<?> token: UserGroupInformation.getCurrentUser().getTokens()) {
          childUGI.addToken(token);
        }
      }
      else {
        childUGI = taskOwner;
      }

      childUGI.doAs(new PrivilegedExceptionAction<Object>()
      {
        @Override
        public Object run() throws Exception
        {
          StreamingContainerContext ctx = umbilical.getInitContext(childId);
          StramChild stramChild = new StramChild(childId, defaultConf, umbilical);
          logger.debug("Got context: " + ctx);
          stramChild.setup(ctx);
          try {
            // main thread enters heartbeat loop
            stramChild.heartbeatLoop();
          }
          finally {
            // teardown
            stramChild.teardown();
          }
          return null;
        }

      });
      exitStatus = 0;
    }
    catch (Exception exception) {
      logger.warn("Exception running child : "
              + StringUtils.stringifyException(exception));
      // Report back any failures, for diagnostic purposes
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

    for (Entry<Integer, Node<?>> e: nodes.entrySet()) {
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
      for (Thread t: activeThreads) {
        t.join();
        disconnectNode(iterator.next());
      }
      assert (activeNodes.isEmpty());
    }
    catch (InterruptedException ex) {
      logger.info("Aborting wait for for operators to get deactivated as got interrupted with {}", ex);
    }

    for (WindowGenerator wg: activeGenerators.keySet()) {
      wg.deactivate();
    }
    activeGenerators.clear();

    for (Stream stream: activeStreams.keySet()) {
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
      String sourceIdentifier = String.valueOf(nodeid).concat(NODE_PORT_CONCAT_SEPARATOR).concat(outputPorts.next());
      ComponentContextPair<Stream, StreamContext> pair = streams.remove(sourceIdentifier);
      if (pair != null) {
        if (activeStreams.remove(pair.component) != null) {
          pair.component.deactivate();
        }

        if (pair.component.isMultiSinkCapable()) {
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
      String sinkIdentifier = String.valueOf(nodeid).concat(NODE_PORT_CONCAT_SEPARATOR).concat(inputPorts.next());
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
      SweepableReservoir releaseReservoir = chosen1.releaseReservoir(Integer.toString(nodeid).concat(NODE_PORT_CONCAT_SEPARATOR).concat(Node.INPUT));
      // should we send the closure of the port to the node?

      int count = 0;
      for (WindowGenerator wg: generators.values()) {
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
    for (OperatorDeployInfo ndi: nodeList) {
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
    for (OperatorDeployInfo ndi: nodeList) {
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
      for (Thread t: joinList) {
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

    for (OperatorDeployInfo ndi: nodeList) {
      nodes.remove(ndi.id);
    }
  }

  public void teardown()
  {
    deactivate();

    assert (streams.isEmpty());

    nodes.clear();

    for (TupleRecorder entry: tupleRecorders.values()) {
      entry.teardown();
    }
    tupleRecorders.clear();

    HashSet<WindowGenerator> gens = new HashSet<WindowGenerator>();
    gens.addAll(generators.values());
    generators.clear();
    for (WindowGenerator wg: gens) {
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

      List<StreamingNodeHeartbeat> heartbeats = new ArrayList<StreamingNodeHeartbeat>(nodes.size());

      // gather heartbeat info for all operators
      for (Map.Entry<Integer, Node<?>> e: nodes.entrySet()) {
        StreamingNodeHeartbeat hb = new StreamingNodeHeartbeat();
        hb.setNodeId(e.getKey());
        hb.setGeneratedTms(currentTime);
        hb.setIntervalMs(heartbeatIntervalMillis);
        if (activeNodes.containsKey(e.getKey())) {
          activeNodes.get(e.getKey()).drainHeartbeatCounters(hb.getWindowStats());
          hb.setState(DNodeState.ACTIVE.toString());
        }
        else {
          hb.setState(e.getValue().isAlive() ? DNodeState.FAILED.toString() : DNodeState.IDLE.toString());
        }

        TupleRecorder tupleRecorder = tupleRecorders.get(String.valueOf(e.getKey()));
        if (tupleRecorder != null) {
          hb.addRecordingName(tupleRecorder.getRecordingName());
        }
        PortMappingDescriptor portMappingDescriptor = e.getValue().getPortMappingDescriptor();
        for (String portName: portMappingDescriptor.inputPorts.keySet()) {
          tupleRecorder = tupleRecorders.get(this.getRecorderKey(e.getKey(), portName));
          if (tupleRecorder != null) {
            hb.addRecordingName(tupleRecorder.getRecordingName());
          }
          if (bufferServerAddress != null) {
            String streamId = e.getKey().toString().concat(StramChild.NODE_PORT_CONCAT_SEPARATOR).concat(portName);
            ComponentContextPair<Stream, StreamContext> stream = streams.get(streamId);
            if (stream != null && (stream.component instanceof ByteCounterStream)) {
              hb.setBufferServerBytes(portName, ((ByteCounterStream)stream.component).getByteCount(true));
            }
          }
        }
        for (String portName: portMappingDescriptor.outputPorts.keySet()) {
          tupleRecorder = tupleRecorders.get(this.getRecorderKey(e.getKey(), portName));
          if (tupleRecorder != null) {
            hb.addRecordingName(tupleRecorder.getRecordingName());
          }
          if (bufferServerAddress != null) {
            String streamId = e.getKey().toString().concat(StramChild.NODE_PORT_CONCAT_SEPARATOR).concat(portName);
            ComponentContextPair<Stream, StreamContext> stream = streams.get(streamId);
            if (stream != null && (stream.component instanceof ByteCounterStream)) {
              hb.setBufferServerBytes(portName, ((ByteCounterStream)stream.component).getByteCount(true));
            }
          }
        }
        heartbeats.add(hb);
      }
      msg.setDnodeEntries(heartbeats);

      // heartbeat call and follow-up processing
      //logger.debug("Sending heartbeat for {} operators.", msg.getDnodeEntries().size());
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

  private long lastCommittedWindowId = -1;

  protected void processHeartbeatResponse(ContainerHeartbeatResponse rsp)
  {
    if (rsp.shutdown) {
      logger.info("Received shutdown request");
      this.exitHeartbeatLoop = true;
      return;
    }

    if (rsp.undeployRequest != null) {
      logger.info("Undeploy request: {}", rsp.undeployRequest);
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

    NodeRequest nr = null;
    if (rsp.committedWindowId != lastCommittedWindowId) {
      lastCommittedWindowId = rsp.committedWindowId;
      for (Entry<Integer, OperatorContext> e: activeNodes.entrySet()) {
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

    if (rsp.nodeRequests != null) {
      // processing of per operator requests
      for (StramToNodeRequest req: rsp.nodeRequests) {
        OperatorContext oc = activeNodes.get(req.getOperatorId());
        if (oc == null) {
          logger.warn("Received request with invalid operator id {} ({})", req.getOperatorId(), req);
        }
        else {
          logger.debug("Stram request: {}", req);
          processStramRequest(oc, req);
        }
      }
    }
  }

  private int getOutputQueueCapacity(List<OperatorDeployInfo> operatorList, int sourceOperatorId, String sourcePortName)
  {
    for (OperatorDeployInfo odi: operatorList) {
      if (odi.id == sourceOperatorId) {
        for (OperatorDeployInfo.OutputDeployInfo odiodi: odi.outputs) {
          if (odiodi.portName.equals(sourcePortName)) {
            return odiodi.contextAttributes.attrValue(PortContext.QUEUE_CAPACITY, PORT_QUEUE_CAPACITY);
          }
        }
      }
    }

    return PORT_QUEUE_CAPACITY;
  }

  /**
   * Process request from stram for further communication through the protocol. Extended reporting is on a per node basis (won't occur under regular operation)
   *
   * @param n
   * @param snr
   */
  private void processStramRequest(OperatorContext context, final StramToNodeRequest snr)
  {
    int operatorId = snr.getOperatorId();
    final Node<?> node = nodes.get(operatorId);
    switch (snr.getRequestType()) {
      case START_RECORDING: {
        final String portName = snr.getPortName();
        logger.debug("Received start recording request for {}", getRecorderKey(operatorId, portName));
        context.request(new OperatorContext.NodeRequest()
        {
          @Override
          public void execute(Operator operator, int operatorId, long windowId) throws IOException
          {
            startRecording(node, operatorId, portName, false);
          }

          @Override
          public String toString()
          {
            return "Start Recording";
          }

        });
      }
      break;

      case STOP_RECORDING: {
        final String portName = snr.getPortName();
        logger.debug("Received stop recording request for {}", getRecorderKey(operatorId, portName));

        context.request(new OperatorContext.NodeRequest()
        {
          @Override
          public void execute(Operator operator, int operatorId, long windowId) throws IOException
          {
            stopRecording(node, operatorId, portName);
          }

          @Override
          public String toString()
          {
            return "Stop Recording";
          }

        });
      }
      break;

      case SYNC_RECORDING: {
        final String portName = snr.getPortName();
        logger.debug("Received sync recording request for {}", getRecorderKey(operatorId, portName));

        context.request(new OperatorContext.NodeRequest()
        {
          @Override
          public void execute(Operator operator, int operatorId, long windowId) throws IOException
          {
            syncRecording(node, operatorId, portName);
          }

          @Override
          public String toString()
          {
            return "Recording Request";
          }

        });
      }
      break;

      case SET_PROPERTY:
        context.request(new OperatorContext.NodeRequest()
        {
          @Override
          public void execute(Operator operator, int id, long windowId) throws IOException
          {
            final Map<String, String> properties = Collections.singletonMap(snr.setPropertyKey, snr.setPropertyValue);
            logger.info("Setting property {} on operator {}", properties, operator);
            DAGPropertiesBuilder.setOperatorProperties(operator, properties);
          }

          @Override
          public String toString()
          {
            return "Set Property";
          }

        });
        break;

      default:
        logger.error("Unknown request {}", snr);
    }
  }

  private synchronized void deploy(List<OperatorDeployInfo> nodeList) throws Exception
  {
    /*
     * A little bit of up front sanity check would reduce the percentage of deploy failures later.
     */
    for (OperatorDeployInfo ndi: nodeList) {
      if (nodes.containsKey(ndi.id)) {
        throw new IllegalStateException("Node with id: " + ndi.id + " already present in the container");
      }
    }

    deployNodes(nodeList);

    HashMap<String, ArrayList<String>> groupedInputStreams = new HashMap<String, ArrayList<String>>();
    for (OperatorDeployInfo ndi: nodeList) {
      groupInputStreams(groupedInputStreams, ndi);
    }

    HashMap<String, ComponentContextPair<Stream, StreamContext>> newStreams = deployOutputStreams(nodeList, groupedInputStreams);
    deployInputStreams(nodeList, newStreams);
    for (ComponentContextPair<Stream, StreamContext> pair: newStreams.values()) {
      pair.component.setup(pair.context);
    }
    streams.putAll(newStreams);

    activate(nodeList, newStreams);
  }

  private void massageUnifierDeployInfo(OperatorDeployInfo odi)
  {
    for (OperatorDeployInfo.InputDeployInfo idi: odi.inputs) {
      idi.portName += "(" + idi.sourceNodeId + NODE_PORT_CONCAT_SEPARATOR + idi.sourcePortName + ")";
    }
  }

  @SuppressWarnings({"unchecked"})
  private void deployNodes(List<OperatorDeployInfo> nodeList) throws Exception
  {
    for (OperatorDeployInfo ndi: nodeList) {
      BackupAgent backupAgent;
      OperatorCodec operatorSerDe;
      if (ndi.contextAttributes == null) {
        backupAgent = new HdfsBackupAgent(this.conf, this.checkpointFsPath, operatorSerDe = StramUtils.getNodeSerDe(null));
      }
      else {
        backupAgent = ndi.contextAttributes.attr(OperatorContext.BACKUP_AGENT).get();
        if (backupAgent == null) {
          backupAgent = new HdfsBackupAgent(this.conf, this.checkpointFsPath, operatorSerDe = StramUtils.getNodeSerDe(null));
          ndi.contextAttributes.attr(OperatorContext.BACKUP_AGENT).set(backupAgent);
        }
        else {
          operatorSerDe = backupAgent.getOperatorSerDe();
        }
      }

      try {
        final Object foreignObject;
        if (ndi.checkpointWindowId > 0) {
          logger.debug("Restoring node {} to checkpoint {}", ndi.id, Codec.getStringWindowId(ndi.checkpointWindowId));
          foreignObject = backupAgent.restore(ndi.id, ndi.checkpointWindowId);
        }
        else {
          foreignObject = operatorSerDe.read(new ByteArrayInputStream(ndi.serializedNode));
        }

        if (foreignObject instanceof InputOperator && ndi.type == OperatorDeployInfo.OperatorType.INPUT) {
          nodes.put(ndi.id, new InputNode(ndi.id, (InputOperator)foreignObject));
        }
        else if (foreignObject instanceof Unifier && ndi.type == OperatorDeployInfo.OperatorType.UNIFIER) {
          nodes.put(ndi.id, new UnifierNode(ndi.id, (Unifier<Object>)foreignObject));
          massageUnifierDeployInfo(ndi);
        }
        else {
          nodes.put(ndi.id, new GenericNode(ndi.id, (Operator)foreignObject));
        }
      }
      catch (Exception e) {
        logger.error(e.getLocalizedMessage());
        throw e;
      }
    }
  }

  private HashMap.SimpleEntry<String, ComponentContextPair<Stream, StreamContext>> deployBufferServerPublisher(
          String sourceIdentifier, long startingWindowId, int queueCapacity, OperatorDeployInfo.OutputDeployInfo nodi)
          throws UnknownHostException
  {
    /*
     * Although there is a node in this container interested in output placed on this stream, there
     * seems to at least one more party interested but placed in a container other than this one.
     */
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

    BufferServerPublisher bsp = new BufferServerPublisher(sourceIdentifier, queueCapacity);
    return new HashMap.SimpleEntry<String, ComponentContextPair<Stream, StreamContext>>(sinkIdentifier, new ComponentContextPair<Stream, StreamContext>(bsp, bssc));
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
    for (OperatorDeployInfo ndi: nodeList) {
      Node<?> node = nodes.get(ndi.id);
      long finishedWindowId = ndi.checkpointWindowId > 0 ? ndi.checkpointWindowId : 0;

      for (OperatorDeployInfo.OutputDeployInfo nodi: ndi.outputs) {
        String sourceIdentifier = Integer.toString(ndi.id).concat(NODE_PORT_CONCAT_SEPARATOR).concat(nodi.portName);
        int queueCapacity = nodi.contextAttributes.attrValue(PortContext.QUEUE_CAPACITY, PORT_QUEUE_CAPACITY);
        logger.debug("for stream {} the queue capacity is {}", sourceIdentifier, queueCapacity);

        ArrayList<String> collection = groupedInputStreams.get(sourceIdentifier);
        if (collection == null) {
          assert (nodi.isInline() == false): "resulting stream cannot be inline: " + nodi;
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

          if (!nodi.isInline()) {
//            StreamContext context = new StreamContext(nodi.declaredStreamId);
//            context.setSourceId(sourceIdentifier);
//            context.setFinishedWindowId(startingWindowId);
//
//            Stream inlineStream = new InlineStream(queueCapacity);
//            pair.component.setSink(, inlineStream);
//          }
//          else {
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
            pair.component.setSink(deployBufferServerPublisher.getKey(), deployBufferServerPublisher.getValue().component);
          }
        }
      }
    }

    //streams.putAll(newStreams);
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
    String identifier = String.valueOf(operatorId).concat(NODE_PORT_CONCAT_SEPARATOR).concat(portname);
    ComponentContextPair<Stream, StreamContext> spair = streams.get(identifier);
    if (spair == null) {
      return null;
    }

    return spair.context.getId();
  }

  private void deployInputStreams(List<OperatorDeployInfo> operatorList, HashMap<String, ComponentContextPair<Stream, StreamContext>> newStreams)
          throws UnknownHostException
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
    for (OperatorDeployInfo ndi: operatorList) {
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
        if (ndi.checkpointWindowId < smallestCheckpointedWindowId) {
          smallestCheckpointedWindowId = ndi.checkpointWindowId;
        }
      }
      else {
        Node<?> node = nodes.get(ndi.id);

        for (OperatorDeployInfo.InputDeployInfo nidi: ndi.inputs) {
          String sourceIdentifier = Integer.toString(nidi.sourceNodeId).concat(NODE_PORT_CONCAT_SEPARATOR).concat(nidi.sourcePortName);
          String sinkIdentifier = Integer.toString(ndi.id).concat(NODE_PORT_CONCAT_SEPARATOR).concat(nidi.portName);

          int queueCapacity = nidi.contextAttributes == null ? PORT_QUEUE_CAPACITY : nidi.contextAttributes.attrValue(PortContext.QUEUE_CAPACITY, PORT_QUEUE_CAPACITY);
          long finishedWindowId = ndi.checkpointWindowId > 0 ? ndi.checkpointWindowId : 0;

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
            assert (nidi.isInline() == false);

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

            BufferServerSubscriber stream = new BufferServerSubscriber("tcp://".concat(nidi.bufferServerHost).concat(":").concat(String.valueOf(nidi.bufferServerPort)).concat("/").concat(sourceIdentifier), queueCapacity);

            SweepableReservoir reservoir = stream.acquireReservoir(sinkIdentifier, queueCapacity);
            if (finishedWindowId > 0) {
              node.connectInputPort(nidi.portName, new WindowIdActivatedReservoir(sinkIdentifier, reservoir, finishedWindowId));
            }
            node.connectInputPort(nidi.portName, reservoir);

            newStreams.put(sinkIdentifier, new ComponentContextPair<Stream, StreamContext>(stream, context));
            logger.debug("put input stream {} against key {}", stream, sinkIdentifier);
          }
          else {
            assert (nidi.isInline());
            /* we are still dealing with the MuxStream originating at the output of the source port */
            StreamContext inlineContext = new StreamContext(nidi.declaredStreamId);
            inlineContext.setSourceId(sourceIdentifier);
            inlineContext.setSinkId(sinkIdentifier);

            int outputQueueCapacity = getOutputQueueCapacity(operatorList, nidi.sourceNodeId, nidi.sourcePortName);
            if (outputQueueCapacity > queueCapacity) {
              queueCapacity = outputQueueCapacity;
            }
            InlineStream stream = new InlineStream(queueCapacity);
            newStreams.put(sinkIdentifier, new ComponentContextPair<Stream, StreamContext>(stream, inlineContext));
            if (finishedWindowId > 0) {
              node.connectInputPort(nidi.portName, new WindowIdActivatedReservoir(sinkIdentifier, stream, finishedWindowId));
            }
            node.connectInputPort(nidi.portName, stream);

            if (!pair.component.isMultiSinkCapable()) {
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
              pair.component.setSink(sinkIdentifier, stream);
            }
            else {
              /*
               * generally speaking we do not have partitions on the inline streams so the control should not
               * come here but if it comes, then we are ready to handle it using the partition aware streams.
               */
              PartitionAwareSink<Object> pas = new PartitionAwareSink<Object>(StramUtils.getSerdeInstance(nidi.serDeClassName), nidi.partitionKeys, nidi.partitionMask, stream);
              pair.component.setSink(sinkIdentifier, pas);
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
      for (OperatorDeployInfo ndi: inputNodes) {
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

  // take the recording mess out of here into something modular.
  @SuppressWarnings({"SleepWhileInLoop", "SleepWhileHoldingLock"})
  public synchronized void activate(List<OperatorDeployInfo> nodeList, Map<String, ComponentContextPair<Stream, StreamContext>> newStreams)
  {
    for (ComponentContextPair<Stream, StreamContext> pair: newStreams.values()) {
      if (!(pair.component instanceof BufferServerSubscriber)) {
        activeStreams.put(pair.component, pair.context);
        pair.component.activate(pair.context);
      }
    }

    final AtomicInteger activatedNodeCount = new AtomicInteger(activeNodes.size());
    for (final OperatorDeployInfo ndi: nodeList) {
      final Node<?> node = nodes.get(ndi.id);
      final Map<String, AttributeMap<PortContext>> inputPortAttributes = new HashMap<String, AttributeMap<PortContext>>();
      final Map<String, AttributeMap<PortContext>> outputPortAttributes = new HashMap<String, AttributeMap<PortContext>>();
      assert (!activeNodes.containsKey(ndi.id));

      for (OperatorDeployInfo.InputDeployInfo idi: ndi.inputs) {
        inputPortAttributes.put(idi.portName, idi.contextAttributes);
      }
      for (OperatorDeployInfo.OutputDeployInfo odi: ndi.outputs) {
        outputPortAttributes.put(odi.portName, odi.contextAttributes);
      }

      new Thread(Integer.toString(ndi.id).concat("/").concat(ndi.declaredId).concat(":").concat(node.getOperator().getClass().getSimpleName()))
      {
        @Override
        public void run()
        {
          try {
            OperatorContext context = new OperatorContext(new Integer(ndi.id), this, ndi.contextAttributes, applicationAttributes, inputPortAttributes, outputPortAttributes);
            node.getOperator().setup(context);
            for (Map.Entry<String, AttributeMap<PortContext>> entry: inputPortAttributes.entrySet()) {
              AttributeMap<PortContext> attrMap = entry.getValue();
              if (attrMap != null && attrMap.attrValue(PortContext.AUTO_RECORD, false)) {
                logger.info("Automatically start recording for operator {}, input port {}", ndi.id, entry.getKey());
                startRecording(node, ndi.id, entry.getKey(), true);
              }
            }
            for (Map.Entry<String, AttributeMap<PortContext>> entry: outputPortAttributes.entrySet()) {
              AttributeMap<PortContext> attrMap = entry.getValue();
              if (attrMap != null && attrMap.attrValue(PortContext.AUTO_RECORD, false)) {
                logger.info("Automatically start recording for operator {}, output port {}", ndi.id, entry.getKey());
                startRecording(node, ndi.id, entry.getKey(), true);
              }
            }

            activeNodes.put(ndi.id, context);

            activatedNodeCount.incrementAndGet();
            logger.info("activating {} in container {}", node, containerId);
            node.activate(context);
          }
          catch (Throwable ex) {
            logger.error("Node stopped abnormally because of exception", ex);
          }
          finally {
            activeNodes.remove(ndi.id);
            node.getOperator().teardown();
            logger.info("deactivated {}", node.id);
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
      while (activatedNodeCount.get() < nodes.size());
    }
    catch (InterruptedException ex) {
      logger.debug(ex.getLocalizedMessage());
    }

    for (ComponentContextPair<Stream, StreamContext> pair: newStreams.values()) {
      if (pair.component instanceof BufferServerSubscriber) {
        activeStreams.put(pair.component, pair.context);
        pair.component.activate(pair.context);
      }
    }

    for (WindowGenerator wg: generators.values()) {
      if (!activeGenerators.containsKey(wg)) {
        activeGenerators.put(wg, generators);
        wg.activate(null);
      }
    }
  }

  private void groupInputStreams(HashMap<String, ArrayList<String>> groupedInputStreams, OperatorDeployInfo ndi)
  {
    for (OperatorDeployInfo.InputDeployInfo nidi: ndi.inputs) {
      String source = Integer.toString(nidi.sourceNodeId).concat(NODE_PORT_CONCAT_SEPARATOR).concat(nidi.sourcePortName);

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
      collection.add(Integer.toString(ndi.id).concat(NODE_PORT_CONCAT_SEPARATOR).concat(nidi.portName));
    }
  }

  private String getRecorderKey(int operatorId, String portName)
  {
    return String.valueOf(operatorId) + (portName != null ? ("$" + portName) : "");
  }

  private void startRecording(Node<?> node, int operatorId, String portName, boolean recordEvenIfNotConnected)
  {
    PortMappingDescriptor descriptor = node.getPortMappingDescriptor();
    String operatorPortName = getRecorderKey(operatorId, portName);
    // check any recording conflict
    boolean conflict = false;
    if (tupleRecorders.containsKey(String.valueOf(operatorId))) {
      conflict = true;
    }
    else if (portName == null) {
      for (Map.Entry<String, InputPort<?>> entry: descriptor.inputPorts.entrySet()) {
        if (tupleRecorders.containsKey(getRecorderKey(operatorId, entry.getKey()))) {
          conflict = true;
          break;
        }
      }
      for (Map.Entry<String, OutputPort<?>> entry: descriptor.outputPorts.entrySet()) {
        if (tupleRecorders.containsKey(getRecorderKey(operatorId, entry.getKey()))) {
          conflict = true;
          break;
        }
      }
    }
    else {
      if (tupleRecorders.containsKey(operatorPortName)) {
        conflict = true;
      }
    }
    if (!conflict) {
      logger.debug("Executing start recording request for " + operatorPortName);

      TupleRecorder tupleRecorder = new TupleRecorder();
      String basePath = StramChild.this.appPath + "/recordings/" + operatorId + "/" + tupleRecorder.getStartTime();
      String defaultName = StramChild.this.containerId + "_" + operatorPortName + "_" + tupleRecorder.getStartTime();
      tupleRecorder.setRecordingName(defaultName);
      tupleRecorder.getStorage().setBasePath(basePath);
      tupleRecorder.getStorage().setBytesPerPartFile(StramChild.this.tupleRecordingPartFileSize);
      tupleRecorder.getStorage().setMillisPerPartFile(StramChild.this.tupleRecordingPartFileTimeMillis);
      if (StramChild.this.daemonAddress != null) {
        String url = "ws://" + StramChild.this.daemonAddress + "/pubsub";
        try {
          tupleRecorder.setPubSubUrl(url);
        }
        catch (URISyntaxException ex) {
          logger.warn("URL {} is not valid. NOT posting live tuples to daemon.", url, ex);
        }
      }
      HashMap<String, Sink<Object>> sinkMap = new HashMap<String, Sink<Object>>();
      for (Map.Entry<String, InputPort<?>> entry: descriptor.inputPorts.entrySet()) {
        String streamId = getDeclaredStreamId(operatorId, entry.getKey());
        if (recordEvenIfNotConnected && streamId == null) {
          streamId = portName + "_implicit_stream";
        }
        if (streamId != null && (portName == null || entry.getKey().equals(portName))) {
          logger.info("Adding recorder sink to input port {}, stream {}", entry.getKey(), streamId);
          tupleRecorder.addInputPortInfo(entry.getKey(), streamId);
          sinkMap.put(entry.getKey(), tupleRecorder.newSink(entry.getKey()));
        }
      }
      for (Map.Entry<String, OutputPort<?>> entry: descriptor.outputPorts.entrySet()) {
        String streamId = getDeclaredStreamId(operatorId, entry.getKey());
        if (recordEvenIfNotConnected && streamId == null) {
          streamId = portName + "_implicit_stream";
        }
        if (streamId != null && (portName == null || entry.getKey().equals(portName))) {
          logger.info("Adding recorder sink to output port {}, stream {}", entry.getKey(), streamId);
          tupleRecorder.addOutputPortInfo(entry.getKey(), streamId);
          sinkMap.put(entry.getKey(), tupleRecorder.newSink(entry.getKey()));
        }
      }
      if (!sinkMap.isEmpty()) {
        logger.debug("Started recording (name: {}) to base path {}", operatorPortName, basePath);
        node.addSinks(sinkMap);
        tupleRecorder.setup(node.getOperator());
        tupleRecorders.put(operatorPortName, tupleRecorder);
      }
      else {
        logger.warn("Tuple recording request ignored because operator is not connected on the specified port.");
      }
    }
    else {
      logger.error("Operator id {} is already being recorded.", operatorPortName);
    }
  }

  private void stopRecording(Node<?> node, int operatorId, String portName)
  {
    String operatorPortName = getRecorderKey(operatorId, portName);
    if (tupleRecorders.containsKey(operatorPortName)) {
      logger.debug("Executing stop recording request for {}", operatorPortName);

      TupleRecorder tupleRecorder = tupleRecorders.get(operatorPortName);
      if (tupleRecorder != null) {
        node.removeSinks(tupleRecorder.getSinkMap());
        tupleRecorder.teardown();
        logger.debug("Stopped recording for operator/port {}", operatorPortName);
        tupleRecorders.remove(operatorPortName);
      }
    }
    else {
      logger.error("Operator/port {} is not being recorded.", operatorPortName);
    }
  }

  private void syncRecording(Node<?> node, int operatorId, String portName)
  {
    String operatorPortName = getRecorderKey(operatorId, portName);
    if (tupleRecorders.containsKey(operatorPortName)) {
      logger.debug("Executing sync recording request for {}" + operatorPortName);

      TupleRecorder tupleRecorder = tupleRecorders.get(operatorPortName);
      if (tupleRecorder != null) {
        tupleRecorder.getStorage().requestSync();
        logger.debug("Requested sync recording for operator/port {}" + operatorPortName);
      }
    }
    else {
      logger.error("(SYNC_RECORDING) Operator/port " + operatorPortName + " is not being recorded.");
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(StramChild.class);
}
