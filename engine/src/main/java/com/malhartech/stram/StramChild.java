/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.Operator.OutputPort;
import com.malhartech.api.Operator.Unifier;
import com.malhartech.api.*;
import com.malhartech.bufferserver.server.Server;
import com.malhartech.bufferserver.storage.DiskStorage;
import com.malhartech.bufferserver.util.Codec;
import com.malhartech.engine.Operators.PortMappingDescriptor;
import com.malhartech.engine.*;
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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.Map.Entry;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import malhar.netlet.DefaultEventLoop;
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
  private static final Logger logger = LoggerFactory.getLogger(StramChild.class);
  private static final String NODE_PORT_SPLIT_SEPARATOR = "\\.";
  public static final String NODE_PORT_CONCAT_SEPARATOR = ".";
  private static final int SPIN_MILLIS = 20;
  private final String containerId;
  private final Configuration conf;
  private final StreamingContainerUmbilicalProtocol umbilical;
  protected final Map<Integer, Node<?>> nodes = new ConcurrentHashMap<Integer, Node<?>>();
  private final Map<String, ComponentContextPair<Stream<Object>, StreamContext>> streams = new ConcurrentHashMap<String, ComponentContextPair<Stream<Object>, StreamContext>>();
  protected final Map<Integer, WindowGenerator> generators = new ConcurrentHashMap<Integer, WindowGenerator>();
  protected final Map<Integer, OperatorContext> activeNodes = new ConcurrentHashMap<Integer, OperatorContext>();
  private final Map<Stream<?>, StreamContext> activeStreams = new ConcurrentHashMap<Stream<?>, StreamContext>();
  private final Map<WindowGenerator, Object> activeGenerators = new ConcurrentHashMap<WindowGenerator, Object>();
  private int heartbeatIntervalMillis = 1000;
  private volatile boolean exitHeartbeatLoop = false;
  private final Object heartbeatTrigger = new Object();
  private String checkpointFsPath;
  private String appPath;
  public static final DefaultEventLoop eventloop;

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
  private final Map<Integer, Long> backupInfo = new ConcurrentHashMap<Integer, Long>();
  private long firstWindowMillis;
  private int windowWidthMillis;
  private InetSocketAddress bufferServerAddress;
  private com.malhartech.bufferserver.server.Server bufferServer;
  private AttributeMap<DAGContext> applicationAttributes;
  protected HashMap<String, TupleRecorder> tupleRecorders = new HashMap<String, TupleRecorder>();
  private int tupleRecordingPartFileSize;
  private String daemonAddress;
  private long tupleRecordingPartFileTimeMillis;

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

    this.appPath = ctx.applicationAttributes.attrValue(DAG.STRAM_APP_PATH, "app-dfs-path-not-configured");
    this.checkpointFsPath = this.appPath + "/" + DAG.SUBDIR_CHECKPOINTS;
    this.tupleRecordingPartFileSize = ctx.applicationAttributes.attrValue(DAG.STRAM_TUPLE_RECORDING_PART_FILE_SIZE, 100 * 1024);
    this.tupleRecordingPartFileTimeMillis = ctx.applicationAttributes.attrValue(DAG.STRAM_TUPLE_RECORDING_PART_FILE_TIME_MILLIS, 30 * 60 * 60 * 1000);
    this.daemonAddress = ctx.applicationAttributes.attrValue(DAG.STRAM_DAEMON_ADDRESS, null);

    try {
      if (ctx.deployBufferServer) {
        if (!eventloop.isActive()) { /* this check is necessary since StramLocalCluster can have multiple children in the same cluster */
          logger.debug("starting event loop {}", eventloop);
          eventloop.start();
        }
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
   * process through the callback address provided on the command line. Deploys
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

    // Communicate with parent as actual task owner.
    UserGroupInformation taskOwner =
            UserGroupInformation.createRemoteUser(StramChild.class.getName());
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
      childUGI = UserGroupInformation.createRemoteUser(System.getenv(ApplicationConstants.Environment.USER.toString()));
      // Add tokens to new user so that it may execute its task correctly.
      for (Token<?> token: UserGroupInformation.getCurrentUser().getTokens()) {
        childUGI.addToken(token);
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
            stramChild.monitorHeartbeat();
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

    for (Stream<?> stream: activeStreams.keySet()) {
      stream.deactivate();
    }
    activeStreams.clear();
  }

  private void disconnectNode(int nodeid)
  {
    Node<?> node = nodes.get(nodeid);
    disconnectWindowGenerator(nodeid, node);

    Set<String> removableStreams = new HashSet<String>(); // temporary fix - find out why List does not work.
    // with the logic i have in here, the list should not contain repeated streams. but it does and that causes problem.
    for (Entry<String, ComponentContextPair<Stream<Object>, StreamContext>> entry: streams.entrySet()) {
      String indexingKey = entry.getKey();
      Stream<?> stream = entry.getValue().component;
      StreamContext context = entry.getValue().context;
      String sourceIdentifier = context.getSourceId();
      String sinkIdentifier = context.getSinkId();
      logger.debug("considering stream {} against id {}", stream, indexingKey);
      if (nodeid == Integer.parseInt(sourceIdentifier.split(NODE_PORT_SPLIT_SEPARATOR)[0])) {
        /*
         * the stream originates at the output port of one of the operators that are going to vanish.
         */
        if (activeStreams.containsKey(stream)) {
          logger.debug("deactivating {}", stream);
          stream.deactivate();
          activeStreams.remove(stream);
        }
        removableStreams.add(sourceIdentifier);

        String[] sinkIds = sinkIdentifier.split(", ");
        for (String sinkId: sinkIds) {
          if (!sinkId.startsWith("tcp://")) {
            String[] nodeport = sinkId.split(NODE_PORT_SPLIT_SEPARATOR);
            Node<?> n = nodes.get(Integer.parseInt(nodeport[0]));
            if (n instanceof UnifierNode) {
              n.connectInputPort(nodeport[1] + "(" + sourceIdentifier + ")", null, null);
            }
            else if (n != null) {
              // check why null pointer exception gets thrown here during shutdown! - chetan
              n.connectInputPort(nodeport[1], null, null);
            }
          }
          else if (stream.isMultiSinkCapable()) {
            ComponentContextPair<Stream<Object>, StreamContext> spair = streams.get(sinkId);
            logger.debug("found stream {} against {}", spair == null ? null : spair.component, sinkId);
            if (spair == null) {
              assert (!sinkId.startsWith("tcp://"));
            }
            else {
              assert (sinkId.startsWith("tcp://"));
              if (activeStreams.containsKey(spair.component)) {
                logger.debug("deactivating {} for sink {}", spair.component, sinkId);
                spair.component.deactivate();
                activeStreams.remove(spair.component);
              }

              removableStreams.add(sinkId);
            }
          }
        }
      }
      else {
        /**
         * the stream may or may not feed into one of the operators which are being undeployed.
         */
        String[] sinkIds = sinkIdentifier.split(", ");
        for (int i = sinkIds.length; i-- > 0;) {
          String[] nodeport = sinkIds[i].split(NODE_PORT_SPLIT_SEPARATOR);
          if (Integer.toString(nodeid).equals(nodeport[0])) {
            stream.setSink(sinkIds[i], null);
            if (node instanceof UnifierNode) {
              node.connectInputPort(nodeport[1] + "(" + sourceIdentifier + ")", null, null);
            }
            else {
              node.connectInputPort(nodeport[1], null, null);
            }
            sinkIds[i] = null;
          }
        }

        String sinkId = null;
        for (int i = sinkIds.length; i-- > 0;) {
          if (sinkIds[i] != null) {
            if (sinkId == null) {
              sinkId = sinkIds[i];
            }
            else {
              sinkId = sinkId.concat(", ").concat(sinkIds[i]);
            }
          }
        }

        if (sinkId == null) {
          if (activeStreams.containsKey(stream)) {
            logger.debug("deactivating {}", stream);
            stream.deactivate();
            activeStreams.remove(stream);
          }

          removableStreams.add(indexingKey);
        }
        else {
          // may be we should also check if the count has changed from something to 1
          // and replace mux with 1:1 sink. it's not necessary though.
          context.setSinkId(sinkId);
        }
      }
    }

    for (String streamId: removableStreams) {
      logger.debug("removing stream {}", streamId);
      // need to check why control comes here twice to remove the stream which was deleted before.
      // is it because of multiSinkCapableStream ?
      ComponentContextPair<Stream<Object>, StreamContext> pair = streams.remove(streamId);
      pair.component.teardown();
    }
  }

  private void disconnectWindowGenerator(int nodeid, Node<?> node)
  {
    WindowGenerator chosen1 = generators.remove(nodeid);
    if (chosen1 != null) {
      chosen1.setSink(Integer.toString(nodeid).concat(NODE_PORT_CONCAT_SEPARATOR).concat(Node.INPUT), null);
      node.connectInputPort(Node.INPUT, null, null);

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

    // track all the ids to undeploy
    // track the ones which are active

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
        t.join();
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
      logger.debug("stopping event loop {}", eventloop);
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

  protected void monitorHeartbeat() throws IOException
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
      if (this.bufferServerAddress != null) {
        msg.bufferServerHost = this.bufferServerAddress.getHostName();
        msg.bufferServerPort = this.bufferServerAddress.getPort();
      }
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

        // propagate the backup window, if any
        Long backupWindowId = backupInfo.get(e.getKey());
        if (backupWindowId != null) {
          hb.setLastBackupWindowId(backupWindowId);
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
        }
        for (String portName: portMappingDescriptor.outputPorts.keySet()) {
          tupleRecorder = tupleRecorders.get(this.getRecorderKey(e.getKey(), portName));
          if (tupleRecorder != null) {
            hb.addRecordingName(tupleRecorder.getRecordingName());
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

    if (rsp.nodeRequests != null) {
      // processing of per operator requests
      for (StramToNodeRequest req: rsp.nodeRequests) {
        OperatorContext nc = activeNodes.get(req.getOperatorId());
        if (nc == null) {
          logger.warn("Received request with invalid operator id {} ({})", req.getOperatorId(), req);
        }
        else {
          logger.debug("Stram request: {}", req);
          processStramRequest(nc, req);
        }
      }
    }
  }

  abstract private class AbstractNodeRequest implements OperatorContext.NodeRequest
  {
    final Node<?> node;
    final StramToNodeRequest snr;

    AbstractNodeRequest(OperatorContext context, final StramToNodeRequest snr)
    {
      this.node = nodes.get(context.getId());
      this.snr = snr;
    }

  };

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

      case CHECKPOINT:
        // avoid filling queue with checkpoint requests that would write same state multiple times
        OperatorContext.NodeRequest nr = context.getRequests().peek();
        if (nr != null && nr instanceof AbstractNodeRequest) {
          AbstractNodeRequest aor = (AbstractNodeRequest)nr;
          if (aor.snr.getRequestType() == StramToNodeRequest.RequestType.CHECKPOINT) {
            aor.snr.setRecoveryCheckpoint(snr.getRecoveryCheckpoint());
            logger.debug("Duplicates queued request, skipping {}", snr);
            return;
          }
        }
        context.request(new AbstractNodeRequest(context, snr)
        {
          @Override
          public void execute(Operator operator, int id, long windowId) throws IOException
          {
            new HdfsBackupAgent(StramChild.this.conf, StramChild.this.checkpointFsPath).backup(id, windowId, operator, StramUtils.getNodeSerDe(null));
            // record last backup window id for heartbeat
            StramChild.this.backupInfo.put(id, windowId);

            node.emitCheckpoint(windowId);

            if (operator instanceof CheckpointListener) {
              ((CheckpointListener)operator).checkpointed(windowId);
              ((CheckpointListener)operator).committed(snr.getRecoveryCheckpoint());
            }
          }

        });
        break;

      case START_RECORDING: {
        final String portName = snr.getPortName();
        final String operatorPortName = getRecorderKey(operatorId, portName);

        logger.debug("Received start recording request for {}", operatorPortName);
        context.request(new OperatorContext.NodeRequest()
        {
          @Override
          public void execute(Operator operator, int operatorId, long windowId) throws IOException
          {
            PortMappingDescriptor descriptor = node.getPortMappingDescriptor();
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
              tupleRecorder.setBasePath(basePath);
              tupleRecorder.setBytesPerPartFile(StramChild.this.tupleRecordingPartFileSize);
              tupleRecorder.setMillisPerPartFile(StramChild.this.tupleRecordingPartFileTimeMillis);
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
                if (streamId != null && (portName == null || entry.getKey().equals(portName))) {
                  logger.info("Adding recorder sink to input port {}, stream {}", entry.getKey(), streamId);
                  tupleRecorder.addInputPortInfo(entry.getKey(), streamId);
                  sinkMap.put(entry.getKey(), tupleRecorder.newSink(entry.getKey()));
                }
              }
              for (Map.Entry<String, OutputPort<?>> entry: descriptor.outputPorts.entrySet()) {
                String streamId = getDeclaredStreamId(operatorId, entry.getKey());
                if (streamId != null && (portName == null || entry.getKey().equals(portName))) {
                  logger.info("Adding recorder sink to output port {}, stream {}", entry.getKey(), streamId);
                  tupleRecorder.addOutputPortInfo(entry.getKey(), streamId);
                  sinkMap.put(entry.getKey(), tupleRecorder.newSink(entry.getKey()));
                }
              }
              if (!sinkMap.isEmpty()) {
                logger.debug("Started recording to base path {}", basePath);
                node.addSinks(sinkMap);
                tupleRecorder.setup(null);
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

        });
      }
      break;

      case STOP_RECORDING: {
        final String portName = snr.getPortName();
        final String operatorPortName = getRecorderKey(operatorId, portName);
        logger.debug("Received stop recording request for {}" + operatorPortName);

        context.request(new OperatorContext.NodeRequest()
        {
          @Override
          public void execute(Operator operator, int operatorId, long windowId) throws IOException
          {
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

        });
      }
      break;

      case SYNC_RECORDING: {
        final String portName = snr.getPortName();
        final String operatorPortName = getRecorderKey(operatorId, portName);
        logger.debug("Received sync recording request for " + operatorId);

        context.request(new OperatorContext.NodeRequest()
        {
          @Override
          public void execute(Operator operator, int operatorId, long windowId) throws IOException
          {
            if (tupleRecorders.containsKey(operatorPortName)) {
              logger.debug("Executing sync recording request for {}" + operatorPortName);

              TupleRecorder tupleRecorder = tupleRecorders.get(operatorPortName);
              if (tupleRecorder != null) {
                tupleRecorder.requestSync();
                logger.debug("Requested sync recording for operator/port {}" + operatorPortName);
              }
            }
            else {
              logger.error("(SYNC_RECORDING) Operator/port " + operatorPortName + " is not being recorded.");
            }
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
    deployOutputStreams(nodeList, groupedInputStreams);

    deployInputStreams(nodeList);

    activate(nodeList);
  }

  private void massageUnifierDeployInfo(OperatorDeployInfo odi)
  {
    for (OperatorDeployInfo.InputDeployInfo idi: odi.inputs) {
      idi.portName += "(" + idi.sourceNodeId + NODE_PORT_CONCAT_SEPARATOR + idi.sourcePortName + ")";
    }
  }

  @SuppressWarnings("unchecked")
  private void deployNodes(List<OperatorDeployInfo> nodeList) throws Exception
  {
    OperatorCodec operatorSerDe = StramUtils.getNodeSerDe(null);
    BackupAgent backupAgent = new HdfsBackupAgent(this.conf, this.checkpointFsPath);
    for (OperatorDeployInfo ndi: nodeList) {
      try {
        final Object foreignObject;
        if (ndi.checkpointWindowId > 0) {
          logger.debug("Restoring node {} to checkpoint {}", ndi.id, Codec.getStringWindowId(ndi.checkpointWindowId));
          foreignObject = backupAgent.restore(ndi.id, ndi.checkpointWindowId, operatorSerDe);
        }
        else {
          foreignObject = operatorSerDe.read(new ByteArrayInputStream(ndi.serializedNode));
        }

        String nodeid = Integer.toString(ndi.id).concat("/").concat(ndi.declaredId).concat(":").concat(foreignObject.getClass().getSimpleName());
        if (foreignObject instanceof InputOperator && ndi.type == OperatorDeployInfo.OperatorType.INPUT) {
          nodes.put(ndi.id, new InputNode(nodeid, (InputOperator)foreignObject));
        }
        else if (foreignObject instanceof Unifier && ndi.type == OperatorDeployInfo.OperatorType.UNIFIER) {
          nodes.put(ndi.id, new UnifierNode(nodeid, (Unifier<Object>)foreignObject));
          massageUnifierDeployInfo(ndi);
        }
        else {
          nodes.put(ndi.id, new GenericNode(nodeid, (Operator)foreignObject));
        }
      }
      catch (Exception e) {
        logger.error(e.getLocalizedMessage());
        throw e;
      }
    }
  }

  private void deployOutputStreams(List<OperatorDeployInfo> nodeList, HashMap<String, ArrayList<String>> groupedInputStreams) throws Exception
  {
    /*
     * We proceed to deploy all the output streams. At the end of this block, our streams collection
     * will contain all the streams which originate at the output port of the operators. The streams
     * are generally mapped against the "nodename.portname" string. But the BufferOutputStreams which
     * share the output port with other inline streams are mapped against the Buffer Server port to
     * avoid collision and at the same time keep track of these buffer streams.
     */
    for (OperatorDeployInfo ndi: nodeList) {
      Node<?> node = nodes.get(ndi.id);

      for (OperatorDeployInfo.OutputDeployInfo nodi: ndi.outputs) {
        String sourceIdentifier = Integer.toString(ndi.id).concat(NODE_PORT_CONCAT_SEPARATOR).concat(nodi.portName);
        String sinkIdentifier;

        StreamContext context = new StreamContext(nodi.declaredStreamId);
        Stream<Object> stream;

        ArrayList<String> collection = groupedInputStreams.get(sourceIdentifier);
        if (collection == null) {
          /*
           * Let's create a stream to carry the data to the Buffer Server.
           * Nobody in this container is interested in the output placed on this stream, but
           * this stream exists. That means someone outside of this container must be interested.
           */
          assert (nodi.isInline() == false): "output should not be inline: " + nodi;
          context.setBufferServerAddress(InetSocketAddress.createUnresolved(nodi.bufferServerHost, nodi.bufferServerPort));
          context.attr(StreamContext.EVENT_LOOP).set(eventloop);
          context.attr(StreamContext.CODEC).set(StramUtils.getSerdeInstance(nodi.serDeClassName));
          if (NetUtils.isLocalAddress(context.getBufferServerAddress().getAddress())) {
            context.setBufferServerAddress(new InetSocketAddress(InetAddress.getByName(null), nodi.bufferServerPort));
          }

          stream = new BufferServerPublisher(sourceIdentifier);
          stream.setup(context);
          logger.debug("deployed a buffer stream {}", stream);

          sinkIdentifier = "tcp://".concat(nodi.bufferServerHost).concat(":").concat(String.valueOf(nodi.bufferServerPort)).concat("/").concat(sourceIdentifier);
        }
        else if (collection.size() == 1) {
          if (nodi.isInline()) {
            /**
             * Let's create an inline stream to carry data from output port to input port of some other node.
             * There is only one node interested in output placed on this stream, and that node is in this container.
             */
            stream = new InlineStream();
            stream.setup(context);

            sinkIdentifier = null;
          }
          else {
            /**
             * Let's create 2 streams: 1 inline and 1 going to the Buffer Server.
             * Although there is a node in this container interested in output placed on this stream, there
             * seems to at least one more party interested but placed in a container other than this one.
             */
            sinkIdentifier = "tcp://".concat(nodi.bufferServerHost).concat(":").concat(String.valueOf(nodi.bufferServerPort)).concat("/").concat(sourceIdentifier);

            StreamContext bssc = new StreamContext(nodi.declaredStreamId);
            bssc.setSourceId(sourceIdentifier);
            bssc.setSinkId(sinkIdentifier);
            bssc.setStartingWindowId(ndi.checkpointWindowId > 0 ? ndi.checkpointWindowId + 1 : 0); // TODO: next window after checkpoint
            bssc.setBufferServerAddress(InetSocketAddress.createUnresolved(nodi.bufferServerHost, nodi.bufferServerPort));
            bssc.attr(StreamContext.CODEC).set(StramUtils.getSerdeInstance(nodi.serDeClassName));
            bssc.attr(StreamContext.EVENT_LOOP).set(eventloop);
            if (NetUtils.isLocalAddress(bssc.getBufferServerAddress().getAddress())) {
              bssc.setBufferServerAddress(new InetSocketAddress(InetAddress.getByName(null), nodi.bufferServerPort));
            }

            BufferServerPublisher bsos = new BufferServerPublisher(sourceIdentifier);
            bsos.setup(bssc);
            logger.debug("deployed a buffer stream {}", bsos);

            streams.put(sinkIdentifier, new ComponentContextPair<Stream<Object>, StreamContext>(bsos, bssc));

            // should we create inline stream here or wait for the input deployments to create the inline streams?
            stream = new MuxStream();
            stream.setup(context);
            stream.setSink(sinkIdentifier, bsos);

            logger.debug("stored stream {} against key {}", bsos, sinkIdentifier);
          }
        }
        else {
          /**
           * Since there are multiple parties interested in this node itself, we are going to come
           * to this block multiple times. The actions we take subsequent times are going to be different
           * than the first time. We create the MuxStream only the first time.
           */
          ComponentContextPair<Stream<Object>, StreamContext> pair = streams.get(sourceIdentifier);
          if (pair == null) {
            /**
             * Let's multiplex the output placed on this stream.
             * This container itself contains more than one parties interested.
             */
            stream = new MuxStream();
            stream.setup(context);
          }
          else {
            stream = pair.component;
          }

          if (nodi.isInline()) {
            sinkIdentifier = null;
          }
          else {
            sinkIdentifier = "tcp://".concat(nodi.bufferServerHost).concat(":").concat(String.valueOf(nodi.bufferServerPort)).concat("/").concat(sourceIdentifier);

            StreamContext bssc = new StreamContext(nodi.declaredStreamId);
            bssc.attr(StreamContext.CODEC).set(StramUtils.getSerdeInstance(nodi.serDeClassName));
            bssc.attr(StreamContext.EVENT_LOOP).set(eventloop);
            bssc.setBufferServerAddress(InetSocketAddress.createUnresolved(nodi.bufferServerHost, nodi.bufferServerPort));
            bssc.setSourceId(sourceIdentifier);
            bssc.setSinkId(sinkIdentifier);
            bssc.setStartingWindowId(ndi.checkpointWindowId > 0 ? ndi.checkpointWindowId + 1 : 0); // TODO: next window after checkpoint
            BufferServerPublisher bsos = new BufferServerPublisher(sourceIdentifier);
            bsos.setup(bssc);
            logger.debug("deployed a buffer stream {}", bsos);

            streams.put(sinkIdentifier, new ComponentContextPair<Stream<Object>, StreamContext>(bsos, bssc));
            logger.debug("stored stream {} against key {}", bsos, sinkIdentifier);

            stream.setup(context);
            stream.setSink(sinkIdentifier, bsos);
          }
        }

        if (!streams.containsKey(sourceIdentifier)) {
          node.connectOutputPort(nodi.portName, nodi.contextAttributes, stream);

          context.setSourceId(sourceIdentifier);
          context.setSinkId(sinkIdentifier);
          context.setStartingWindowId(ndi.checkpointWindowId > 0 ? ndi.checkpointWindowId + 1 : 0); // TODO: next window after checkpoint

          streams.put(sourceIdentifier, new ComponentContextPair<Stream<Object>, StreamContext>(stream, context));
          logger.debug("stored stream {} against key {}", stream, sourceIdentifier);
        }
      }
    }
  }

  /**
   * If the port is connected, find return the declared stream Id.
   *
   * @param operatorId id of the operator to which the port belongs.
   * @param portname name of port to which the stream is connected.
   * @return Stream Id if connected, null otherwise.
   */
  public final String getDeclaredStreamId(int operatorId, String portname)
  {
    String identifier = String.valueOf(operatorId).concat(NODE_PORT_CONCAT_SEPARATOR).concat(portname);
    ComponentContextPair<Stream<Object>, StreamContext> spair = streams.get(identifier);
    if (spair == null) {
      return null;
    }

    return spair.context.getId();
  }

  private void deployInputStreams(List<OperatorDeployInfo> nodeList) throws Exception
  {
    // collect any input operators along with their smallest window id,
    // those are subsequently used to setup the window generator
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
    for (OperatorDeployInfo ndi: nodeList) {
      if (ndi.inputs == null || ndi.inputs.isEmpty()) {
        /**
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

          ComponentContextPair<Stream<Object>, StreamContext> pair = streams.get(sourceIdentifier);
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
            context.setStartingWindowId(ndi.checkpointWindowId > 0 ? ndi.checkpointWindowId + 1 : 0); // TODO: next window after checkpoint

            @SuppressWarnings("unchecked")
            Stream<Object> stream = (Stream)new BufferServerSubscriber(nidi.declaredStreamId);
            stream.setup(context);
            logger.debug("deployed buffer input stream {}", stream);

            Sink<Object> s = node.connectInputPort(nidi.portName, nidi.contextAttributes, stream);
            stream.setSink(sinkIdentifier,
                           ndi.checkpointWindowId > 0 ? new WindowIdActivatedSink<Object>(stream, sinkIdentifier, s, ndi.checkpointWindowId) : s);

            streams.put(sinkIdentifier,
                        new ComponentContextPair<Stream<Object>, StreamContext>(stream, context));
            logger.debug("put input stream {} against key {}", stream, sinkIdentifier);
          }
          else {
            String streamSinkId = pair.context.getSinkId();

            Sink<Object> s;
            if (streamSinkId == null) {
              s = node.connectInputPort(nidi.portName, nidi.contextAttributes, pair.component);
              pair.context.setSinkId(sinkIdentifier);
            }
            else if (pair.component.isMultiSinkCapable()) {
              s = node.connectInputPort(nidi.portName, nidi.contextAttributes, pair.component);
              pair.context.setSinkId(streamSinkId.concat(", ").concat(sinkIdentifier));
            }
            else {
              /**
               * we are trying to tap into existing InlineStream or BufferServerPublisher.
               * Since none of those streams are MultiSinkCapable, we need to replace them with Mux.
               */
              StreamContext context = new StreamContext(nidi.declaredStreamId);
              context.setSourceId(sourceIdentifier);
              context.setSinkId(streamSinkId.concat(", ").concat(sinkIdentifier));
              context.setStartingWindowId(ndi.checkpointWindowId > 0 ? ndi.checkpointWindowId + 1 : 0); // TODO: next window after checkpoint

              Stream<Object> stream = new MuxStream();
              stream.setup(context);
              logger.debug("deployed input mux stream {}", stream);
              s = node.connectInputPort(nidi.portName, nidi.contextAttributes, stream);
              streams.put(sourceIdentifier, new ComponentContextPair<Stream<Object>, StreamContext>(stream, context));
              logger.debug("stored input stream {} against key {}", stream, sourceIdentifier);

              /**
               * Lets wire the MuxStream to upstream node.
               */
              String[] nodeport = sourceIdentifier.split(NODE_PORT_SPLIT_SEPARATOR);
              Node<?> upstreamNode = nodes.get(Integer.parseInt(nodeport[0]));
              upstreamNode.connectOutputPort(nodeport[1], nidi.contextAttributes, stream);

              Sink<Object> existingSink;
              if (pair.component instanceof InlineStream) {
                String[] np = streamSinkId.split(NODE_PORT_SPLIT_SEPARATOR);
                Node<?> anotherNode = nodes.get(Integer.parseInt(np[0]));
                existingSink = anotherNode.connectInputPort(np[1], nidi.contextAttributes, stream); // the context object here is probably wrong

                /*
                 * we do not need to do this but looks bad if leave it in limbo.
                 */
                pair.component.deactivate();
                pair.component.teardown();
              }
              else {
                existingSink = pair.component;

                /*
                 * we got this stream since it was mapped against sourceId, but since
                 * we took that place for MuxStream, we give this a new place of its own.
                 */
                streams.put(pair.context.getSinkId(), pair);
                logger.debug("relocated stream {} against key {}", pair.context.getSinkId());
              }

              stream.setSink(streamSinkId, existingSink);
            }

            if (nidi.partitionKeys == null || nidi.partitionKeys.isEmpty()) {
              logger.debug("got simple inline stream from {} to {} - {}", new Object[] {sourceIdentifier, sinkIdentifier, nidi});
              pair.component.setSink(sinkIdentifier,
                                     ndi.checkpointWindowId > 0 ? new WindowIdActivatedSink<Object>(pair.component, sinkIdentifier, s, ndi.checkpointWindowId) : s);
            }
            else {
              /*
               * generally speaking we do not have partitions on the inline streams so the control should not
               * come here but if it comes, then we are ready to handle it using the partition aware streams.
               */
              logger.debug("got partitions on the inline stream from {} to {} - {}", new Object[] {sourceIdentifier, sinkIdentifier, nidi});
              PartitionAwareSink<Object> pas = new PartitionAwareSink<Object>(StramUtils.getSerdeInstance(nidi.serDeClassName), nidi.partitionKeys, nidi.partitionMask, s);
              pair.component.setSink(sinkIdentifier,
                                     ndi.checkpointWindowId > 0 ? new WindowIdActivatedSink<Object>(pair.component, sinkIdentifier, pas, ndi.checkpointWindowId) : pas);
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
        Sink<Object> s = node.connectInputPort(Node.INPUT, null, windowGenerator);
        windowGenerator.setSink(Integer.toString(ndi.id).concat(NODE_PORT_CONCAT_SEPARATOR).concat(Node.INPUT),
                                ndi.checkpointWindowId > 0 ? new WindowIdActivatedSink<Object>(windowGenerator, Integer.toString(ndi.id).concat(NODE_PORT_CONCAT_SEPARATOR).concat(Node.INPUT), s, ndi.checkpointWindowId) : s);
      }
    }
  }

  /**
   * Create the window generator for the given start window id.
   * This is a hook for tests to control the window generation.
   *
   * @param smallestWindowId
   * @return WindowGenerator
   */
  protected WindowGenerator setupWindowGenerator(long smallestWindowId)
  {
    WindowGenerator windowGenerator = new WindowGenerator(new ScheduledThreadPoolExecutor(1, "WindowGenerator"));
    /**
     * let's make sure that we send the same window Ids with the same reset windows.
     */
    windowGenerator.setResetWindow(firstWindowMillis);

    long millisAtFirstWindow = (smallestWindowId >> 32) * 1000 + windowWidthMillis * (smallestWindowId & WindowGenerator.MAX_WINDOW_ID);
    windowGenerator.setFirstWindow(millisAtFirstWindow > firstWindowMillis ? millisAtFirstWindow : firstWindowMillis);

    windowGenerator.setWindowWidth(windowWidthMillis);
    return windowGenerator;
  }

  @SuppressWarnings({"SleepWhileInLoop", "SleepWhileHoldingLock"})
  public synchronized void activate(List<OperatorDeployInfo> nodeList)
  {
    for (ComponentContextPair<Stream<Object>, StreamContext> pair: streams.values()) {
      if (!(pair.component instanceof BufferServerSubscriber || activeStreams.containsKey(pair.component))) {
        activeStreams.put(pair.component, pair.context);
        pair.component.activate(pair.context);
      }
    }

    final AtomicInteger activatedNodeCount = new AtomicInteger(activeNodes.size());
    for (final OperatorDeployInfo ndi: nodeList) {
      final Node<?> node = nodes.get(ndi.id);
      assert (!activeNodes.containsKey(ndi.id));
      new Thread(node.id)
      {
        @Override
        public void run()
        {
          try {
            OperatorContext context = new OperatorContext(new Integer(ndi.id), this, ndi.contextAttributes, applicationAttributes);
            node.getOperator().setup(context);
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

    for (ComponentContextPair<Stream<Object>, StreamContext> pair: streams.values()) {
      if (pair.component instanceof BufferServerSubscriber && !activeStreams.containsKey(pair.component)) {
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

}
