/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.dag.BackupAgent;
import com.malhartech.dag.Component;
import com.malhartech.dag.ComponentContextPair;
import com.malhartech.dag.Node;
import com.malhartech.dag.NodeSerDe;
import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.NodeContext;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Stream;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeat;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingNodeHeartbeat;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingNodeHeartbeat.DNodeState;
import com.malhartech.stream.BufferServerInputStream;
import com.malhartech.stream.BufferServerOutputStream;
import com.malhartech.stream.BufferServerStreamContext;
import com.malhartech.stream.InlineStream;
import com.malhartech.stream.MuxStream;
import com.malhartech.stream.PartitionAwareSink;
import com.malhartech.stream.SocketInputStream;
import com.malhartech.util.ScheduledThreadPoolExecutor;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.log4j.LogManager;
import org.fusesource.hawtbuf.ByteArrayInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * The main() for streaming node processes launched by {@link com.malhartech.stram.StramAppMaster}.<p>
 * <br>
 *
 */
public class StramChild
{
  private static Logger LOG = LoggerFactory.getLogger(StramChild.class);
  final private String containerId;
  final private Configuration conf;
  final private StreamingNodeUmbilicalProtocol umbilical;
  final private Map<String, ComponentContextPair<Node, NodeContext>> nodes = new ConcurrentHashMap<String, ComponentContextPair<Node, NodeContext>>();
  final private Set<ComponentContextPair<Node, NodeContext>> activeNodes = new HashSet<ComponentContextPair<Node, NodeContext>>();
  final private Map<String, ComponentContextPair<Stream, StreamContext>> streams = new ConcurrentHashMap<String, ComponentContextPair<Stream, StreamContext>>();
  final private Set<ComponentContextPair<Stream, StreamContext>> activeStreams = new HashSet<ComponentContextPair<Stream, StreamContext>>();
  private long heartbeatIntervalMillis = 1000;
  private boolean exitHeartbeatLoop = false;
  private final Object heartbeatTrigger = new Object();
  private WindowGenerator windowGenerator;
  private String checkpointDfsPath;
  private Configuration dagConfig = new Configuration(); // STRAM should provide this object, we are mimicking here.
  /**
   * Map of last backup window id that is used to communicate checkpoint state back to Stram. TODO: Consider adding this to the node context instead.
   */
  private Map<String, Long> backupInfo = new ConcurrentHashMap<String, Long>();

  protected StramChild(String containerId, Configuration conf, StreamingNodeUmbilicalProtocol umbilical)
  {
    this.umbilical = umbilical;
    this.containerId = containerId;
    this.conf = conf;
  }

  public String getContainerId()
  {
    return this.containerId;
  }

  protected Map<String, ComponentContextPair<Node, NodeContext>> getNodes()
  {
    return this.nodes;
  }

  protected void setWindowGenerator(WindowGenerator wgen)
  {
    this.windowGenerator = wgen;
  }

  /**
   * Initialize container with nodes and streams in the context.
   * Existing nodes are not affected by this operation.
   *
   * @param ctx
   * @throws IOException
   */
  protected void init(StreamingContainerContext ctx) throws IOException
  {

    this.heartbeatIntervalMillis = ctx.getHeartbeatIntervalMillis();
    if (this.heartbeatIntervalMillis == 0) {
      this.heartbeatIntervalMillis = 1000;
    }

    if ((this.checkpointDfsPath = ctx.getCheckpointDfsPath()) == null) {
      this.checkpointDfsPath = "checkpoint-dfs-path-not-configured";
    }

    dagConfig.setLong(WindowGenerator.FIRST_WINDOW_MILLIS, ctx.getStartWindowMillis());
    dagConfig.setInt(WindowGenerator.WINDOW_WIDTH_MILLIS, ctx.getWindowSizeMillis());

    deployNodes(ctx.nodeList);
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
    LOG.debug("Entering hearbeat loop (interval is {} ms)", this.heartbeatIntervalMillis);
    while (!exitHeartbeatLoop) {

      synchronized (this.heartbeatTrigger) {
        try {
          this.heartbeatTrigger.wait(heartbeatIntervalMillis);
        }
        catch (InterruptedException e1) {
          LOG.warn("Interrupted in heartbeat loop, exiting..");
          break;
        }
      }

      long currentTime = System.currentTimeMillis();
      ContainerHeartbeat msg = new ContainerHeartbeat();
      msg.setContainerId(this.containerId);
      List<StreamingNodeHeartbeat> heartbeats = new ArrayList<StreamingNodeHeartbeat>(nodes.size());

      // gather heartbeat info for all nodes
      for (Map.Entry<String, ComponentContextPair<Node, NodeContext>> e: nodes.entrySet()) {
        StreamingNodeHeartbeat hb = new StreamingNodeHeartbeat();
        hb.setNodeId(e.getKey());
        hb.setGeneratedTms(currentTime);
        hb.setIntervalMs(heartbeatIntervalMillis);
        e.getValue().context.drainHeartbeatCounters(hb.getHeartbeatsContainer());
        DNodeState state = DNodeState.PROCESSING;
        synchronized (activeNodes) {
          if (!activeNodes.contains(e.getValue())) {
            state = DNodeState.IDLE;
          }
        }
        hb.setState(state.name());
        // propagate the backup window, if any
        Long backupWindowId = backupInfo.get(e.getKey());
        if (backupWindowId != null) {
          hb.setLastBackupWindowId(backupWindowId);
        }
        heartbeats.add(hb);
      }
      msg.setDnodeEntries(heartbeats);

      // heartbeat call and follow-up processing
      LOG.debug("Sending heartbeat for {} nodes.", msg.getDnodeEntries().size());
      try {
        ContainerHeartbeatResponse rsp = umbilical.processHeartbeat(msg);
        if (rsp != null) {
          processHeartbeatResponse(rsp);
          // keep polling at smaller interval if work is pending
          while (rsp != null && rsp.hasPendingRequests) {
            LOG.info("Waiting for pending request.");
            synchronized (this.heartbeatTrigger) {
              try {
                this.heartbeatTrigger.wait(500);
              }
              catch (InterruptedException e1) {
                LOG.warn("Interrupted in heartbeat loop, exiting..");
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
        LOG.warn("Exception received (may be during shutdown?) {}", e.getLocalizedMessage(), e);
      }
    }
    LOG.debug("Exiting hearbeat loop");
    umbilical.log(containerId, "[" + containerId + "] Exiting heartbeat loop..");
  }

  protected void processHeartbeatResponse(ContainerHeartbeatResponse rsp)
  {
    if (rsp.shutdown) {
      LOG.info("Received shutdown request");
      this.exitHeartbeatLoop = true;
      return;
    }

    if (rsp.undeployRequest != null) {
      LOG.info("Undeploy request: {}", rsp.undeployRequest);
      undeployNodes(rsp.undeployRequest);
    }

    if (rsp.deployRequest != null) {
      LOG.info("Deploy request: {}", rsp.deployRequest);
      deployNodes(rsp.deployRequest);
    }

    if (rsp.nodeRequests != null) {
      // extended processing per node
      for (StramToNodeRequest req : rsp.nodeRequests) {
        ComponentContextPair<Node, NodeContext> pair = nodes.get(req.getNodeId());
        if (pair == null) {
          LOG.warn("Received request with invalid node id {} ({})", req.getNodeId(), req);
        }
        else {
          LOG.debug("Stram request: {}", req);
          processStramRequest(pair, req);
        }
      }
    }
  }

  /**
   * Process request from stram for further communication through the protocol. Extended reporting is on a per node basis (won't occur under regular operation)
   *
   * @param n
   * @param snr
   */
  private void processStramRequest(ComponentContextPair<Node, NodeContext> pair, StramToNodeRequest snr)
  {
    switch (snr.getRequestType()) {
      case REPORT_PARTION_STATS:
        LOG.warn("Ignoring stram request {}", snr);
        break;

      case CHECKPOINT:
        pair.context.requestBackup(new HdfsBackupAgent(StramUtils.getNodeSerDe(null)));
        break;

      default:
        LOG.error("Unknown request from stram {}", snr);
    }
  }

  public static void main(String[] args) throws Throwable
  {
    LOG.debug("Child starting with classpath: {}", System.getProperty("java.class.path"));

    final Configuration defaultConf = new Configuration();
    // TODO: streaming node config
    //defaultConf.addResource(MRJobConfig.JOB_CONF_FILE);
    UserGroupInformation.setConfiguration(defaultConf);

    String host = args[0];
    int port = Integer.parseInt(args[1]);
    final InetSocketAddress address =
            NetUtils.createSocketAddrForHost(host, port);

    final String childId = args[2];
    //Token<JobTokenIdentifier> jt = loadCredentials(defaultConf, address);

    // Communicate with parent as actual task owner.
    UserGroupInformation taskOwner =
            UserGroupInformation.createRemoteUser(StramChild.class.getName());
    //taskOwner.addToken(jt);
    final StreamingNodeUmbilicalProtocol umbilical =
            taskOwner.doAs(new PrivilegedExceptionAction<StreamingNodeUmbilicalProtocol>()
    {
      @Override
      public StreamingNodeUmbilicalProtocol run() throws Exception
      {
        return RPC.getProxy(StreamingNodeUmbilicalProtocol.class,
                            StreamingNodeUmbilicalProtocol.versionID, address, defaultConf);
      }
    });

    LOG.debug("PID: " + System.getenv().get("JVM_PID"));
    UserGroupInformation childUGI;

    try {
      childUGI = UserGroupInformation.createRemoteUser(System.getenv(ApplicationConstants.Environment.USER.toString()));
      // Add tokens to new user so that it may execute its task correctly.
      for (Token<?> token: UserGroupInformation.getCurrentUser().getTokens()) {
        childUGI.addToken(token);
      }

      // TODO: activate node in doAs block
      childUGI.doAs(new PrivilegedExceptionAction<Object>()
      {
        @Override
        public Object run() throws Exception
        {
          StramChild stramChild = new StramChild(childId, defaultConf, umbilical);
          StreamingContainerContext ctx = umbilical.getInitContext(childId);
          LOG.debug("Got context: " + ctx);
          stramChild.init(ctx);
          // main thread enters heartbeat loop
          stramChild.heartbeatLoop();
          // shutdown
          stramChild.shutdown();
          return null;
        }
      });
    }
    catch (FSError e) {
      LOG.error("FSError from child", e);
      umbilical.log(childId, e.getMessage());
    }
    catch (Exception exception) {
      LOG.warn("Exception running child : "
              + StringUtils.stringifyException(exception));
      // Report back any failures, for diagnostic purposes
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      exception.printStackTrace(new PrintStream(baos));
      umbilical.log(childId, "FATAL: " + baos.toString());
    }
    catch (Throwable throwable) {
      LOG.error("Error running child : "
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
  }

  private void deactivate()
  {
    if (windowGenerator != null) {
      windowGenerator.deactivate();
    }

    synchronized (activeNodes) {
      for (ComponentContextPair<Node, NodeContext> pair: activeNodes) {
        pair.component.deactivate();
      }
    }

    for (ComponentContextPair<Stream, StreamContext> pair: activeStreams) {
      pair.component.deactivate();
    }
    activeStreams.clear();
  }

  @SuppressWarnings("SleepWhileInLoop")
  private void deployNodes(List<NodeDeployInfo> nodeList)
  {
    /**
     * A little bit of sanity check would reduce the percentage of deploy failures upfront.
     */
    for (NodeDeployInfo ndi: nodeList) {
      if (nodes.containsKey(ndi.id)) {
        throw new IllegalStateException("Node with id: " + ndi.id + " already present in the container");
      }
    }

    /**
     * changes midflight are always dangerous and will result in disaster if not done correctly.
     * So we temporarily halt the engine. We will need to discuss the midflight changes for good handling.
     */
    deactivate();

    NodeSerDe nodeSerDe = StramUtils.getNodeSerDe(null);
    HashMap<String, ArrayList<String>> groupedInputStreams = new HashMap<String, ArrayList<String>>();
    for (NodeDeployInfo ndi: nodeList) {
      NodeContext nc = new NodeContext(ndi.id);
      try {
        Object foreignObject = nodeSerDe.read(new ByteArrayInputStream(ndi.serializedNode));
        Node node = (Node)foreignObject;
        node.setup(new NodeConfiguration(ndi.properties));
        nodes.put(ndi.id, new ComponentContextPair<Node, NodeContext>(node, nc));

        groupInputStreams(groupedInputStreams, ndi);
      }
      catch (ClassCastException cce) {
        LOG.error(cce.getLocalizedMessage());
      } catch (IOException e) {
        throw new IllegalArgumentException("Failed to read object " + ndi, e);
      }
    }

    // lets create all the output streams
    for (NodeDeployInfo ndi: nodeList) {
      Node node = nodes.get(ndi.id).component;

      for (NodeDeployInfo.NodeOutputDeployInfo nodi: ndi.outputs) {
        String sourceIdentifier = ndi.id.concat(".").concat(nodi.portName);
        String sinkIdentifier;

        Stream stream;

        ArrayList<String> collection = groupedInputStreams.get(sourceIdentifier);
        if (collection == null) {
          /**
           * Let's create a stream to carry the data to the Buffer Server.
           * Nobody in this container is interested in the output placed on this stream, but
           * this stream exists. That means someone outside of this container must be interested.
           */
          assert (nodi.isInline() == false);

          StreamConfiguration config = new StreamConfiguration();
          config.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved(nodi.bufferServerHost, nodi.bufferServerPort));

          stream = new BufferServerOutputStream(StramUtils.getSerdeInstance(nodi.serDeClassName));
          stream.setup(config);

          sinkIdentifier = nodi.bufferServerHost.concat(":").concat(String.valueOf(nodi.bufferServerPort)).concat("/").concat(sourceIdentifier);
        }
        else if (collection.size() == 1) {
          if (nodi.isInline()) {
            /**
             * Let's create an inline stream to carry data from output port to input port of some other node.
             * There is only one node interested in output placed on this stream, and that node is in this container.
             */
            stream = new InlineStream();
            stream.setup(new StreamConfiguration());

            sinkIdentifier = null;
          }
          else {
            /**
             * Let's create 2 streams: 1 inline and 1 going to the Buffer Server.
             * Although there is a node in this container interested in output placed on this stream, there
             * seems to at least one more party interested but place in a container other than this one.
             */
            stream = new MuxStream();
            stream.setup(new StreamConfiguration());

            StreamConfiguration config = new StreamConfiguration();
            config.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved(nodi.bufferServerHost, nodi.bufferServerPort));

            BufferServerOutputStream bsos = new BufferServerOutputStream(StramUtils.getSerdeInstance(nodi.serDeClassName));
            bsos.setup(config);

            sinkIdentifier = nodi.bufferServerHost.concat(":").concat(String.valueOf(nodi.bufferServerPort)).concat("/").concat(sourceIdentifier);

            BufferServerStreamContext bssc = new BufferServerStreamContext(nodi.declaredStreamId);
            bssc.setSourceId(sourceIdentifier);
            bssc.setSinkId(sinkIdentifier);

            Sink s = bsos.connect(Component.INPUT, stream);
            stream.connect(sinkIdentifier, s);

            streams.put(sinkIdentifier, new ComponentContextPair<Stream, StreamContext>(bsos, bssc));
          }
        }
        else {
          /**
           * Since there are multiple parties interested in this node itself, we are going to come
           * to this block multiple times. The actions we take subsequent times are going to be different
           * than the first time.
           */
          ComponentContextPair<Stream, StreamContext> pair = streams.get(sourceIdentifier);
          if (pair == null) {
            /**
             * Let's multiplex the output placed on this stream.
             * This container itself contains more than one parties interested.
             */
            stream = new MuxStream();
            stream.setup(new StreamConfiguration());
          }
          else {
            stream = pair.component;
          }

          if (nodi.isInline()) {
            sinkIdentifier = null;
          }
          else {
            StreamConfiguration config = new StreamConfiguration();
            config.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved(nodi.bufferServerHost, nodi.bufferServerPort));

            BufferServerOutputStream bsos = new BufferServerOutputStream(StramUtils.getSerdeInstance(nodi.serDeClassName));
            bsos.setup(config);

            sinkIdentifier = nodi.bufferServerHost.concat(":").concat(String.valueOf(nodi.bufferServerPort)).concat("/").concat(sourceIdentifier);

            BufferServerStreamContext bssc = new BufferServerStreamContext(nodi.declaredStreamId);
            bssc.setSourceId(sourceIdentifier);
            bssc.setSinkId(sinkIdentifier);

            Sink s = bsos.connect(Component.INPUT, stream);
            stream.connect(sinkIdentifier, s);

            streams.put(sinkIdentifier, new ComponentContextPair<Stream, StreamContext>(bsos, bssc));
          }
        }

        if (!streams.containsKey(sourceIdentifier)) {
          Sink s = stream.connect(Component.INPUT, node);
          node.connect(nodi.portName, s);

          StreamContext context = new StreamContext(nodi.declaredStreamId);
          context.setSourceId(sourceIdentifier);
          context.setSinkId(sinkIdentifier);

          streams.put(sourceIdentifier, new ComponentContextPair<Stream, StreamContext>(stream, context));
        }
      }
    }

    // lets create all the input streams
    for (NodeDeployInfo ndi: nodeList) {
      Node node = nodes.get(ndi.id).component;
      if (ndi.inputs == null || ndi.inputs.isEmpty()) {
        /**
         * This has to be AbstractInputNode, so let's hook the WindowGenerator to it.
         * A node which does not take any input cannot exist in the DAG since it would be completely
         * unaware of the windows. So for that reason, AbstractInputNode allows Component.INPUT port.
         */
        if (windowGenerator == null) {
          windowGenerator = new WindowGenerator(new ScheduledThreadPoolExecutor(1));
          windowGenerator.setup(dagConfig);
        }

        Sink s = node.connect(Component.INPUT, windowGenerator);
        windowGenerator.connect(containerId, s);
      }
      else {
        for (NodeDeployInfo.NodeInputDeployInfo nidi: ndi.inputs) {
          String sourceIdentifier = nidi.sourceNodeId.concat(".").concat(nidi.sourcePortName);
          String sinkIdentifier = ndi.id.concat(".").concat(nidi.portName);

          ComponentContextPair<Stream, StreamContext> pair = streams.get(sourceIdentifier);
          if (pair == null) {
            // it's buffer server stream
            assert (nidi.isInline() == false);
            sourceIdentifier = nidi.bufferServerHost.concat(":").concat(String.valueOf(nidi.bufferServerPort)).concat("/").concat(sourceIdentifier);

            StreamConfiguration config = new StreamConfiguration();
            config.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved(nidi.bufferServerHost, nidi.bufferServerPort));

            Stream stream = new BufferServerInputStream(StramUtils.getSerdeInstance(nidi.serDeClassName));
            stream.setup(config);

            BufferServerStreamContext context = new BufferServerStreamContext(nidi.declaredStreamId);
            context.setPartitions(nidi.partitionKeys);
            context.setSourceId(sourceIdentifier);
            context.setSinkId(sinkIdentifier);

            pair = new ComponentContextPair<Stream, StreamContext>(stream, context);
            streams.put(sinkIdentifier, pair);

            Sink s = node.connect(nidi.portName, stream);
            stream.connect(sinkIdentifier, s);
          }
          else {
            Sink s = node.connect(nidi.portName, pair.component);
            String streamSinkId = pair.context.getSinkId();

            if (streamSinkId == null) {
              pair.context.setSinkId(sinkIdentifier);
            }
            else {
              pair.context.setSinkId(streamSinkId.concat(", ").concat(sinkIdentifier));
            }

            if (nidi.partitionKeys == null || nidi.partitionKeys.isEmpty()) {
              pair.component.connect(sinkIdentifier, s);
            }
            else {
              /*
               * generally speaking we do not have partitions on the inline streams so the control should not
               * come here but if it comes, then we are ready to handle it using the partition aware streams.
               */
              PartitionAwareSink pas = new PartitionAwareSink(StramUtils.getSerdeInstance(nidi.serDeClassName), nidi.partitionKeys, s);
              pair.component.connect(sinkIdentifier, pas);
            }
          }
        }
      }
    }

    for (ComponentContextPair<Stream, StreamContext> pair: streams.values()) {
      if (!(pair.component instanceof SocketInputStream)) {
        pair.component.activate(pair.context);
        activeStreams.add(pair);
      }
    }

    for (final ComponentContextPair<Node, NodeContext> pair: nodes.values()) {
      Thread t = new Thread("node-" + pair.context.getId())
      {
        @Override
        public void run()
        {
          synchronized (activeNodes) {
            activeNodes.add(pair);
          }
          pair.component.activate(pair.context);
          synchronized (activeNodes) {
            activeNodes.remove(pair);
          }
        }
      };
      t.start();
    }

    /**
     * we need to make sure that before any of the nodes gets the first message, it's activated.
     */
    try {
      int size;
      do {
        Thread.sleep(20);
        synchronized (activeNodes) {
          size = activeNodes.size();
        }
      }
      while (size < nodes.size());
    }
    catch (InterruptedException ex) {
      LOG.debug(ex.getLocalizedMessage());
    }

    for (ComponentContextPair<Stream, StreamContext> pair: streams.values()) {
      if (pair.component instanceof SocketInputStream) {
        pair.component.activate(pair.context);
        activeStreams.add(pair);
      }
    }

    if (windowGenerator != null) {
      windowGenerator.activate(null);
    }
  }

  private void groupInputStreams(HashMap<String, ArrayList<String>> plumbing, NodeDeployInfo ndi)
  {
    for (NodeDeployInfo.NodeInputDeployInfo nidi: ndi.inputs) {
      String source = nidi.sourceNodeId.concat(".").concat(nidi.sourcePortName);

      /**
       * if we do not want to combine multiple streams with different partitions from the
       * same upstream node, we could also use the partition to group the streams together.
       * This logic comes with the danger that the performance of the group which shares the same
       * stream is bounded on the higher side by the performance of the lowest performer. May be
       * combining the streams is not such a good thing but let's see if we allow this as an option
       * to the user, what they end up choosing the most.
       */
      ArrayList<String> collection = plumbing.get(source);
      if (collection == null) {
        collection = new ArrayList<String>();
        plumbing.put(source, collection);
      }
      collection.add(ndi.id.concat(".").concat(nidi.portName));
    }
  }

  protected void shutdown()
  {
    if (windowGenerator != null) {
      windowGenerator.deactivate();
      windowGenerator.teardown();
      windowGenerator = null;
    }

    synchronized (activeNodes) {
      for (ComponentContextPair<Node, NodeContext> pair: activeNodes) {
        pair.component.deactivate();
        pair.component.teardown();
      }
    }

    for (ComponentContextPair<Stream, StreamContext> pair: activeStreams) {
      pair.component.deactivate();
      pair.component.teardown();
    }
    activeStreams.clear();
  }

  private void undeployNodes(List<NodeDeployInfo> nodeList)
  {
    HashMap<String, ArrayList<String>> groupedInputStreams = new HashMap<String, ArrayList<String>>();

    for (NodeDeployInfo ndi: nodeList) {
      ComponentContextPair<Node, NodeContext> pair = nodes.get(ndi.id);
      boolean contains;
      synchronized (activeNodes) {
        contains = activeNodes.contains(pair);
      }

      if (contains) {
        pair.component.deactivate();
        pair.component.teardown();
      }

      // we need to do the same thing as we did during undeployment to figure out how the streams were laid out
      groupInputStreams(groupedInputStreams, ndi);
    }


  }

  private class HdfsBackupAgent implements BackupAgent
  {
    final private NodeSerDe serDe;

    private HdfsBackupAgent(NodeSerDe serde) {
      serDe = serde;
    }

    @Override
    public void backup(String nodeId, long windowId, Object o) throws IOException
    {
      FileSystem fs = FileSystem.get(conf);
      Path path = new Path(StramChild.this.checkpointDfsPath + "/" + nodeId + "/" + windowId);
      LOG.debug("Backup path: {}", path);
      FSDataOutputStream output = fs.create(path);
      try {
        serDe.write(o, output);
        // record last backup window id for heartbeat
        StramChild.this.backupInfo.put(nodeId, windowId);
      } finally {
        output.close();
      }

    }

    @Override
    public Object restore(String id, long windowId) throws IOException
    {
      FileSystem fs = FileSystem.get(conf);
      FSDataInputStream input = fs.open(new Path(StramChild.this.checkpointDfsPath + "/" + id + "/" + windowId));
      try {
        return serDe.read(input);
      } finally {
        input.close();
      }
    }
  }
}
