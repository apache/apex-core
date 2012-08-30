/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.malhartech.dag.BackupAgent;
import com.malhartech.dag.Component;
import com.malhartech.dag.ComponentContextPair;
import com.malhartech.dag.DefaultSerDe;
import com.malhartech.dag.Node;
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
import com.malhartech.util.ScheduledThreadPoolExecutor;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
  private Object heartbeatTrigger = new Object();
  private WindowGenerator windowGenerator;
  private String checkpointDfsPath;
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
        hb.setCurrentWindowId(e.getValue().context.getCurrentWindowId());
        e.getValue().context.drainHeartbeatCounters(hb.getHeartbeatsContainer());
        DNodeState state = DNodeState.PROCESSING;
        if (!activeNodes.contains(e.getValue())) {
          state = DNodeState.IDLE;
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
          while (rsp != null && rsp.isPendingRequests()) {
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
    if (rsp.isShutdown()) {
      LOG.info("Received shutdown request");
      this.exitHeartbeatLoop = true;
      return;
    }

    if (rsp.getUndeployRequest() != null) {
      LOG.info("Undeploy request: {}", rsp.getUndeployRequest());
      undeployNodes(rsp.getUndeployRequest().nodeList);
    }

    if (rsp.getDeployRequest() != null) {
      LOG.info("Deploy request: {}", rsp.getDeployRequest());
      deployNodes(rsp.getDeployRequest().nodeList);
    }

    if (rsp.getNodeRequests() != null) {
      // extended processing per node
      for (StramToNodeRequest req: rsp.getNodeRequests()) {
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
        pair.context.requestBackup(new HdfsBackupAgent());
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

  private void deployNodes(List<NodeDeployInfo> nodeList)
  {
    Kryo kryo = new Kryo();
    HashMap<String, ArrayList<String>> one2ManyPlumbing = new HashMap<String, ArrayList<String>>();  // this holds all the plumbing hints
    for (NodeDeployInfo ndi: nodeList) {
      NodeContext nc = new NodeContext(ndi.id);
      Object foreignObject = kryo.readClassAndObject(new Input(ndi.serializedNode));
      try {
        Node node = (Node)foreignObject;
        node.setup(new NodeConfiguration(ndi.properties));
        nodes.put(ndi.id, new ComponentContextPair<Node, NodeContext>(node, nc));
        estimateStreams(one2ManyPlumbing, ndi);
      }
      catch (ClassCastException cce) {
        LOG.error("Expected {} but found {}", Node.class, foreignObject.getClass());
      }
    }

    // lets create all the output streams
    for (NodeDeployInfo ndi: nodeList) {
      Node node = nodes.get(ndi.id).component;
      for (NodeDeployInfo.NodeOutputDeployInfo nodi: ndi.outputs) {
        String source = ndi.id.concat(".").concat(nodi.portName);

        Stream stream;
        StreamContext context;

        ArrayList<String> collection = one2ManyPlumbing.get(source);
        if (collection == null) {
          // this must be buffer stream
          assert (nodi.isInline() == false);

          StreamConfiguration config = new StreamConfiguration();
          config.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved(nodi.bufferServerHost, nodi.bufferServerPort));

          stream = new BufferServerOutputStream();
          stream.setup(config);

          context = new StreamContext(nodi.declaredStreamId);
          context.setSourceId(source);
          context.setSinkId(nodi.bufferServerHost.concat(":").concat(String.valueOf(nodi.bufferServerPort)));
        }
        else if (collection.size() == 1) {
          assert (nodi.isInline());

          stream = new InlineStream();
          stream.setup(new StreamConfiguration());

          context = new StreamContext(nodi.declaredStreamId);
          context.setSourceId(source);
        }
        else {
          stream = new MuxStream();
          stream.setup(new StreamConfiguration());

          context = new StreamContext(nodi.declaredStreamId);
          context.setSourceId(source);
        }

        Sink s = stream.connect(Component.INPUT, node);
        node.connect(nodi.portName, s);

        streams.put(source, new ComponentContextPair<Stream, StreamContext>(stream, context));
      }
    }

    // lets create all the input streams
    for (NodeDeployInfo ndi: nodeList) {
      Node node = nodes.get(ndi.id).component;
      if (ndi.inputs == null || ndi.inputs.isEmpty()) {
        // this node is load generator aka input adapter node
        if (windowGenerator == null) {
          Configuration dagConfig = new Configuration(); // STRAM should provide this object, we are mimicing here.
          dagConfig.setLong("StartMillis", System.currentTimeMillis()); // no need to set if done right
          dagConfig.setInt("IntervalMillis", 500); // no need to set if done right

          windowGenerator = new WindowGenerator(new ScheduledThreadPoolExecutor(1));
          windowGenerator.setup(dagConfig);
        }

        Sink s = node.connect(Component.INPUT, windowGenerator);
        windowGenerator.connect(containerId, s);
      }
      else {
        for (NodeDeployInfo.NodeInputDeployInfo nidi: ndi.inputs) {
          String source = nidi.sourceNodeId.concat(".").concat(nidi.sourcePortName);
          String sink = ndi.id.concat(".").concat(nidi.portName);

          ComponentContextPair<Stream, StreamContext> pair = streams.get(source);
          if (pair == null) {
            // it's buffer server stream
            assert (nidi.isInline() == false);

            StreamConfiguration config = new StreamConfiguration();
            config.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved(nidi.bufferServerHost, nidi.bufferServerPort));

            Stream stream = new BufferServerInputStream();
            stream.setup(config);

            StreamContext context = new BufferServerStreamContext(nidi.declaredStreamId);
            ((BufferServerStreamContext)context).setPartitions(nidi.partitionKeys);
            context.setSourceId(source);
            context.setSinkId(sink);
            pair = new ComponentContextPair<Stream, StreamContext>(stream, context);

            Sink s = node.connect(nidi.portName, stream);
            stream.connect(sink, s);
          }
          else {
            Sink s = node.connect(nidi.portName, pair.component);
            String streamSinkId = pair.context.getSinkId();
            
            if (streamSinkId == null) {
              pair.context.setSinkId(sink);
            }
            else {
              pair.context.setSinkId(streamSinkId.concat(", ").concat(sink));
            }

            if (nidi.partitionKeys == null || nidi.partitionKeys.isEmpty()) {
              pair.component.connect(sink, s);
            }
            else {
              /*
               * generally speaking we do not have partitions on the inline streams so the control should not
               * come here but if it comes, then we are ready to handle it using the partition aware streams.
               */
              PartitionAwareSink pas = new PartitionAwareSink(new DefaultSerDe(), nidi.partitionKeys, s); // serde should be something else
              pair.component.connect(sink, pas);
            }
          }

          streams.put(sink, pair);
        }
      }
    }

    activeStreams.addAll(streams.values());
    for (ComponentContextPair pair: activeStreams) {
      pair.component.activate(pair.context);
    }

    for (final ComponentContextPair pair: nodes.values()) {
      Thread t = new Thread()
      {
        @Override
        public void run()
        {
          pair.component.activate(pair.context);
          activeNodes.remove(pair);
        }
      };
      t.start();
      activeNodes.add(pair);
    }

    if (windowGenerator != null) {
      windowGenerator.activate(null);
    }
  }

  private void estimateStreams(HashMap<String, ArrayList<String>> plumbing, NodeDeployInfo ndi)
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

  private void shutdown()
  {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  private void undeployNodes(List<NodeDeployInfo> nodeList)
  {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  private class HdfsBackupAgent implements BackupAgent
  {
    private FSDataOutputStream output;
    private String outputNodeId;
    private long outputWindowId;

    @Override
    public OutputStream borrowOutputStream(String id, long windowId) throws IOException
    {
      FileSystem fs = FileSystem.get(conf);
      Path path = new Path(StramChild.this.checkpointDfsPath + "/" + id + "/" + windowId);
      LOG.debug("Backup path: {}", path);
      outputNodeId = id;
      outputWindowId = windowId;
      return (output = fs.create(path));
    }

    @Override
    public void returnOutputStream(OutputStream os) throws IOException
    {
      assert (output == os);
      output.close();
      // record last backup window id for heartbeat
      StramChild.this.backupInfo.put(outputNodeId, outputWindowId);
    }

    @Override
    public InputStream getInputStream(String id, long windowId) throws IOException
    {
      FileSystem fs = FileSystem.get(conf);
      FSDataInputStream input = fs.open(new Path(StramChild.this.checkpointDfsPath + "/" + id + "/" + windowId));
      return input;
    }
  }
}