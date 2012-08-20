/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

import com.malhartech.dag.BackupAgent;
import com.malhartech.dag.InputAdapter;
import com.malhartech.dag.InternalNode;
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

/**
 * The main() for streaming node processes launched by {@link com.malhartech.stram.StramAppMaster}.
 */
public class StramChild
{
  private static Logger LOG = LoggerFactory.getLogger(StramChild.class);
  final private String containerId;
  final private Configuration conf;
  final private StreamingNodeUmbilicalProtocol umbilical;
  final private Map<String, InternalNode> nodeList = new ConcurrentHashMap<String, InternalNode>();
  final private Map<String, Thread> activeNodeList = new ConcurrentHashMap<String, Thread>();
  final private List<Stream> streams = new ArrayList<Stream>();
  final private List<InputAdapter> inputAdapters = new ArrayList<InputAdapter>();
  private long heartbeatIntervalMillis = 1000;
  private boolean exitHeartbeatLoop = false;
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

  /**
   * Make accessible for unit testing
   */
  protected List<InputAdapter> getInputAdapters()
  {
    return this.inputAdapters;
  }

  /**
   * Initialize stream between 2 nodes
   *
   * @param sc
   * @param ctx
   */
  private void initStream(StreamPConf sc, Map<String, NodePConf> nodeConfMap)
  {
    InternalNode sourceNode = nodeList.get(sc.getSourceNodeId());
    
    if (sourceNode instanceof AdapterWrapperNode) {
      AdapterWrapperNode wrapper = (AdapterWrapperNode) sourceNode;
      // input adapter
      this.inputAdapters.add((InputAdapter) wrapper.getAdapterStream());
    }

    InternalNode targetNode = nodeList.get(sc.getTargetNodeId());
    if (sc.isInline()) {
      LOG.info("inline connection from {} to {}", sc.getSourceNodeId(), sc.getTargetNodeId());
      InlineStream stream = new InlineStream();
      StreamContext dsc = new StreamContext(sc.getSourceNodeId(), sc.getTargetNodeId());
      dsc.setStartingWindowId(nodeConfMap.get(sc.getSourceNodeId()).getCheckpointWindowId());
      stream.setContext(dsc);

      Sink sink = targetNode.getSink(dsc);
//      LOG.info(dsc + " setting sink to " + sink);
      dsc.setSink(sink);
      // operation is additive - there can be multiple output streams
      sourceNode.addOutputStream(dsc);
      
      this.streams.add(stream);
    }
    else {

      // buffer server connection between nodes
      LOG.info("buffer server stream from {} to {}", sc.getSourceNodeId(), sc.getTargetNodeId());

      BufferServerStreamContext streamContext = new BufferServerStreamContext(sc.getSourceNodeId(), sc.getTargetNodeId());
      streamContext.setSerde(StramUtils.getSerdeInstance(sc.getProperties()));
      streamContext.setId(sc.getId());
      
      StreamConfiguration streamConf = new StreamConfiguration(sc.getProperties());
      streamConf.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved(sc.getBufferServerHost(), sc.getBufferServerPort()));
      if (sourceNode != null) {
        // setup output stream as sink for source node
//        LOG.info("Node {} is publisher for {}/{}", new Object[]{sourceNode, sc.getId(), sc.getSourceNodeId()});
        streamContext.setStartingWindowId(nodeConfMap.get(sc.getSourceNodeId()).getCheckpointWindowId());
        BufferServerOutputStream oss = new BufferServerOutputStream();
        oss.setup(streamConf);

        oss.setContext(streamContext);
//        LOG.info(streamContext + " setting sink to " + oss);

        streamContext.setSink(oss);
        sourceNode.addOutputStream(streamContext);
        this.streams.add(oss);
      }

      if (targetNode != null) {
        streamContext.setStartingWindowId(nodeConfMap.get(sc.getTargetNodeId()).getCheckpointWindowId());

        Sink sink = targetNode.getSink(streamContext);
//        LOG.info(streamContext + " setting sink to " + sink);

        streamContext.setSink(sink);

        // setup input stream for target node
//        LOG.info("Node {} is subscriber for {}/{}", new Object[]{targetNode, sc.getId(), sc.getSourceNodeId()});
        BufferServerInputStream iss = new BufferServerInputStream();
        iss.setup(streamConf);

        streamContext.setPartitions(sc.getPartitionKeys());

        iss.setContext(streamContext);
        this.streams.add(iss);
      }
    }
  }

  protected void init(StreamingContainerContext ctx) throws IOException
  {
    this.heartbeatIntervalMillis = ctx.getHeartbeatIntervalMillis();
    if (this.heartbeatIntervalMillis == 0) {
      this.heartbeatIntervalMillis = 1000;
    }

    if ((this.checkpointDfsPath = ctx.getCheckpointDfsPath()) == null) {
      this.checkpointDfsPath = "checkpoint-dfs-path-not-configured";
    }

    final Map<String, NodePConf> nodeConfMap = new HashMap<String, NodePConf>();
    
    // create nodes
    for (NodePConf nodeConf : ctx.getNodes()) {
      nodeConfMap.put(nodeConf.getDnodeId(), nodeConf);
      InternalNode dnode = initOrRestoreNode(nodeConf, conf);
      NodeConfiguration nc = new NodeConfiguration(nodeConf.getProperties());
      dnode.setup(nc);
//      LOG.info("Initialized node {} ({})", snc.getDnodeId(), snc.getLogicalId());
      nodeList.put(nodeConf.getDnodeId(), dnode);
    }

    // wire stream connections
    for (StreamPConf sc : ctx.getStreams()) {
//      LOG.debug("Deploying stream " + sc.getId());
      if (sc.getSourceNodeId() == null || sc.getTargetNodeId() == null) {
        throw new IllegalArgumentException("Invalid stream conf (source and target need to be set): " + sc.getId());
      }
      initStream(sc, nodeConfMap);
    }

    // ideally we would like to activate the output streams for a node before the input streams
    // are activated. But does not look like we have that fine control here. we should get it.
    for (Stream s : this.streams) {
      LOG.debug("activate {} with startWindowId {}", s, s.getContext().getStartingWindowId());
      s.activate();
    }

    for (Entry<String, InternalNode> e : nodeList.entrySet()) {
      final InternalNode node = e.getValue();
      final String id = e.getKey();
      // launch nodes
      Runnable nodeRunnable = new Runnable()
      {
        @Override
        public void run()
        {
          NodePConf nodeConf = nodeConfMap.get(id);
          NodeContext nc = new NodeContext(id);
          nc.setCurrentWindowId(nodeConf.getCheckpointWindowId());
          node.start(nc);
          /*
           * processing has ended
           */
          activeNodeList.remove(id);
        }
      };

      Thread launchThread = new Thread(nodeRunnable, "node-" + e.getKey());
      activeNodeList.put(e.getKey(), launchThread);
      launchThread.start();
    }

    // activate all the input adapters if any
    for (Stream ia : inputAdapters) {
      ia.activate();
    }

    windowGenerator = new WindowGenerator(this.inputAdapters, ctx.getStartWindowMillis(), ctx.getWindowSizeMillis());
    if (ctx.getWindowSizeMillis() > 0) {
      windowGenerator.start();
    }
  }

  protected void shutdown()
  {
    windowGenerator.stop();

    // ideally we should do the graph traversal and shutdown as we descend down. At this time
    // we do not have a choice because the things are not setup to facilitate it, so brute force.

    /*
     * first stop all the input adapters.
     */
    for (InternalNode node : nodeList.values()) {
      if (node instanceof AdapterWrapperNode && ((AdapterWrapperNode) node).isInput()) {
        LOG.debug("teardown " + node);
        node.stop();
      }
    }

    /*
     * now stop all the nodes.
     */
    for (InternalNode node : nodeList.values()) {
      if (!(node instanceof AdapterWrapperNode)) {
        LOG.debug("teardown " + node);
        node.stop();
      }
    }

    /*
     * stop all the streams.
     */
    for (Stream s : this.streams) {
      LOG.debug("teardown " + s);
      s.teardown();
      s.setContext(Stream.DISCONNECTED_STREAM_CONTEXT);
    }


    /*
     * stop all the output adapters
     */
    for (InternalNode node : this.nodeList.values()) {
      if (node instanceof AdapterWrapperNode && !((AdapterWrapperNode) node).isInput()) {
        LOG.debug("teardown " + node);
        node.stop();
      }
    }

    for (Thread t : activeNodeList.values()) {
      try {
        LOG.debug("Joining thread {}", t.getName());
        t.join(2000);
      }
      catch (Exception e) {
        LOG.warn("Interrupted while waiting for thread {} to complete.", t.getName());
      }
    }

  }

  private void heartbeatLoop() throws IOException
  {
    umbilical.log(containerId, "[" + containerId + "] Entering heartbeat loop..");
    LOG.debug("Entering hearbeat loop (interval is {} ms)", this.heartbeatIntervalMillis);
    while (!exitHeartbeatLoop) {

      try {
        Thread.sleep(heartbeatIntervalMillis);
      }
      catch (InterruptedException e1) {
        LOG.warn("Interrupted in heartbeat loop, exiting..");
        break;
      }

      long currentTime = System.currentTimeMillis();
      ContainerHeartbeat msg = new ContainerHeartbeat();
      msg.setContainerId(this.containerId);
      List<StreamingNodeHeartbeat> heartbeats = new ArrayList<StreamingNodeHeartbeat>(nodeList.size());

      // gather heartbeat info for all nodes
      for (Map.Entry<String, InternalNode> e : nodeList.entrySet()) {
        StreamingNodeHeartbeat hb = new StreamingNodeHeartbeat();
        hb.setNodeId(e.getKey());
        hb.setGeneratedTms(currentTime);
        hb.setIntervalMs(heartbeatIntervalMillis);
        hb.setCurrentWindowId(e.getValue().getContext().getCurrentWindowId());
        e.getValue().getContext().drainHeartbeatCounters(hb.getHeartbeatsContainer());
        DNodeState state = DNodeState.PROCESSING;
        if (!activeNodeList.containsKey(e.getKey())) {
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
            Thread.sleep(500);
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
      LOG.warn("Ignoring undeploy request: {}", rsp.getUndeployRequest());
    }
    
    if (rsp.getDeployRequest() != null) {
      LOG.warn("Ignoring deploy request: {}", rsp.getDeployRequest());
    }
    
    if (rsp.getNodeRequests() != null) {
      // extended processing per node
      for (StramToNodeRequest req : rsp.getNodeRequests()) {
        InternalNode n = nodeList.get(req.getNodeId());
        if (n == null) {
          LOG.warn("Received request with invalid node id {} ({})", req.getNodeId(), req);
        }
        else {
          LOG.debug("Stram request: {}", req);
          processStramRequest(n, req);
        }
      }
    }    
  }

  // look at the implementation of the stream context it stores the source and sink ids.
  // I am not sure of node_id is unique (should be since it's stored in the map) and 
  // whether the source id and sink id are unique in a container. Does it still hold true
  // when load balancing happens within a container? The following logic works provided
  // assumption of node_id, source and sink ids are correct.
  public void shutdown(String node_id)
  {
    /*
     * make sure that we have a node we are asked to shutdown in this container.
     */
    InternalNode in = nodeList.get(node_id);
    if (in == null) {
      throw new IllegalArgumentException("node with nodeid " + node_id + " does not exist");
    }

    ArrayList<Stream> inputStreams = new ArrayList<Stream>();
    /*
     * lets find out all the input streams for this node.
     */
    for (Stream s : inputAdapters) {
      if (s.getContext().getSinkId().equals(node_id)) {
        inputStreams.add(s);
      }
    }


    ArrayList<Stream> outputStreams = new ArrayList<Stream>();
    /*
     * lets also find out remaining input streams and output streams.
     */
    for (Stream s : streams) {
      if (node_id.equals(s.getContext().getSinkId())) {
        inputStreams.add(s);
      }
      else if (node_id.equals(s.getContext().getSourceId())) {
        outputStreams.add(s);
      }
    }

    assert (!(inputStreams.isEmpty() && outputStreams.isEmpty()));

    /*
     * we bring down the node by first stopping inputs, then outputs and then the nodes itself.
     */

    for (Stream s : inputStreams) {
      s.teardown();
      s.setContext(Stream.DISCONNECTED_STREAM_CONTEXT);
      if (s instanceof InputAdapter) {
        inputAdapters.remove((InputAdapter) s);
      }
      else {
        streams.remove(s);
      }
    }

    for (Stream s : outputStreams) {
      s.teardown();
      s.setContext(Stream.DISCONNECTED_STREAM_CONTEXT);
      streams.remove(s);
    }

    if (activeNodeList.containsKey(node_id)) {
      in.stop();
      activeNodeList.remove(node_id);
    }

    nodeList.remove(node_id);
  }
  
  /**
   * Process request from stram for further communication through the protocol. Extended reporting is on a per node basis (won't occur under regular operation)
   *
   * @param n
   * @param snr
   */
  private void processStramRequest(InternalNode n, StramToNodeRequest snr)
  {
    assert (n.getContext() != null);
    switch (snr.getRequestType()) {
      case REPORT_PARTION_STATS:
        LOG.warn("Ignoring stram request {}", snr);
        break;

      case CHECKPOINT:
        n.getContext().requestBackup(new HdfsBackupAgent());
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
      for (Token<?> token : UserGroupInformation.getCurrentUser().getTokens()) {
        childUGI.addToken(token);
      }

      // TODO: start node in doAs block
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
      // This assumes that on return from Task.start()
      // there is no more logging done.
      LogManager.shutdown();
    }
  }

  private InternalNode initOrRestoreNode(NodePConf nodeConf, Configuration conf) {
    if (nodeConf.getCheckpointWindowId() != 0) {
      LOG.info("Restore node {} to checkpoint {}", nodeConf, nodeConf.getCheckpointWindowId());

      // NodeContext here only required to pass the id to restore.
      // The actual node context will be set when activating the node.
      NodeContext restoreCtx = new NodeContext(nodeConf.getDnodeId());    
      try {
        return (InternalNode)restoreCtx.restore(new HdfsBackupAgent(), nodeConf.getCheckpointWindowId());
      } catch (Exception e) {
        throw new RuntimeException("Failed to restore node from checkpoint.", e);
      }
    } else {
      return StramUtils.initNode(nodeConf, conf);
    }
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
