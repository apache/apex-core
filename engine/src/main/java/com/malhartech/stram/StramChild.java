/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.dag.NodeContext.HeartbeatCounters;
import com.malhartech.dag.*;
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
import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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
 * The main() for streaming node processes launched by {@link com.malhartech.stram.StramAppMaster}.
 */
public class StramChild
{
  private static Logger LOG = LoggerFactory.getLogger(StramChild.class);
  final private String containerId;
  final private Configuration conf;
  final private StreamingNodeUmbilicalProtocol umbilical;
  final private Map<String, AbstractNode> nodeList = new ConcurrentHashMap<String, AbstractNode>();
  final private Map<String, Thread> activeNodeList = new ConcurrentHashMap<String, Thread>();
  final private List<Stream> streams = new ArrayList<Stream>();
  final private List<InputAdapter> inputAdapters = new ArrayList<InputAdapter>();
  private long heartbeatIntervalMillis = 1000;
  private boolean exitHeartbeatLoop = false;
  private WindowGenerator windowGenerator;
  private String checkpointDfsPath;
  /**
   * Map of last backup window id that is used to communicate checkpoint state back to Stram.
   * TODO: Consider adding this to the node context instead.
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
  protected List<InputAdapter> getInputAdapters() {
    return this.inputAdapters;
  }
  
  /**
   * Initialize stream between 2 nodes
   *
   * @param sc
   * @param ctx
   */
  private void initStream(StreamPConf sc, StreamingContainerContext ctx)
  {
    AbstractNode sourceNode = nodeList.get(sc.getSourceNodeId());
    if (sourceNode instanceof AdapterWrapperNode) {
      AdapterWrapperNode wrapper = (AdapterWrapperNode) sourceNode;
      // input adapter
      this.inputAdapters.add((InputAdapter) wrapper.getAdapterStream());
    }

    AbstractNode targetNode = nodeList.get(sc.getTargetNodeId());
    if (sc.isInline()) {
      LOG.info("inline connection from {} to {}", sc.getSourceNodeId(), sc.getTargetNodeId());
      InlineStream stream = new InlineStream();
      com.malhartech.dag.StreamContext dsc = new com.malhartech.dag.StreamContext();
      stream.setContext(dsc);
      Sink sink = targetNode.getSink(dsc);

//      LOG.info(dsc + " setting sink to " + sink);
      dsc.setSink(sink);

      // operation is additive - there can be multiple output streams
      sourceNode.addOutputStream(dsc);
    }
    else {

      // buffer server connection between nodes
      LOG.info("buffer server stream from {} to {}", sc.getSourceNodeId(), sc.getTargetNodeId());

      BufferServerStreamContext streamContext = new BufferServerStreamContext();
      streamContext.setSerde(StramUtils.getSerdeInstance(sc.getProperties()));
      streamContext.setSourceId(sc.getSourceNodeId());
      streamContext.setSinkId(sc.getTargetNodeId());
      streamContext.setId(sc.getId());

      StreamConfiguration streamConf = new StreamConfiguration(sc.getProperties());
      streamConf.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved(sc.getBufferServerHost(), sc.getBufferServerPort()));
      if (sourceNode != null) {
        // setup output stream as sink for source node
//        LOG.info("Node {} is publisher for {}/{}", new Object[]{sourceNode, sc.getId(), sc.getSourceNodeId()});
        BufferServerOutputStream oss = new BufferServerOutputStream();
        oss.setup(streamConf);

        oss.setContext(streamContext);
//        LOG.info(streamContext + " setting sink to " + oss);

        streamContext.setSink(oss);
        sourceNode.addOutputStream(streamContext);
        this.streams.add(oss);
      }

      if (targetNode != null) {
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
    
    // create nodes
    for (NodePConf snc : ctx.getNodes()) {
      AbstractNode dnode = initNode(snc, conf);
      NodeConfiguration nc = new NodeConfiguration(snc.getProperties());
      dnode.setup(nc);
//      LOG.info("Initialized node {} ({})", snc.getDnodeId(), snc.getLogicalId());
      nodeList.put(snc.getDnodeId(), dnode);
    }

    // wire stream connections
    for (StreamPConf sc : ctx.getStreams()) {
//      LOG.debug("Deploying stream " + sc.getId());
      if (sc.getSourceNodeId() == null || sc.getTargetNodeId() == null) {
        throw new IllegalArgumentException("Invalid stream conf (source and target need to be set): " + sc.getId());
      }
      initStream(sc, ctx);
    }

    // ideally we would like to activate the output streams for a node before the input streams
    // are activated. But does not look like we have that fine control here. we should get it.
    for (Stream s : this.streams) {
//      LOG.info("activate " + s);
      s.activate();
    }

    for (Entry<String, AbstractNode> e : nodeList.entrySet()) {
      final AbstractNode node = e.getValue();
      final String id = e.getKey();
      // launch nodes
      Runnable nodeRunnable = new Runnable()
      {
        @Override
        public void run()
        {
          node.run(new NodeContext(id));
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
    for (AbstractNode node : nodeList.values()) {
      if (node instanceof AdapterWrapperNode && ((AdapterWrapperNode) node).isInput()) {
        LOG.debug("teardown " + node);
        node.stopSafely();
      }
    }

    /*
     * now stop all the nodes.
     */
    for (AbstractNode node : nodeList.values()) {
      if (!(node instanceof AdapterWrapperNode)) {
        LOG.debug("teardown " + node);
        node.stopSafely();
      }
    }

    /*
     * stop all the streams.
     */
    for (Stream s : this.streams) {
      LOG.debug("teardown " + s);
      s.teardown();
    }


    /*
     * stop all the output adapters
     */
    for (AbstractNode node : this.nodeList.values()) {
      if (node instanceof AdapterWrapperNode && !((AdapterWrapperNode) node).isInput()) {
        LOG.debug("teardown " + node);
        node.stopSafely();
      }
    }
    
    for (Thread t : activeNodeList.values()) {
      try {
        LOG.debug("Joining thread {}", t.getName());
        t.join(2000);
      } catch (Exception e) {
        LOG.warn("Interrupted while waiting for thread {} to complete.", t.getName());
      }
    }
    
  }

  private void heartbeatLoop() throws IOException
  {
    umbilical.echo(containerId, "[" + containerId + "] Entering heartbeat loop..");
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
      for (Map.Entry<String, AbstractNode> e : nodeList.entrySet()) {
        StreamingNodeHeartbeat hb = new StreamingNodeHeartbeat();
        HeartbeatCounters counters = e.getValue().getContext().resetHeartbeatCounters();
        hb.setNodeId(e.getKey());
        hb.setGeneratedTms(currentTime);
        hb.setNumberTuplesProcessed((int) counters.tuplesProcessed);
        hb.setIntervalMs(heartbeatIntervalMillis);
        hb.setCurrentWindowId(e.getValue().getContext().getCurrentWindowId());
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
        }
      }
      catch (Exception e) {
        LOG.warn("Exception received (may be during shutdown?) {}", e.getLocalizedMessage());
      }
    }
    LOG.debug("Exiting hearbeat loop");
    umbilical.echo(containerId, "[" + containerId + "] Exiting heartbeat loop..");
  }

  protected void processHeartbeatResponse(ContainerHeartbeatResponse rsp)
  {
    if (rsp.isShutdown()) {
      LOG.info("Received shutdown request");
      this.exitHeartbeatLoop = true;
      return;
    }
    if (rsp.getNodeRequests() != null) {
      // extended processing per node
      for (StramToNodeRequest req : rsp.getNodeRequests()) {
        AbstractNode n = nodeList.get(req.getNodeId());
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

  /**
   * Process request from stram for further communication through the protocol. Extended reporting is on a per node basis (won't occur under regular operation)
   *
   * @param n
   * @param snr
   */
  private void processStramRequest(AbstractNode n, StramToNodeRequest snr)
  {
    switch (snr.getRequestType()) {
      case SHUTDOWN:
      //LOG.info("Received shutdown request");
      //this.exitHeartbeatLoop = true;
      //break;
      case REPORT_PARTION_STATS:
      case RECONFIGURE:
        LOG.warn("Ignoring stram request {}", snr);
        break;

      case CHECKPOINT:
        // the follow code needs scrubbing to ensure that the input and output are setup correctly.
        n.getContext().requestBackup(
          new BackupAgent()
          {
            private FSDataOutputStream output;
            private String outputNodeId;
            private long outputWindowId;
            private long inputWindowId;
            
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
            public InputStream getInputStream(String id) throws IOException
            {
              FileSystem fs = FileSystem.get(conf);
              FSDataInputStream input = fs.open(new Path(StramChild.this.checkpointDfsPath + "/" + id + "/" + inputWindowId));
              return input;
            }
          });
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

      // TODO: run node in doAs block
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
      umbilical.echo(childId, e.getMessage());
    }
    catch (Exception exception) {
      LOG.warn("Exception running child : "
               + StringUtils.stringifyException(exception));
      // Report back any failures, for diagnostic purposes
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      exception.printStackTrace(new PrintStream(baos));
      umbilical.echo(childId, "FATAL: " + baos.toString());
    }
    catch (Throwable throwable) {
      LOG.error("Error running child : "
                + StringUtils.stringifyException(throwable));
      Throwable tCause = throwable.getCause();
      String cause = tCause == null
                     ? throwable.getMessage()
                     : StringUtils.stringifyException(tCause);
      umbilical.echo(childId, cause);
    }
    finally {
      RPC.stopProxy(umbilical);
      DefaultMetricsSystem.shutdown();
      // Shutting down log4j of the child-vm...
      // This assumes that on return from Task.run()
      // there is no more logging done.
      LogManager.shutdown();
    }
  }

  /**
   * Instantiate node from configuration. (happens in the child container, not the stram master process.)
   *
   * @param nodeConf
   * @param conf
   */
  public static AbstractNode initNode(NodePConf nodeCtx, Configuration conf)
  {
    try {
      Class<? extends AbstractNode> nodeClass = Class.forName(nodeCtx.getDnodeClassName()).asSubclass(AbstractNode.class);
      Constructor<? extends AbstractNode> c = nodeClass.getConstructor();
      AbstractNode node = c.newInstance();
      // populate custom properties
      BeanUtils.populate(node, nodeCtx.getProperties());
      return node;
    }
    catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Node class not found: " + nodeCtx.getDnodeClassName(), e);
    }
    catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    }
    catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    }
    catch (SecurityException e) {
      throw new IllegalArgumentException("Error creating instance of class: " + nodeCtx.getDnodeClassName(), e);
    }
    catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Constructor with NodeContext not found: " + nodeCtx.getDnodeClassName(), e);
    }
    catch (InstantiationException e) {
      throw new IllegalArgumentException("Failed to instantiate: " + nodeCtx.getDnodeClassName(), e);
    }
  }
}
