/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSError;
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

import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.InputAdapter;
import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.NodeContext;
import com.malhartech.dag.NodeContext.HeartbeatCounters;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Stream;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeat;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingNodeHeartbeat;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingNodeHeartbeat.DNodeState;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stream.BufferServerInputStream;
import com.malhartech.stream.BufferServerOutputStream;
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
  final private Map<String, AbstractNode> nodeList = new ConcurrentHashMap<String, AbstractNode>();
  final private Map<String, Thread> activeNodeList = new ConcurrentHashMap<String, Thread>();
  final private Map<String, Stream> streams = new ConcurrentHashMap<String, Stream>();
  final private Map<String, InputAdapter> inputAdapters = new ConcurrentHashMap<String, InputAdapter>();
  private long heartbeatIntervalMillis = 1000;
  private boolean exitHeartbeatLoop = false;
  private WindowGenerator windowGenerator;

  protected StramChild(String containerId, Configuration conf, StreamingNodeUmbilicalProtocol umbilical)
  {
    this.umbilical = umbilical;
    this.containerId = containerId;
    this.conf = conf;
  }

  private void init() throws IOException
  {
    StreamingContainerContext ctx = umbilical.getInitContext(containerId);
    LOG.info("Got context: " + ctx);

    this.heartbeatIntervalMillis = ctx.getHeartbeatIntervalMillis();

    // create nodes
    for (StreamingNodeContext snc : ctx.getNodes()) {
        AbstractNode dnode = initNode(snc, conf);
        NodeConfiguration nc = new NodeConfiguration();
        for (Map.Entry<String, String> e : snc.getProperties().entrySet()) {
            nc.set(e.getKey(), e.getValue());
        }
        dnode.setup(nc);
        LOG.info("Initialized node " + snc.getLogicalId());
        nodeList.put(snc.getDnodeId(), dnode);
    }

    // wire stream connections
    for (StreamContext sc : ctx.getStreams()) {
      LOG.debug("Deploy stream " + sc.getId());
      if (sc.isInline()) {
        AbstractNode source = nodeList.get(sc.getSourceNodeId());
        AbstractNode target = nodeList.get(sc.getTargetNodeId());
        LOG.info("inline connection from {} to {}", source, target);
        InlineStream stream = new InlineStream();
        com.malhartech.dag.StreamContext dsc = new com.malhartech.dag.StreamContext();
        stream.setContext(dsc);
        Sink sink = target.getSink(dsc);
        dsc.setSink(sink); // this is circular... we may want to chance it later.
        // operation is additive - there can be multiple output streams
        source.addSink(stream);
      }
      else if (sc.getSourceNodeId() != null && sc.getTargetNodeId() != null) {
        // buffer server connection between nodes
        LOG.info("buffer server stream from {} to {}", sc.getSourceNodeId(), sc.getTargetNodeId());
        AbstractNode sourceNode = nodeList.get(sc.getSourceNodeId());
        AbstractNode targetNode = nodeList.get(sc.getTargetNodeId());
        com.malhartech.dag.StreamContext streamContext = new com.malhartech.dag.StreamContext();
        if (targetNode != null) {
          Sink sink = targetNode.getSink(streamContext);
          streamContext.setSink(sink);
        }
        streamContext.setSerde(StramUtils.getSerdeInstance(sc.getProperties()));
        streamContext.setWindowId(ctx.getStartWindowMillis());

        StreamConfiguration streamConf = new StreamConfiguration();
        streamConf.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved(sc.getBufferServerHost(), sc.getBufferServerPort()));
        if (sourceNode != null) {
          // setup output stream as sink for source node
          LOG.info("Node {} is buffer server publisher for stream {}", sourceNode, sc.getId());
          BufferServerOutputStream oss = new BufferServerOutputStream();
          oss.setup(streamConf);
          oss.setContext(streamContext, sc.getSourceNodeId(), sc.getId());
          sourceNode.addSink(oss);
          this.streams.put(sc.getId(), oss);
        }
        if (targetNode != null) {
          // setup input stream for target node
          LOG.info("Node {} is buffer server subscriber for stream {}", targetNode, sc.getId());
          BufferServerInputStream iss = new BufferServerInputStream();
          iss.setup(streamConf);
          List<String> partitions = Collections.emptyList();
          if (sc.getPartitionKeys() != null) {
            partitions = new ArrayList<String>(sc.getPartitionKeys().size());
            for (byte[] partition : sc.getPartitionKeys()) {
              partitions.add(new String(partition));
            }
          }
          iss.setContext(streamContext, sc.getSourceNodeId(), sc.getId(), sc.getTargetNodeId(), partitions);
          this.streams.put(sc.getId(), iss);
        }
      }
      else {

        StreamConfiguration streamConf = new StreamConfiguration();
        streamConf.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved(sc.getBufferServerHost(), sc.getBufferServerPort()));

        for (Map.Entry<String, String> e : sc.getProperties().entrySet()) {
          streamConf.set(e.getKey(), e.getValue());
        }

        if (sc.getSourceNodeId() == null) {
          // input adapter
          InputAdapter stream = initStream(sc.getProperties(), streamConf, nodeList.get(sc.getTargetNodeId()));
          LOG.debug("Created input adapter {}", sc.getId());
          this.inputAdapters.put(sc.getId(), stream);
          this.streams.put(sc.getId(), stream);
        }
        else {
          AbstractNode source = nodeList.get(sc.getSourceNodeId());

          // output adapter
          Stream stream = initStream(sc.getProperties(), streamConf, null); // no sink
          this.streams.put(sc.getId(), stream);
          source.addSink((Sink) stream);

        }

      }
    }

    for (final AbstractNode node : nodeList.values()) {
      // launch nodes
      Runnable nodeRunnable = new Runnable()
      {

        @Override
        public void run()
        {
          node.run();
          node.teardown();
          // processing has ended
          activeNodeList.remove(node.getContext().getId());
        }
      };
      Thread launchThread = new Thread(nodeRunnable);
      activeNodeList.put(node.getContext().getId(), launchThread);
      launchThread.start();
    }

    windowGenerator = new WindowGenerator(this.inputAdapters.values(), ctx.getStartWindowMillis(), ctx.getWindowSizeMillis());
    if (ctx.getWindowSizeMillis() > 0) {
      windowGenerator.start();
    }
  }

  private void shutdown()
  {
    windowGenerator.stop();
    for (Stream s : this.streams.values()) {
      s.teardown();
    }
  }

  private void heartbeatLoop() throws IOException
  {
    umbilical.echo(containerId, "[" + containerId + "] Entering heartbeat loop..");
    LOG.info("Entering hearbeat loop");
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
        HeartbeatCounters counters = e.getValue().resetHeartbeatCounters();
        hb.setNodeId(e.getKey());
        hb.setGeneratedTms(currentTime);
        hb.setNumberTuplesProcessed((int) counters.tuplesProcessed);
        hb.setIntervalMs(heartbeatIntervalMillis);
        DNodeState state = DNodeState.PROCESSING;
        if (!activeNodeList.containsKey(e.getKey())) {
          state = DNodeState.IDLE;
        }
        hb.setState(state.name());
        heartbeats.add(hb);
      }
      msg.setDnodeEntries(heartbeats);

      // heartbeat call and follow-up processing
      ContainerHeartbeatResponse rsp = umbilical.processHeartbeat(msg);
      if (rsp != null) {
        processHeartbeatResponse(rsp);
      }
    }
    LOG.info("Exiting hearbeat loop");
    umbilical.echo(containerId, "[" + containerId + "] Exiting heartbeat loop..");
  }

  private void processHeartbeatResponse(ContainerHeartbeatResponse rsp)
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
          LOG.info("Stram request: {}", req);
          processStramRequest(n, req);
        }
      }
    }
  }

  /**
   * Process request from stram for further communication through the protocol.
   * Extended reporting is on a per node basis (won't occur under regular
   * operation)
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
      default:
        LOG.error("Unknown request from stram {}", snr);
    }
  }

  public static void main(String[] args) throws Throwable
  {
    LOG.info("Child starting with classpath: {}", System.getProperty("java.class.path"));

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
        return (StreamingNodeUmbilicalProtocol) RPC.getProxy(StreamingNodeUmbilicalProtocol.class,
                                                             StreamingNodeUmbilicalProtocol.versionID, address, defaultConf);
      }
    });

    LOG.debug("PID: " + System.getenv().get("JVM_PID"));
    UserGroupInformation childUGI = null;

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
          stramChild.init();
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

  public static <T extends Stream> T initStream(Map<String, String> properties, StreamConfiguration streamConf, AbstractNode node)
  {
    String className = properties.get(TopologyBuilder.STREAM_CLASSNAME);
    if (className == null) {
      // should have been caught during submit validation
      throw new IllegalArgumentException(String.format("Stream class not configured (key '%s')", TopologyBuilder.STREAM_CLASSNAME));
    }
    try {
      Class<?> clazz = Class.forName(className);
      Class<? extends Stream> subClass = clazz.asSubclass(Stream.class);
      Constructor<? extends Stream> c = subClass.getConstructor();
      @SuppressWarnings("unchecked")
      T instance = (T) c.newInstance();
      // populate custom properties
      BeanUtils.populate(instance, properties);

      instance.setup(streamConf);

      com.malhartech.dag.StreamContext ctx = new com.malhartech.dag.StreamContext();
      if (node != null) {
        /*
         * no sink for the output streams.
         */
        ctx.setSink(node.getSink(ctx));
      }

      ctx.setSerde(StramUtils.getSerdeInstance(properties));
      instance.setContext(ctx);

      return instance;
    }
    catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Node class not found: " + className, e);
    }
    catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    }
    catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    }
    catch (SecurityException e) {
      throw new IllegalArgumentException("Error creating instance of class: " + className, e);
    }
    catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Constructor with NodeContext not found: " + className, e);
    }
    catch (InstantiationException e) {
      throw new IllegalArgumentException("Failed to instantiate: " + className, e);
    }
  }

  /**
   * Instantiate node from configuration. (happens in the child container, not
   * the stram master process.)
   *
   * @param nodeConf
   * @param conf
   */
  public static AbstractNode initNode(StreamingNodeContext nodeCtx, Configuration conf)
  {
    try {
      Class<? extends AbstractNode> nodeClass = Class.forName(nodeCtx.getDnodeClassName()).asSubclass(AbstractNode.class);
      Constructor<? extends AbstractNode> c = nodeClass.getConstructor(NodeContext.class);
      AbstractNode node = c.newInstance(new NodeContext(nodeCtx.getDnodeId()));
      //DNode node = ReflectionUtils.newInstance(nodeClass, conf);
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
