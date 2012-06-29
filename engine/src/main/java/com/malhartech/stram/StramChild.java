package com.malhartech.stram;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
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
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.dag.DNode;
import com.malhartech.dag.DNode.HeartbeatCounters;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeat;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamContext;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingNodeContext;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingNodeHeartbeat;

/**
 * The main() for streaming node processes launched by {@link com.malhartech.stram.StramAppMaster}.
 */
public class StramChild {

  private static Logger LOG = LoggerFactory.getLogger(StramChild.class);

  final private String containerId;
  final private Configuration conf;
  final private StreamingNodeUmbilicalProtocol umbilical;
  final private Map<String, DNode> nodeList = new ConcurrentHashMap<String, DNode>();
  private long heartbeatIntervalMillis = 1000;
  private boolean exitHeartbeatLoop = false;
  
  protected StramChild(String containerId, Configuration conf, StreamingNodeUmbilicalProtocol umbilical) {
    this.umbilical = umbilical;
    this.containerId = containerId;
    this.conf = conf;
  }
  
  private void init() throws IOException {
    StreamingContainerContext ctx = umbilical.getInitContext(containerId);
    LOG.info("Got context: " + ctx);

    this.heartbeatIntervalMillis = ctx.getHeartbeatIntervalMillis();
    
    // create nodes
    for (StreamingNodeContext snc : ctx.getNodes()) {
        DNode dnode = initNode(snc, conf);
        nodeList.put(snc.getDnodeId(), dnode);
    }
    
    // wire nodes (inline streams and buffer server connections
    // TODO: this looks too complicated
    for (StreamContext sc : ctx.getStreams()) {
        if (sc.isInline()) {
          DNode source = nodeList.get(sc.getSourceNodeId());
          DNode target = nodeList.get(sc.getTargetNodeId());

          LOG.info("inline connection from {} to {}", source, target);
          // TODO: link nodes directly via blocking queue
        } else {
          if (sc.getSourceNodeId() != null) {
            DNode sourceNode = nodeList.get(sc.getSourceNodeId());
            if (sourceNode != null) {
              LOG.info("Node {} is buffer server publisher for stream {}", sourceNode, sc.getId());
            }
          }
          
          if (sc.getTargetNodeId() != null) {
            DNode targetNode = nodeList.get(sc.getTargetNodeId());
            if (targetNode != null) {
              LOG.info("Node {} is buffer server subscriber for stream {}", targetNode, sc.getId());
            }
          }
        }
    }
  }

  private void heartbeatLoop() throws IOException {
    umbilical.echo(containerId, "[" + containerId + "] Entering heartbeat loop..");
    LOG.info("Entering hearbeat loop");
    while (!exitHeartbeatLoop) {
      
      try {
        Thread.sleep(heartbeatIntervalMillis);
      } catch (InterruptedException e1) {
        LOG.warn("Interrupted in heartbeat loop, exiting..");
        break;
      }
    
      long currentTime = System.currentTimeMillis();
      ContainerHeartbeat msg = new ContainerHeartbeat();
      msg.setContainerId(this.containerId);
      List<StreamingNodeHeartbeat> heartbeats = new ArrayList<StreamingNodeHeartbeat>(nodeList.size());
  
      // gather heartbeat info for all nodes
      for (Map.Entry<String, DNode> e : nodeList.entrySet()) {
        StreamingNodeHeartbeat hb = new StreamingNodeHeartbeat();
        HeartbeatCounters counters = e.getValue().getResetCounters();
        hb.setNodeId(e.getKey());
        hb.setGeneratedTms(currentTime);
        hb.setNumberTuplesProcessed((int)counters.tuplesProcessed);
        hb.setIntervalMs(heartbeatIntervalMillis);
        hb.setState(e.getValue().getState().name());
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

  private void processHeartbeatResponse(ContainerHeartbeatResponse rsp) {
    if (rsp.isShutdown()) {
      LOG.info("Received shutdown request");
      this.exitHeartbeatLoop = true;
      return;
    }
    if (rsp.getNodeRequests() != null) {
      // extended processing per node
      for (StramToNodeRequest req : rsp.getNodeRequests()) {
        DNode n = nodeList.get(req.getNodeId());
        if (n == null) {
          LOG.warn("Received request with invalid node id {} ({})", req.getNodeId(), req);
        } else {
          LOG.info("Stram request: {}", req);
          processStramRequest(n, req);
        }
      }
    }
  }

  /**
   * Process request from stram for further communication through the protocol.
   * Extended reporting is on a per node basis (won't occur under regular operation)
   * @param n
   * @param snr
   */
  private void processStramRequest(DNode n, StramToNodeRequest snr) {
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
  
  public static void main(String[] args) throws Throwable {
    LOG.debug("Child starting");

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
      taskOwner.doAs(new PrivilegedExceptionAction<StreamingNodeUmbilicalProtocol>() {
      @Override
      public StreamingNodeUmbilicalProtocol run() throws Exception {
        return (StreamingNodeUmbilicalProtocol)RPC.getProxy(StreamingNodeUmbilicalProtocol.class,
            StreamingNodeUmbilicalProtocol.versionID, address, defaultConf);
      }
    });

    LOG.debug("PID: " + System.getenv().get("JVM_PID"));
    UserGroupInformation childUGI = null;

    try {
      childUGI = UserGroupInformation.createRemoteUser(System
          .getenv(ApplicationConstants.Environment.USER.toString()));
      // Add tokens to new user so that it may execute its task correctly.
      for(Token<?> token : UserGroupInformation.getCurrentUser().getTokens()) {
        childUGI.addToken(token);
      }

      // TODO: run node in doAs block
      childUGI.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          StramChild stramChild = new StramChild(childId, defaultConf, umbilical);
          stramChild.init();
          // main thread enters heartbeat loop
          stramChild.heartbeatLoop();
          
          return null;
        }
      });
    } catch (FSError e) {
      LOG.error("FSError from child", e);
      umbilical.echo(childId, e.getMessage());
    } catch (Exception exception) {
      LOG.warn("Exception running child : "
          + StringUtils.stringifyException(exception));
      // Report back any failures, for diagnostic purposes
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      exception.printStackTrace(new PrintStream(baos));
      umbilical.echo(childId, "FATAL: " + baos.toString());
    } catch (Throwable throwable) {
      LOG.error("Error running child : "
    	        + StringUtils.stringifyException(throwable));
        Throwable tCause = throwable.getCause();
        String cause = tCause == null
                                 ? throwable.getMessage()
                                 : StringUtils.stringifyException(tCause);
        umbilical.echo(childId, cause);
    } finally {
      RPC.stopProxy(umbilical);
      DefaultMetricsSystem.shutdown();
      // Shutting down log4j of the child-vm...
      // This assumes that on return from Task.run()
      // there is no more logging done.
      LogManager.shutdown();
    }
  }
  
  /**
   * TODO: Move to Stram initialization
   * Instantiate node from configuration. 
   * (happens in the execution container, not the stram master process.)
   * @param nodeConf
   * @param conf
   */
  public static DNode initNode(StreamingNodeContext nodeCtx, Configuration conf) {
    try {
      Class<? extends DNode> nodeClass = Class.forName(nodeCtx.getDnodeClassName()).asSubclass(DNode.class);    
      DNode node = ReflectionUtils.newInstance(nodeClass, conf);
      node.setId(nodeCtx.getDnodeId());
      // populate the custom properties
      BeanUtils.populate(node, nodeCtx.getProperties());
      return node;
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Node class not found: " + nodeCtx.getDnodeClassName(), e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    }
  }
  
}
