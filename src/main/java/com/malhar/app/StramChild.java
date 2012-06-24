package com.malhar.app;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

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

import com.malhar.app.StreamingNodeUmbilicalProtocol.StreamingNodeContext;
import com.malhar.node.DNode;

/**
 * The main() for streaming node processes launched by {@link com.malhar.app.StramAppMaster}.
 */
public class StramChild {

  private static Logger LOG = LoggerFactory.getLogger(StramChild.class);
  
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
          StreamingNodeContext streamingNodeCtx = umbilical.getNodeContext(childId);
          LOG.info("Got context: " + streamingNodeCtx);
          initNode(streamingNodeCtx, defaultConf);
          // TODO: run node in doAs block
          umbilical.echo(childId, "[" + childId + "] Nothing to do as of yet!");
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
      Class<? extends DNode> nodeClass = Class.forName(nodeCtx.dnodeClassName).asSubclass(DNode.class);    
      DNode node = ReflectionUtils.newInstance(nodeClass, conf);
      // populate the custom properties
      BeanUtils.populate(node, nodeCtx.properties);
      return node;
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Node class not found: " + nodeCtx.dnodeClassName, e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    }
  }
  
}
