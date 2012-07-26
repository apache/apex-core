/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.cli;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StramClientUtils {

  public static class YarnClientHelper {
    private static final Logger LOG = LoggerFactory.getLogger(YarnClientHelper.class);

    // Configuration
    final private Configuration conf;

    // RPC to communicate to RM
    final private YarnRPC rpc;
    
    
    public YarnClientHelper(Configuration conf) throws Exception  {
      // Set up the configuration and RPC
      this.conf = conf;
      this.rpc = YarnRPC.create(conf);
    }
 
    public YarnRPC  getYarnRPC() {
      return rpc;
    }

    /**
     * Connect to the Resource Manager/Applications Manager
     * @return Handle to communicate with the ASM
     * @throws IOException 
     */
    public ClientRMProtocol connectToASM() throws IOException {

      /*
      UserGroupInformation user = UserGroupInformation.getCurrentUser();
      applicationsManager = user.doAs(new PrivilegedAction<ClientRMProtocol>() {
        public ClientRMProtocol run() {
          InetSocketAddress rmAddress = NetUtils.createSocketAddr(conf.get(
            YarnConfiguration.RM_SCHEDULER_ADDRESS,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));   
          LOG.info("Connecting to ResourceManager at " + rmAddress);
          Configuration appsManagerServerConf = new Configuration(conf);
          appsManagerServerConf.setClass(YarnConfiguration.YARN_SECURITY_INFO,
          ClientRMSecurityInfo.class, SecurityInfo.class);
          ClientRMProtocol asm = ((ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class, rmAddress, appsManagerServerConf));
          return asm;
        }
      });
       */
      YarnConfiguration yarnConf = new YarnConfiguration(conf);
      InetSocketAddress rmAddress = yarnConf.getSocketAddr(
          YarnConfiguration.RM_ADDRESS,
          YarnConfiguration.DEFAULT_RM_ADDRESS,
          YarnConfiguration.DEFAULT_RM_PORT);
      LOG.info("Connecting to ResourceManager at " + rmAddress);
      return  ((ClientRMProtocol) rpc.getProxy(
          ClientRMProtocol.class, rmAddress, conf));
    }   
    
  }
  
}
