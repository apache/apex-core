package com.malhar.app;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAllApplicationsRequestPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Run as:
 * hadoop jar stramproto-0.1-SNAPSHOT.jar com.malhar.app.StramClient --jar stramproto-0.1-SNAPSHOT.jar --shell_command date --num_containers 1
 */
public class StramClient2 {
	private static Logger LOG = LoggerFactory.getLogger(StramClient2.class);

	/**
	 * @param args
	 */
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// see mapreduce ResourceMgrDelegate
		
		// needs yarn-site.xml in classpath
		
		ClientRMProtocol applicationsManager; 
	    YarnConfiguration yarnConf = new YarnConfiguration();
	    InetSocketAddress rmAddress = 
	        NetUtils.createSocketAddr(
	        		yarnConf.get(YarnConfiguration.RM_ADDRESS,YarnConfiguration.DEFAULT_RM_ADDRESS) 
	            		//"192.168.1.4:8032"
	            		//"192.168.1.13:58012"
	        		    /*"localhost:58012"*/
	        		);             
	    LOG.info("Connecting to ResourceManager at " + rmAddress);

	    Configuration appsManagerServerConf = new Configuration(yarnConf);
	   // appsManagerServerConf.setClass(
	   //     YarnConfiguration.YARN_SECURITY_INFO,
	   //     ClientRMSecurityInfo.class, SecurityInfo.class);

	    YarnRPC rpc = YarnRPC.create(yarnConf);	    
	    applicationsManager = ((ClientRMProtocol) rpc.getProxy(
	        ClientRMProtocol.class, rmAddress, appsManagerServerConf));    		

	    
	    LOG.info(applicationsManager.toString());
        // get the list of applications from the RM
	    GetAllApplicationsRequest req = new GetAllApplicationsRequestPBImpl(); 
	    GetAllApplicationsResponse rsp = applicationsManager.getAllApplications(req);
	    LOG.info("GetAllApplicationsResponse.getApplicationList: {}", rsp.getApplicationList());
	    

	}

}
