/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.malhartech.dag.InputAdapter;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest.RequestType;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;
import com.malhartech.stram.conf.TopologyBuilder.StreamConf;
import com.malhartech.stream.SocketStreamTest.TestChildContainer;

/**
 *
 */
public class CheckpointTest {

  private static File testWorkDir = new File("target", CheckpointTest.class.getName());
  
  @BeforeClass
  public static void setup() {
    try {
      FileContext.getLocalFSFileContext().delete(
          new Path(testWorkDir.getAbsolutePath()), true);
    } catch (Exception e) {
      throw new RuntimeException("could not cleanup test dir", e);
    }     
  }
  
  @Test
  public void testAdapterWrapperNodeInit() throws Exception {
    AdapterWrapperNode wn = new AdapterWrapperNode();
    Map<String, String> properties = new HashMap<String, String>();
    properties.put(AdapterWrapperNode.KEY_IS_INPUT, "true");
    BeanUtils.populate(wn, properties);
    Assert.assertTrue(wn.isInput());
  }  
  
  /**
   * Instantiate physical model with adapters and partitioning in mock container.
   *
   * @throws Exception
   */
  @Test
  public void testBackup() throws Exception
  {
    TopologyBuilder b = new TopologyBuilder(new Configuration());

    NodeConf node1 = b.getOrAddNode("node1");

    StreamConf input1 = b.getOrAddStream("input1");
    input1.addProperty(TopologyBuilder.STREAM_CLASSNAME,
                       NumberGeneratorInputAdapter.class.getName());
    input1.addProperty(TopologyBuilder.STREAM_INLINE, "true");
    input1.addProperty("maxTuples", "1");
    
    node1.addInput(input1);

    for (NodeConf nodeConf : b.getAllNodes().values()) {
      nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }

    DNodeManager dnm = new DNodeManager(b);
    Assert.assertEquals("number required containers", 1, dnm.getNumRequiredContainers());

    String containerId = "container1";
    StreamingContainerContext cc = dnm.assignContainer(containerId, InetSocketAddress.createUnresolved("localhost", 0));
    TestChildContainer container = new TestChildContainer(containerId);
    cc.setWindowSizeMillis(0); // disable window generator
    cc.setCheckpointDfsPath(testWorkDir.getPath());
    container.init(cc);

    List<InputAdapter> inputAdapters = container.getInputAdapters();
    Assert.assertEquals("number input adapters", 1, inputAdapters.size());

    InputAdapter input = inputAdapters.get(0);
    input.resetWindow(0, 1);
    input.beginWindow(1);

    StramToNodeRequest backupRequest = new StramToNodeRequest();
    backupRequest.setNodeId(cc.getNodes().get(0).getDnodeId());
    backupRequest.setRequestType(RequestType.CHECKPOINT);
    ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
    rsp.setNodeRequests(Collections.singletonList(backupRequest));
    // TODO: ensure node is running and context set (startup timing)
    container.processHeartbeatResponse(rsp);
    
    input.endWindow(1);
 
    container.shutdown();

    File expectedFile = new File(testWorkDir, cc.getNodes().get(0).getDnodeId());
    Assert.assertTrue("checkpoint file exists: " + expectedFile, expectedFile.exists());
    
  }
  
}
