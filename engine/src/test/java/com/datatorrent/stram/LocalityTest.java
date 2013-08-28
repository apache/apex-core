/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import java.io.File;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DAGContext;
import com.datatorrent.stram.PhysicalPlan.PTContainer;
import com.datatorrent.stram.PhysicalPlan.PTOperator;
import com.datatorrent.stram.StramChildAgent.ContainerStartRequest;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class LocalityTest {

  @Test
  public void testNodeLocal() {

    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().attr(DAGContext.APPLICATION_PATH).set(new File("target", LocalityTest.class.getName()).getAbsolutePath());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);

    GenericTestOperator partitioned = dag.addOperator("partitioned", GenericTestOperator.class);
    dag.getMeta(partitioned).getAttributes().attr(OperatorContext.INITIAL_PARTITION_COUNT).set(2);

    GenericTestOperator partitionedParallel = dag.addOperator("partitionedParallel", GenericTestOperator.class);

    dag.addStream("o1_outport1", o1.outport1, partitioned.inport1).setLocality(null);

    dag.addStream("partitioned_outport1", partitioned.outport1, partitionedParallel.inport2).setLocality(Locality.NODE_LOCAL);
    dag.setInputPortAttribute(partitionedParallel.inport2, PortContext.PARTITION_PARALLEL, true);

    GenericTestOperator single = dag.addOperator("single", GenericTestOperator.class);
    dag.addStream("partitionedParallel_outport1", partitionedParallel.outport1, single.inport1);

    int maxContainers = 7;
    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, maxContainers);

    StreamingContainerManager scm = new StreamingContainerManager(dag);
    Assert.assertEquals("number required containers", 7, scm.containerStartRequests.size());

    ResourceRequestHandler rr = new ResourceRequestHandler();

    int containerMem = 1000;
    Map<String, NodeReport> nodeReports = Maps.newHashMap();
    NodeReport nr = BuilderUtils.newNodeReport(BuilderUtils.newNodeId("host1", 0),
    		NodeState.RUNNING, "httpAddress", "rackName", BuilderUtils.newResource(0, 0), BuilderUtils.newResource(containerMem*2, 2), 0, null);
    nodeReports.put(nr.getNodeId().getHost(), nr);
    nr = BuilderUtils.newNodeReport(BuilderUtils.newNodeId("host2", 0),
        NodeState.RUNNING, "httpAddress", "rackName", BuilderUtils.newResource(0, 0), BuilderUtils.newResource(containerMem*2, 2), 0, null);
    nodeReports.put(nr.getNodeId().getHost(), nr);

    // set resources
    rr.updateNodeReports(Lists.newArrayList(nodeReports.values()));

    Map<PTContainer, String> requestedHosts = Maps.newHashMap();
    for (ContainerStartRequest csr : scm.containerStartRequests) {
      String host = rr.getHost(csr, containerMem);
      csr.container.host = host;
      // update the node report
      if (host != null) {
        requestedHosts.put(csr.container, host);
        nr = nodeReports.get(host);
        nr.getUsed().setMemory(nr.getUsed().getMemory() + containerMem);
      }
    }

    Assert.assertEquals("" + requestedHosts, nodeReports.keySet(), Sets.newHashSet(requestedHosts.values()));

    for (Map.Entry<PTContainer, String> e : requestedHosts.entrySet()) {
      for (PTOperator oper : e.getKey().operators) {
        if (oper.getNodeLocalOperators().size() > 1) {
          String expHost = null;
          for (PTOperator nodeLocalOper : oper.getNodeLocalOperators()) {
            Assert.assertNotNull("host null "+nodeLocalOper.container, nodeLocalOper.container.host);
            if (expHost == null) {
              expHost = nodeLocalOper.container.host;
            } else {
              Assert.assertEquals("expected same host " + nodeLocalOper, expHost, nodeLocalOper.container.host);
            }
          }
        }
      }
    }

  }

}
