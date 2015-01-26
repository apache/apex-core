/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;


import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;

import com.datatorrent.lib.partitioner.StatelessPartitioner;
import com.datatorrent.stram.StreamingContainerAgent.ContainerStartRequest;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.support.StramTestSupport.MemoryStorageAgent;

public class HostLocalTest
{
  private static class LocalityPartitioner extends StatelessPartitioner<GenericTestOperator>
  {
    private static final long serialVersionUID = 1L;

    @Override
    public Collection<Partition<GenericTestOperator>> definePartitions(Collection<Partition<GenericTestOperator>> partitions, PartitioningContext context)
    {
      Collection<Partition<GenericTestOperator>> newPartitions = super.definePartitions(partitions, context);
      Iterator<Partition<GenericTestOperator>> it = newPartitions.iterator();
      for (int i=0; i<newPartitions.size() && it.hasNext(); i++) {
        it.next().getAttributes().put(OperatorContext.LOCALITY_HOST, "host" + (i + 1));
      }
      return newPartitions;
    }
  }

  @Test
  public void testPartitionLocality()
  {
    int partitionCount = 3;
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(com.datatorrent.api.Context.DAGContext.APPLICATION_PATH, new File("target", HostLocalTest.class.getName()).getAbsolutePath());
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);

    GenericTestOperator partitioned = dag.addOperator("partitioned", GenericTestOperator.class);
    LocalityPartitioner partitioner = new LocalityPartitioner();
    partitioner.setPartitionCount(partitionCount);
    dag.getMeta(partitioned).getAttributes().put(OperatorContext.PARTITIONER, partitioner);
    dag.addStream("o1_outport1", o1.outport1, partitioned.inport1);

    StreamingContainerManager scm = new StreamingContainerManager(dag);

    ResourceRequestHandler rr = new ResourceRequestHandler();

    int containerMem = 1000;
    Map<String, NodeReport> nodeReports = Maps.newHashMap();
    for(int i = 0; i< partitionCount; i++) {
      NodeReport nr = BuilderUtils.newNodeReport(BuilderUtils.newNodeId("host" + (i+1), 0),
        NodeState.RUNNING, "httpAddress", "rackName", BuilderUtils.newResource(0, 0), BuilderUtils.newResource(containerMem * 2, 2), 0, null, 0);
      nodeReports.put(nr.getNodeId().getHost(), nr);
    }

    // set resources
    rr.updateNodeReports(Lists.newArrayList(nodeReports.values()));
    Set<String> expectedHosts = Sets.newHashSet();
    for(int i =0; i< partitionCount; i++){
      expectedHosts.add("host"+(i+1));
    }
    for (ContainerStartRequest csr : scm.containerStartRequests) {
      String host = rr.getHost(csr, containerMem, true);
      if(host != null){
        expectedHosts.remove(host);
      }
    }
    Assert.assertTrue("All the allocated hosts removed", expectedHosts.isEmpty());

  }

  @Test
  public void testNodeLocal()
  {

    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(com.datatorrent.api.Context.DAGContext.APPLICATION_PATH, new File("target", HostLocalTest.class.getName()).getAbsolutePath());
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());


    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);

    GenericTestOperator partitioned = dag.addOperator("partitioned", GenericTestOperator.class);
    dag.getMeta(partitioned).getAttributes().put(OperatorContext.LOCALITY_HOST, "host1");

    dag.addStream("o1_outport1", o1.outport1, partitioned.inport1).setLocality(Locality.NODE_LOCAL);

    StreamingContainerManager scm = new StreamingContainerManager(dag);

    ResourceRequestHandler rr = new ResourceRequestHandler();

    int containerMem = 1000;
    Map<String, NodeReport> nodeReports = Maps.newHashMap();
    NodeReport nr = BuilderUtils.newNodeReport(BuilderUtils.newNodeId("host1", 0),
                                               NodeState.RUNNING, "httpAddress", "rackName", BuilderUtils.newResource(0, 0), BuilderUtils.newResource(containerMem * 2, 2), 0, null, 0);
    nodeReports.put(nr.getNodeId().getHost(), nr);
    nr = BuilderUtils.newNodeReport(BuilderUtils.newNodeId("host2", 0),
                                    NodeState.RUNNING, "httpAddress", "rackName", BuilderUtils.newResource(0, 0), BuilderUtils.newResource(containerMem * 2, 2), 0, null, 0);
    nodeReports.put(nr.getNodeId().getHost(), nr);

    // set resources
    rr.updateNodeReports(Lists.newArrayList(nodeReports.values()));

    for (ContainerStartRequest csr : scm.containerStartRequests) {
      String host = rr.getHost(csr, containerMem, true);
      csr.container.host = host;
      Assert.assertEquals("Hosts set to host1", "host1", host);
    }

  }

  @Test
  public void testContainerLocal()
  {

    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(com.datatorrent.api.Context.DAGContext.APPLICATION_PATH, new File("target", HostLocalTest.class.getName()).getAbsolutePath());
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    dag.getMeta(o1).getAttributes().put(OperatorContext.LOCALITY_HOST, "host2");

    GenericTestOperator partitioned = dag.addOperator("partitioned", GenericTestOperator.class);
    //dag.getMeta(partitioned).getAttributes().put(OperatorContext.LOCALITY_HOST, "host2");

    dag.addStream("o1_outport1", o1.outport1, partitioned.inport1).setLocality(Locality.CONTAINER_LOCAL);

    StreamingContainerManager scm = new StreamingContainerManager(dag);

    ResourceRequestHandler rr = new ResourceRequestHandler();

    int containerMem = 1000;
    Map<String, NodeReport> nodeReports = Maps.newHashMap();
    NodeReport nr = BuilderUtils.newNodeReport(BuilderUtils.newNodeId("host1", 0),
                                               NodeState.RUNNING, "httpAddress", "rackName", BuilderUtils.newResource(0, 0), BuilderUtils.newResource(containerMem * 2, 2), 0, null, 0);
    nodeReports.put(nr.getNodeId().getHost(), nr);
    nr = BuilderUtils.newNodeReport(BuilderUtils.newNodeId("host2", 0),
                                    NodeState.RUNNING, "httpAddress", "rackName", BuilderUtils.newResource(0, 0), BuilderUtils.newResource(containerMem * 2, 2), 0, null, 0);
    nodeReports.put(nr.getNodeId().getHost(), nr);

    // set resources
    rr.updateNodeReports(Lists.newArrayList(nodeReports.values()));

    for (ContainerStartRequest csr : scm.containerStartRequests) {
      String host = rr.getHost(csr, containerMem, true);
      csr.container.host = host;
      Assert.assertEquals("Hosts set to host2", "host2", host);
    }

  }

}
