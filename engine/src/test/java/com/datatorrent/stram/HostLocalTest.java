/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Test;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.PartitionableOperator.Partition;
import com.datatorrent.api.PartitionableOperator.PartitionKeys;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.PartitionableOperator;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.stram.StramChildAgent.ContainerStartRequest;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.plan.PhysicalPlanTest.PartitioningTestOperator;
import com.datatorrent.stram.plan.PhysicalPlanTest.PartitioningTestStreamCodec;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class HostLocalTest {

  public static class PartitioningTestOperator extends GenericTestOperator implements PartitionableOperator {
    
    final public transient InputPort<Object> inportWithCodec = new DefaultInputPort<Object>() {
      @Override
      public Class<? extends StreamCodec<Object>> getStreamCodec() {
        return PartitioningTestStreamCodec.class;
      }

      @Override
      final public void process(Object payload) {
      }

    };

    @Override
    @SuppressWarnings("unchecked")
    public Collection<Partition<?>> definePartitions(Collection<? extends Partition<?>> partitions, int incrementalCapacity) {
      List<Partition<?>> newPartitions = new ArrayList<Partition<?>>(incrementalCapacity+1);
      Partition<PartitioningTestOperator> templatePartition = (Partition<PartitioningTestOperator>)partitions.iterator().next();
      for (int i = 0; i < 1; i++) {
        Partition<PartitioningTestOperator> p = templatePartition.getInstance(new PartitioningTestOperator());
        p.getAttributes().put(OperatorContext.LOCALITY_HOST, "host1");
        newPartitions.add(p);
      }
      for (int i = 1; i < 1 +incrementalCapacity; i++) {
        Partition<PartitioningTestOperator> p = templatePartition.getInstance(new PartitioningTestOperator());
        p.getAttributes().put(OperatorContext.LOCALITY_HOST, "host2");
        newPartitions.add(p);
      }
      return newPartitions;
    }

  }

  @Test
  public void testParitionLocality() {

    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(DAGContext.APPLICATION_PATH, new File("target", HostLocalTest.class.getName()).getAbsolutePath());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);

    GenericTestOperator partitioned = dag.addOperator("partitioned", PartitioningTestOperator.class);
    dag.getMeta(partitioned).getAttributes().put(OperatorContext.INITIAL_PARTITION_COUNT, 2);
    dag.getMeta(partitioned).getAttributes().put(OperatorContext.LOCALITY_HOST, "host1");

    dag.addStream("o1_outport1", o1.outport1, partitioned.inport1);

    StreamingContainerManager scm = new StreamingContainerManager(dag);
   
    ResourceRequestHandler rr = new ResourceRequestHandler();

    int containerMem = 1000;
    Map<String, NodeReport> nodeReports = Maps.newHashMap();
    NodeReport nr = BuilderUtils.newNodeReport(BuilderUtils.newNodeId("host1", 0),
        NodeState.RUNNING, "httpAddress", "rackName", BuilderUtils.newResource(0, 0), BuilderUtils.newResource(containerMem*2, 2), 0, null, 0);
    nodeReports.put(nr.getNodeId().getHost(), nr);
    nr = BuilderUtils.newNodeReport(BuilderUtils.newNodeId("host2", 0),
        NodeState.RUNNING, "httpAddress", "rackName", BuilderUtils.newResource(0, 0), BuilderUtils.newResource(containerMem*2, 2), 0, null, 0);
    nodeReports.put(nr.getNodeId().getHost(), nr);

    // set resources
    rr.updateNodeReports(Lists.newArrayList(nodeReports.values()));

    for (ContainerStartRequest csr : scm.containerStartRequests) {
      String host = rr.getHost(csr, containerMem);
      csr.container.host = host;
      //Assert.assertEquals("Hosts set to host1" , "host1",host);      
    }   

  }
  
  @Test
  public void testNodeLocal() {

    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(DAGContext.APPLICATION_PATH, new File("target", HostLocalTest.class.getName()).getAbsolutePath());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);

    GenericTestOperator partitioned = dag.addOperator("partitioned", GenericTestOperator.class);
    dag.getMeta(partitioned).getAttributes().put(OperatorContext.LOCALITY_HOST, "host1");

    dag.addStream("o1_outport1", o1.outport1, partitioned.inport1).setLocality(Locality.NODE_LOCAL);

    StreamingContainerManager scm = new StreamingContainerManager(dag);
   
    ResourceRequestHandler rr = new ResourceRequestHandler();

    int containerMem = 1000;
    Map<String, NodeReport> nodeReports = Maps.newHashMap();
    NodeReport nr = BuilderUtils.newNodeReport(BuilderUtils.newNodeId("host1", 0),
        NodeState.RUNNING, "httpAddress", "rackName", BuilderUtils.newResource(0, 0), BuilderUtils.newResource(containerMem*2, 2), 0, null, 0);
    nodeReports.put(nr.getNodeId().getHost(), nr);
    nr = BuilderUtils.newNodeReport(BuilderUtils.newNodeId("host2", 0),
        NodeState.RUNNING, "httpAddress", "rackName", BuilderUtils.newResource(0, 0), BuilderUtils.newResource(containerMem*2, 2), 0, null, 0);
    nodeReports.put(nr.getNodeId().getHost(), nr);

    // set resources
    rr.updateNodeReports(Lists.newArrayList(nodeReports.values()));

    for (ContainerStartRequest csr : scm.containerStartRequests) {
      String host = rr.getHost(csr, containerMem);
      csr.container.host = host;
      Assert.assertEquals("Hosts set to host1" , "host1",host);      
    }   

  }
  
  @Test
  public void testContainerLocal() {

    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(DAGContext.APPLICATION_PATH, new File("target", HostLocalTest.class.getName()).getAbsolutePath());

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
        NodeState.RUNNING, "httpAddress", "rackName", BuilderUtils.newResource(0, 0), BuilderUtils.newResource(containerMem*2, 2), 0, null, 0);
    nodeReports.put(nr.getNodeId().getHost(), nr);
    nr = BuilderUtils.newNodeReport(BuilderUtils.newNodeId("host2", 0),
        NodeState.RUNNING, "httpAddress", "rackName", BuilderUtils.newResource(0, 0), BuilderUtils.newResource(containerMem*2, 2), 0, null, 0);
    nodeReports.put(nr.getNodeId().getHost(), nr);

    // set resources
    rr.updateNodeReports(Lists.newArrayList(nodeReports.values()));

    for (ContainerStartRequest csr : scm.containerStartRequests) {
      String host = rr.getHost(csr, containerMem);
      csr.container.host = host;
      Assert.assertEquals("Hosts set to host2" , "host2",host);      
    }   

  }

}
