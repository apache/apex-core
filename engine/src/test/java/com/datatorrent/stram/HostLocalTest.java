/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.common.partitioner.StatelessPartitioner;
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
      for (int i = 0; i < newPartitions.size() && it.hasNext(); i++) {
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
    for (int i = 0; i < partitionCount; i++) {
      NodeReport nr = BuilderUtils.newNodeReport(BuilderUtils.newNodeId("host" + (i + 1), 0),
          NodeState.RUNNING, "httpAddress", "rackName", BuilderUtils.newResource(0, 0), BuilderUtils.newResource(containerMem * 2, 2), 0, null, 0);
      nodeReports.put(nr.getNodeId().getHost(), nr);
    }

    // set resources
    rr.updateNodeReports(Lists.newArrayList(nodeReports.values()));
    Set<String> expectedHosts = Sets.newHashSet();
    for (int i = 0; i < partitionCount; i++) {
      expectedHosts.add("host" + (i + 1));
    }
    for (ContainerStartRequest csr : scm.containerStartRequests) {
      String host = rr.getHost(csr, true);
      if (host != null) {
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
    dag.setOperatorAttribute(o1,OperatorContext.MEMORY_MB,256);

    GenericTestOperator partitioned = dag.addOperator("partitioned", GenericTestOperator.class);
    dag.setOperatorAttribute(partitioned,OperatorContext.MEMORY_MB,256);
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
      String host = rr.getHost(csr, true);
      csr.container.host = host;
      Assert.assertEquals("Hosts set to host1", "host1", host);
    }

  }

  @Test
  public void testThreadLocal()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(com.datatorrent.api.Context.DAGContext.APPLICATION_PATH, new File("target", HostLocalTest.class.getName()).getAbsolutePath());
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    dag.getMeta(o1).getAttributes().put(OperatorContext.LOCALITY_HOST, "host2");

    GenericTestOperator partitioned = dag.addOperator("partitioned", GenericTestOperator.class);
    dag.addStream("o1_outport1", o1.outport1, partitioned.inport1).setLocality(Locality.THREAD_LOCAL);
    dag.setOperatorAttribute(o1,OperatorContext.MEMORY_MB,256);
    dag.setOperatorAttribute(partitioned,OperatorContext.MEMORY_MB,256);

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
    Assert.assertEquals("number of containers is 1", 1, scm.containerStartRequests.size());
    for (ContainerStartRequest csr : scm.containerStartRequests) {
      String host = rr.getHost(csr, true);
      csr.container.host = host;
      Assert.assertEquals("Hosts set to host2", "host2", host);
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
    dag.addStream("o1_outport1", o1.outport1, partitioned.inport1).setLocality(Locality.CONTAINER_LOCAL);
    dag.setOperatorAttribute(o1, OperatorContext.MEMORY_MB, 256);
    dag.setOperatorAttribute(partitioned,OperatorContext.MEMORY_MB,256);

    StreamingContainerManager scm = new StreamingContainerManager(dag);

    ResourceRequestHandler rr = new ResourceRequestHandler();

    int containerMem = 1000;
    Map<String, NodeReport> nodeReports = Maps.newHashMap();
    NodeReport nr = BuilderUtils.newNodeReport(BuilderUtils.newNodeId("host1", 0), NodeState.RUNNING, "httpAddress", "rackName", BuilderUtils.newResource(0, 0), BuilderUtils.newResource(containerMem * 2, 2), 0, null, 0);
    nodeReports.put(nr.getNodeId().getHost(), nr);
    nr = BuilderUtils.newNodeReport(BuilderUtils.newNodeId("host2", 0), NodeState.RUNNING, "httpAddress", "rackName", BuilderUtils.newResource(0, 0), BuilderUtils.newResource(containerMem * 2, 2), 0, null, 0);
    nodeReports.put(nr.getNodeId().getHost(), nr);

    // set resources
    rr.updateNodeReports(Lists.newArrayList(nodeReports.values()));
    Assert.assertEquals("number of containers is 1", 1, scm.containerStartRequests.size());
    for (ContainerStartRequest csr : scm.containerStartRequests) {
      String host = rr.getHost(csr, true);
      csr.container.host = host;
      Assert.assertEquals("Hosts set to host2", "host2", host);
    }
  }

  @Test
  public void testContainerLocalWithVCores()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(com.datatorrent.api.Context.DAGContext.APPLICATION_PATH, new File("target", HostLocalTest.class.getName()).getAbsolutePath());
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    dag.getMeta(o1).getAttributes().put(OperatorContext.LOCALITY_HOST, "host2");

    GenericTestOperator partitioned = dag.addOperator("partitioned", GenericTestOperator.class);
    dag.addStream("o1_outport1", o1.outport1, partitioned.inport1).setLocality(Locality.CONTAINER_LOCAL);
    dag.setOperatorAttribute(o1,OperatorContext.MEMORY_MB,256);
    dag.setOperatorAttribute(o1,OperatorContext.VCORES,1);
    dag.setOperatorAttribute(partitioned,OperatorContext.VCORES,1);

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
    Assert.assertEquals("number of containers is 1", 1, scm.containerStartRequests.size());
    for (ContainerStartRequest csr : scm.containerStartRequests) {
      String host = rr.getHost(csr, true);
      csr.container.host = host;
      Assert.assertEquals("number of vcores", 2, csr.container.getRequiredVCores());
      Assert.assertEquals("Hosts set to host2", "host2", host);
    }
  }

  @Test
  public void testUnavailableResources()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(com.datatorrent.api.Context.DAGContext.APPLICATION_PATH, new File("target", HostLocalTest.class.getName()).getAbsolutePath());
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    dag.getMeta(o1).getAttributes().put(OperatorContext.LOCALITY_HOST, "host2");

    GenericTestOperator partitioned = dag.addOperator("partitioned", GenericTestOperator.class);
    dag.addStream("o1_outport1", o1.outport1, partitioned.inport1).setLocality(Locality.CONTAINER_LOCAL);
    dag.setOperatorAttribute(o1, OperatorContext.MEMORY_MB, 256);
    dag.setOperatorAttribute(o1, OperatorContext.VCORES, 2);
    dag.setOperatorAttribute(partitioned, OperatorContext.VCORES, 1);

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
    Assert.assertEquals("number of containers is 1", 1, scm.containerStartRequests.size());
    for (ContainerStartRequest csr : scm.containerStartRequests) {
      String host = rr.getHost(csr, true);
      Assert.assertEquals("number of vcores", 3, csr.container.getRequiredVCores());
      Assert.assertNull("Host is null", host);
    }
  }
}
