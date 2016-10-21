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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.AffinityRule;
import com.datatorrent.api.AffinityRule.Type;
import com.datatorrent.api.AffinityRulesSet;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.stram.StreamingContainerAgent.ContainerStartRequest;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.support.StramTestSupport.MemoryStorageAgent;
import com.datatorrent.stram.support.StramTestSupport.TestMeta;

public class AffinityRulesTest
{
  private static final Logger LOG = LoggerFactory.getLogger(AffinityRulesTest.class);

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testOperatorPartitionsAntiAffinity()
  {
    LogicalPlan dag = new LogicalPlan();
    TestGeneratorInputOperator o1 = dag.addOperator("O1", new TestGeneratorInputOperator());
    GenericTestOperator o2 = dag.addOperator("O2", new GenericTestOperator());
    GenericTestOperator o3 = dag.addOperator("O3", new GenericTestOperator());
    dag.addStream("stream1", o1.outport, o2.inport1);
    dag.addStream("stream2", o2.outport1, o3.inport1);

    dag.setOperatorAttribute(o2, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(5));

    AffinityRulesSet ruleSet = new AffinityRulesSet();
    // Valid case:
    List<AffinityRule> rules = new ArrayList<>();
    ruleSet.setAffinityRules(rules);
    AffinityRule rule1 = new AffinityRule(Type.ANTI_AFFINITY, Locality.NODE_LOCAL, false, "O2", "O2");
    rules.add(rule1);
    dag.setAttribute(DAGContext.AFFINITY_RULES_SET, ruleSet);
    dag.validate();
    dag.getAttributes().put(com.datatorrent.api.Context.DAGContext.APPLICATION_PATH, testMeta.getAbsolutePath());
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    StreamingContainerManager scm = new StreamingContainerManager(dag);

    // Check Physical Plan assigns anti-affinity preferences correctly

    for (ContainerStartRequest csr : scm.containerStartRequests) {
      PTContainer container = csr.container;
      // Check that for containers for O2 partitions, anti-affinity Preference
      // are set properly

      if (container.getOperators().get(0).getName().equals("O2")) {
        Assert.assertEquals("Anti-affinity containers set should have 4 containers for other partitions ", 4, container.getStrictAntiPrefs().size());
        for (PTContainer c : container.getStrictAntiPrefs()) {
          for (PTOperator operator : c.getOperators()) {
            Assert.assertEquals("Partion for O2 should be Anti Prefs", "O2", operator.getName());
          }
        }
      }
    }

    // Check resource handler assigns different hosts for each partition

    ResourceRequestHandler rr = new ResourceRequestHandler();
    int containerMem = 1000;
    Map<String, NodeReport> nodeReports = Maps.newHashMap();
    for (int i = 0; i < 10; i++) {
      String hostName = "host" + i;
      NodeReport nr = BuilderUtils.newNodeReport(BuilderUtils.newNodeId(hostName, 0), NodeState.RUNNING, "httpAddress", "rackName", BuilderUtils.newResource(0, 0), BuilderUtils.newResource(containerMem * 2, 2), 0, null, 0);
      nodeReports.put(nr.getNodeId().getHost(), nr);
    }

    // set resources
    rr.updateNodeReports(Lists.newArrayList(nodeReports.values()));

    Set<String> partitionHostNames = new HashSet<>();
    for (ContainerStartRequest csr : scm.containerStartRequests) {
      String host = rr.getHost(csr, true);
      csr.container.host = host;
      if (csr.container.getOperators().get(0).getName().equals("O2")) {
        Assert.assertNotNull("Host name should not be null", host);
        LOG.info("Partition {} for operator O2 has host = {} ", csr.container.getId(), host);
        Assert.assertTrue("Each Partition should have a different host", !partitionHostNames.contains(host));
        partitionHostNames.add(host);
      }
    }
  }

  @Test
  public void testAntiAffinityInOperators()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(com.datatorrent.api.Context.DAGContext.APPLICATION_PATH, testMeta.getAbsolutePath());
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("O1", GenericTestOperator.class);
    dag.setOperatorAttribute(o1, OperatorContext.MEMORY_MB, 256);

    GenericTestOperator o2 = dag.addOperator("O2", GenericTestOperator.class);
    dag.setOperatorAttribute(o2, OperatorContext.MEMORY_MB, 256);

    dag.getMeta(o1).getAttributes().put(OperatorContext.LOCALITY_HOST, "host1");
    AffinityRulesSet ruleSet = new AffinityRulesSet();

    List<AffinityRule> rules = new ArrayList<>();
    ruleSet.setAffinityRules(rules);
    AffinityRule rule1 = new AffinityRule(Type.ANTI_AFFINITY, Locality.NODE_LOCAL, false, "O1", "O2");
    rules.add(rule1);
    dag.setAttribute(DAGContext.AFFINITY_RULES_SET, ruleSet);

    dag.addStream("o1_outport1", o1.outport1, o2.inport1);// .setLocality(Locality.NODE_LOCAL);

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

    for (ContainerStartRequest csr : scm.containerStartRequests) {
      String host = rr.getHost(csr, true);
      csr.container.host = host;
      if (csr.container.getOperators().get(0).getName().equals("O1")) {
        Assert.assertEquals("Hosts set to host1 for Operator O1", "host1", host);
      }
      if (csr.container.getOperators().get(0).getName().equals("O2")) {
        Assert.assertEquals("Hosts set to host2 for Operator O2", "host2", host);
      }
    }
  }
}
