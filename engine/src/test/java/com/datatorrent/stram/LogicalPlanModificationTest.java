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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.FutureTask;

import javax.validation.ValidationException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Sets;

import com.datatorrent.api.AffinityRule;
import com.datatorrent.api.AffinityRule.Type;
import com.datatorrent.api.AffinityRulesSet;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.common.util.FSStorageAgent;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.TestPlanContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.requests.CreateOperatorRequest;
import com.datatorrent.stram.plan.logical.requests.LogicalPlanRequest;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.plan.physical.PhysicalPlanTest;
import com.datatorrent.stram.plan.physical.PlanModifier;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.TestMeta;


public class LogicalPlanModificationTest
{
  private LogicalPlan dag;

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Before
  public void setup()
  {
    dag = StramTestSupport.createDAG(testMeta);
  }

  @Test
  public void testAddOperator()
  {
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1);
    dag.addStream("o2.outport1", o2.outport1, o3.inport1);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    ctx.deploy.clear();
    ctx.undeploy.clear();

    Assert.assertEquals("containers", 3, plan.getContainers().size());

    PlanModifier pm = new PlanModifier(plan);
    GenericTestOperator added1 = new GenericTestOperator();
    pm.addOperator("added1", added1);

    pm.addStream("added1.outport1", added1.outport1, o3.inport2);

    Assert.assertEquals("undeploy " + ctx.undeploy, 0, ctx.undeploy.size());
    Assert.assertEquals("deploy " + ctx.deploy, 0, ctx.deploy.size());

    pm.applyChanges(ctx);

    Assert.assertEquals("containers post change", 4, plan.getContainers().size());

    Assert.assertEquals("undeploy " + ctx.undeploy, 1, ctx.undeploy.size());
    Assert.assertEquals("deploy " + ctx.deploy, 2, ctx.deploy.size());

  }

  @Test
  public void testAddOperatorWithAffinityRules()
  {
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1);
    dag.addStream("o2.outport1", o2.outport1, o3.inport1);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    ctx.deploy.clear();
    ctx.undeploy.clear();

    Assert.assertEquals("containers", 3, plan.getContainers().size());

    AffinityRulesSet ruleSet = new AffinityRulesSet();
    List<AffinityRule> rules = new ArrayList<>();
    ruleSet.setAffinityRules(rules);
    rules.add(new AffinityRule(Type.AFFINITY, Locality.CONTAINER_LOCAL, false, "o1", "added1"));
    rules.add(new AffinityRule(Type.ANTI_AFFINITY, Locality.NODE_LOCAL, false, "o3", "added1"));
    dag.setAttribute(DAGContext.AFFINITY_RULES_SET, ruleSet);

    PlanModifier pm = new PlanModifier(plan);
    GenericTestOperator added1 = new GenericTestOperator();
    pm.addOperator("added1", added1);

    pm.addStream("added1.outport1", added1.outport1, o3.inport2);

    Assert.assertEquals("undeploy " + ctx.undeploy, 0, ctx.undeploy.size());
    Assert.assertEquals("deploy " + ctx.deploy, 0, ctx.deploy.size());

    pm.applyChanges(ctx);

    Assert.assertEquals("containers post change", 4, plan.getContainers().size());

    Assert.assertEquals("undeploy " + ctx.undeploy, 1, ctx.undeploy.size());
    Assert.assertEquals("deploy " + ctx.deploy, 2, ctx.deploy.size());

    // Validate affinity rules are applied
    for (PTContainer c : plan.getContainers()) {
      if (c.getOperators().contains("added1")) {
        Assert.assertEquals("Operators O1 and added1 should be in the same container as per affinity rule", 2, c.getOperators().size());
        Assert.assertEquals("Operators O1 and added1 should be in the same container as per affinity rule", "o1", c.getOperators().get(0).getOperatorMeta().getName());
        Assert.assertEquals("Operators O1 and added1 should be in the same container as per affinity rule", "added1", c.getOperators().get(1).getOperatorMeta().getName());

        Set<PTContainer> antiAffinityList = c.getStrictAntiPrefs();
        Assert.assertEquals("There should be one container in antiaffinity list", 1, antiAffinityList.size());
        List<PTOperator> antiAffinityOperators = antiAffinityList.iterator().next().getOperators();
        Assert.assertEquals("AntiAffinity operators should containn operator O3", antiAffinityOperators.iterator().next().getOperatorMeta().getName(), "o3");
      }
    }
  }

  @Test
  public void testSetOperatorProperty()
  {
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    OperatorMeta o1Meta = dag.getMeta(o1);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    ctx.deploy.clear();
    ctx.undeploy.clear();

    PlanModifier pm = new PlanModifier(plan);
    try {
      pm.setOperatorProperty(o1Meta.getName(), "myStringProperty", "propertyValue");
      Assert.fail("validation error exepected");
    } catch (javax.validation.ValidationException e) {
      Assert.assertTrue(e.getMessage().contains(o1Meta.toString()));
    }

    GenericTestOperator newOperator = new GenericTestOperator();
    pm.addOperator("newOperator", newOperator);
    pm.setOperatorProperty("newOperator", "myStringProperty", "propertyValue");
    Assert.assertEquals("", "propertyValue", newOperator.getMyStringProperty());
  }

  @Test
  public void testRemoveOperator()
  {
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    OperatorMeta o1Meta = dag.getMeta(o1);
    GenericTestOperator o12 = dag.addOperator("o12", GenericTestOperator.class);
    OperatorMeta o12Meta = dag.getMeta(o12);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    OperatorMeta o2Meta = dag.getMeta(o2);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);
    OperatorMeta o3Meta = dag.getMeta(o3);

    LogicalPlan.StreamMeta s1 = dag.addStream("o1.outport1", o1.outport1, o2.inport1, o12.inport1);
    LogicalPlan.StreamMeta s2 = dag.addStream("o2.outport1", o2.outport1, o3.inport1);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    ctx.deploy.clear();
    ctx.undeploy.clear();
    Assert.assertEquals("containers " + plan.getContainers(), 4, plan.getContainers().size());
    Assert.assertEquals("physical operators " + plan.getAllOperators(), 4, plan.getAllOperators().size());
    Assert.assertEquals("sinks s1 " + s1.getSinks(), 2, s1.getSinks().size());

    List<PTOperator> o2PhysicalOpers = plan.getOperators(o2Meta);
    Assert.assertEquals("instances " + o2Meta, 1, o2PhysicalOpers.size());
    PlanModifier pm = new PlanModifier(plan);

    try {
      pm.removeOperator(o2Meta.getName());
      Assert.fail("validation error (connected output stream) expected");
    } catch (ValidationException ve) {
      // all good
    }

    // remove output stream required before removing operator
    pm.removeStream(s2.getName());

    pm.removeOperator(o2Meta.getName());
    pm.applyChanges(ctx);

    Assert.assertEquals("sinks s1 " + s1.getSinks(), 1, s1.getSinks().size());
    Assert.assertTrue("undeploy " + ctx.undeploy, ctx.undeploy.containsAll(o2PhysicalOpers));
    Assert.assertTrue("deploy " + ctx.deploy, !ctx.deploy.containsAll(o2PhysicalOpers));
    Assert.assertEquals("streams " + dag.getAllStreams(), 1, dag.getAllStreams().size());
    Assert.assertEquals("operators " + dag.getAllOperators(), 3, dag.getAllOperators().size());
    Assert.assertTrue("operators " + dag.getAllOperators(), dag.getAllOperators().containsAll(Sets.newHashSet(o1Meta, o3Meta)));

    try {
      plan.getOperators(o2Meta);
      Assert.fail("removed from physical plan: " + o2Meta);
    } catch (Exception e) {
      // all good
    }
    Assert.assertEquals("containers " + plan.getContainers(), 3, plan.getContainers().size());
    Assert.assertEquals("physical operators " + plan.getAllOperators(), 3, plan.getAllOperators().size());
    Assert.assertEquals("removed containers " + ctx.releaseContainers, 1, ctx.releaseContainers.size());

    try {
      pm.removeOperator(o12Meta.getName());
      Assert.fail("cannot remove operator prior to removing input stream");
    } catch (ValidationException ve) {
      Assert.assertTrue("" + ve.getMessage(), ve.getMessage().matches(".*Operator o12 connected to input streams.*"));
    }

    pm.removeStream(s1.getName());
    pm.removeOperator(o12Meta.getName());

  }

  @Test
  public void testRemoveOperator2()
  {
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    OperatorMeta o1Meta = dag.getMeta(o1);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    OperatorMeta o2Meta = dag.getMeta(o2);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);
    OperatorMeta o3Meta = dag.getMeta(o3);

    LogicalPlan.StreamMeta s1 = dag.addStream("o1.outport1", o1.outport1, o2.inport1, o3.inport1).setLocality(Locality.CONTAINER_LOCAL);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    ctx.deploy.clear();
    ctx.undeploy.clear();
    Assert.assertEquals("containers " + plan.getContainers(), 1, plan.getContainers().size());
    Assert.assertEquals("physical operators " + plan.getAllOperators(), 3, plan.getAllOperators().size());
    Assert.assertEquals("sinks s1 " + s1.getSinks(), 2, s1.getSinks().size());

    List<PTOperator> o2PhysicalOpers = plan.getOperators(o2Meta);
    Assert.assertEquals("instances " + o2Meta, 1, o2PhysicalOpers.size());
    PlanModifier pm = new PlanModifier(plan);
    pm.removeOperator(o2Meta.getName()); // remove operator w/o removing the stream
    pm.applyChanges(ctx);

    Assert.assertEquals("sinks s1 " + s1.getSinks(), 1, s1.getSinks().size());
    Assert.assertTrue("undeploy " + ctx.undeploy, ctx.undeploy.containsAll(o2PhysicalOpers));
    Set<PTOperator> expDeploy = Sets.newHashSet();
    // TODO: container local operators should be included in undeploy/deploy
    //expDeploy.addAll(plan.getOperators(o1Meta));
    //expDeploy.addAll(plan.getOperators(o3Meta));
    Assert.assertEquals("deploy " + ctx.deploy, ctx.deploy, expDeploy);
    Assert.assertEquals("streams " + dag.getAllStreams(), 1, dag.getAllStreams().size());
    Assert.assertEquals("operators " + dag.getAllOperators(), 2, dag.getAllOperators().size());
    Assert.assertTrue("operators " + dag.getAllOperators(), dag.getAllOperators().containsAll(Sets.newHashSet(o1Meta, o3Meta)));

  }

  @Test
  public void testRemoveStream()
  {
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);

    PlanModifier pm = new PlanModifier(plan);
    pm.removeStream("o1.outport1");
    pm.applyChanges(ctx);

    Assert.assertEquals("undeploy " + ctx.undeploy, 2, ctx.undeploy.size());
    Assert.assertEquals("deploy " + ctx.deploy, 2, ctx.deploy.size());
  }

  @Test
  public void testAddStream()
  {
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);

    List<PTOperator> o1Instances = plan.getOperators(dag.getMeta(o1));
    Assert.assertEquals("o1Instances " + o1Instances, 1, o1Instances.size());
    PTOperator o1p1 = o1Instances.get(0);

    OperatorMeta om2 = dag.getMeta(o2);
    List<PTOperator> o2Instances = plan.getOperators(om2);
    Assert.assertEquals("o2Instances " + o2Instances, 1, o2Instances.size());
    PTOperator o2p1 = o2Instances.get(0);

    Assert.assertEquals("outputs " + o1p1, 0, o1p1.getOutputs().size());
    Assert.assertEquals("inputs " + o2p1, 0, o2p1.getInputs().size());

    PlanModifier pm = new PlanModifier(plan);
    pm.addStream("o1.outport1", o1.outport1, o2.inport1);
    pm.addStream("o1.outport1", o1.outport1, o2.inport2);
    pm.applyChanges(ctx);

    Assert.assertEquals("undeploy " + ctx.undeploy, 2, ctx.undeploy.size());
    Assert.assertEquals("deploy " + ctx.deploy, 2, ctx.deploy.size());

    Assert.assertEquals("outputs " + o1p1, 1, o1p1.getOutputs().size());
    Assert.assertEquals("inputs " + o2p1, 2, o2p1.getInputs().size());
    Set<String> portNames = Sets.newHashSet();
    for (PTOperator.PTInput in : o2p1.getInputs()) {
      portNames.add(in.portName);
    }
    Set<String> expPortNames = Sets.newHashSet(GenericTestOperator.IPORT1, GenericTestOperator.IPORT2);
    Assert.assertEquals("input port names " + o2p1.getInputs(), expPortNames, portNames);

  }

  private void testExecutionManager(StorageAgent agent) throws Exception
  {
    dag.setAttribute(OperatorContext.STORAGE_AGENT, agent);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    Assert.assertEquals("" + dnm.containerStartRequests, dnm.containerStartRequests.size(), 0);


    CreateOperatorRequest cor = new CreateOperatorRequest();
    cor.setOperatorFQCN(TestGeneratorInputOperator.class.getName());
    cor.setOperatorName("o1");

    FutureTask<?> lpmf = dnm.logicalPlanModification(Collections.<LogicalPlanRequest>singletonList(cor));
    while (!lpmf.isDone()) {
      dnm.processEvents();
    }

    lpmf.get();

    Assert.assertEquals("" + dnm.containerStartRequests, 1, dnm.containerStartRequests.size());
    PTContainer c = dnm.containerStartRequests.poll().container;
    Assert.assertEquals("operators " + c, 1, c.getOperators().size());

    int deployStatusCnt = 0;
    for (PTOperator oper : c.getOperators()) {
      if (oper.getState() == PTOperator.State.PENDING_DEPLOY) {
        deployStatusCnt++;
      }
    }
    Assert.assertEquals("deploy requests " + c, 1, deployStatusCnt);

    PTOperator oper = c.getOperators().get(0);
    Assert.assertEquals("operator name", "o1", oper.getOperatorMeta().getName());
    Assert.assertEquals("operator class", TestGeneratorInputOperator.class, oper.getOperatorMeta().getOperator().getClass());

  }

  @Test
  public void testExecutionManagerWithSyncStorageAgent() throws Exception
  {
    testExecutionManager(new FSStorageAgent(testMeta.getPath(), null));
  }

  @Test
  public void testExecutionManagerWithAsyncStorageAgent() throws Exception
  {
    testExecutionManager(new AsyncFSStorageAgent(testMeta.getPath(), null));
  }

  @Test
  public void testNewOperatorRecoveryWindowIds()
  {
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    ctx.deploy.clear();
    ctx.undeploy.clear();

    LogicalPlan.OperatorMeta o1Meta = dag.getMeta(o1);
    List<PTOperator> o1Partitions = plan.getOperators(o1Meta);
    PhysicalPlanTest.setActivationCheckpoint(o1Partitions.get(0), 10);

    PlanModifier pm = new PlanModifier(plan);
    GenericTestOperator o2 = new GenericTestOperator();
    GenericTestOperator o3 = new GenericTestOperator();
    pm.addOperator("o2", o2);
    pm.addOperator("o3", o3);
    pm.addStream("s1", o1.outport1, o2.inport2);
    pm.addStream("s2", o2.outport1, o3.inport1);

    pm.applyChanges(ctx);

    LogicalPlan.OperatorMeta o2Meta = plan.getLogicalPlan().getMeta(o2);
    List<PTOperator> o2Partitions = plan.getOperators(o2Meta);
    Assert.assertEquals("o2 activation checkpoint " + o2Meta, 10, o2Partitions.get(0).getRecoveryCheckpoint().windowId);

    LogicalPlan.OperatorMeta o3Meta = plan.getLogicalPlan().getMeta(o3);
    List<PTOperator> o3Partitions = plan.getOperators(o3Meta);
    Assert.assertEquals("o3 activation checkpoint " + o2Meta, 10, o3Partitions.get(0).getRecoveryCheckpoint().windowId);
  }
}
