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
package com.datatorrent.stram.plan.logical.module;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Module;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

public class TestModuleExpansion
{
  public static class DummyInputOperator extends BaseOperator implements InputOperator
  {
    private int inputOperatorProp = 0;

    Random r = new Random();
    public transient DefaultOutputPort<Integer> out = new DefaultOutputPort<>();

    @Override
    public void emitTuples()
    {
      out.emit(r.nextInt());
    }

    public int getInputOperatorProp()
    {
      return inputOperatorProp;
    }

    public void setInputOperatorProp(int inputOperatorProp)
    {
      this.inputOperatorProp = inputOperatorProp;
    }
  }

  public static class DummyOperator extends BaseOperator
  {
    private int operatorProp = 0;

    @OutputPortFieldAnnotation(optional = true)
    public final transient DefaultOutputPort<Integer> out1 = new DefaultOutputPort<>();

    @OutputPortFieldAnnotation(optional = true)
    public final transient DefaultOutputPort<Integer> out2 = new DefaultOutputPort<>();

    @InputPortFieldAnnotation(optional = true)
    public final transient DefaultInputPort<Integer> in = new DefaultInputPort<Integer>()
    {
      @Override
      public void process(Integer tuple)
      {
        out1.emit(tuple);
        out2.emit(tuple);
      }
    };

    public int getOperatorProp()
    {
      return operatorProp;
    }

    public void setOperatorProp(int operatorProp)
    {
      this.operatorProp = operatorProp;
    }
  }

  public static class TestPartitioner implements Partitioner<DummyOperator>, Serializable
  {
    @Override
    public Collection<Partition<DummyOperator>> definePartitions(Collection<Partition<DummyOperator>> partitions, PartitioningContext context)
    {
      ArrayList<Partition<DummyOperator>> lst = new ArrayList();
      lst.add(partitions.iterator().next());
      return lst;
    }

    @Override
    public void partitioned(Map<Integer, Partition<DummyOperator>> partitions)
    {

    }
  }

  public static class Level1Module implements Module
  {
    private int level1ModuleProp = 0;

    @InputPortFieldAnnotation(optional = true)
    public final transient ProxyInputPort<Integer> mIn = new ProxyInputPort<>();
    @OutputPortFieldAnnotation(optional = true)
    public final transient ProxyOutputPort<Integer> mOut = new ProxyOutputPort<>();
    private int memory = 512;
    private int portMemory = 2;

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      DummyOperator o1 = dag.addOperator("O1", new DummyOperator());
      o1.setOperatorProp(level1ModuleProp);

      /** set various attribute on the operator for testing */
      Attribute.AttributeMap attr = dag.getMeta(o1).getAttributes();
      attr.put(OperatorContext.MEMORY_MB, memory);
      attr.put(OperatorContext.APPLICATION_WINDOW_COUNT, 2);
      attr.put(OperatorContext.LOCALITY_HOST, "host1");
      attr.put(OperatorContext.PARTITIONER, new TestPartitioner());
      attr.put(OperatorContext.CHECKPOINT_WINDOW_COUNT, 120);
      attr.put(OperatorContext.STATELESS, true);
      attr.put(OperatorContext.SPIN_MILLIS, 20);

      dag.setInputPortAttribute(o1.in, Context.PortContext.BUFFER_MEMORY_MB, portMemory);
      mIn.set(o1.in);
      mOut.set(o1.out1);
    }

    public int getLevel1ModuleProp()
    {
      return level1ModuleProp;
    }

    public void setLevel1ModuleProp(int level1ModuleProp)
    {
      this.level1ModuleProp = level1ModuleProp;
    }

    public int getMemory()
    {
      return memory;
    }

    public void setMemory(int memory)
    {
      this.memory = memory;
    }

    public int getPortMemory()
    {
      return portMemory;
    }

    public void setPortMemory(int portMemory)
    {
      this.portMemory = portMemory;
    }
  }

  public static class Level2ModuleA implements Module
  {
    private int level2ModuleAProp1 = 0;
    private int level2ModuleAProp2 = 0;
    private int level2ModuleAProp3 = 0;

    @InputPortFieldAnnotation(optional = true)
    public final transient ProxyInputPort<Integer> mIn = new ProxyInputPort<>();

    @OutputPortFieldAnnotation(optional = true)
    public final transient ProxyOutputPort<Integer> mOut1 = new ProxyOutputPort<>();

    @OutputPortFieldAnnotation(optional = true)
    public final transient ProxyOutputPort<Integer> mOut2 = new ProxyOutputPort<>();

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      Level1Module m1 = dag.addModule("M1", new Level1Module());
      m1.setMemory(1024);
      m1.setPortMemory(1);
      m1.setLevel1ModuleProp(level2ModuleAProp1);

      Level1Module m2 = dag.addModule("M2", new Level1Module());
      m2.setMemory(2048);
      m2.setPortMemory(2);
      m2.setLevel1ModuleProp(level2ModuleAProp2);

      DummyOperator o1 = dag.addOperator("O1", new DummyOperator());
      o1.setOperatorProp(level2ModuleAProp3);

      dag.addStream("M1_M2&O1", m1.mOut, m2.mIn, o1.in).setLocality(DAG.Locality.CONTAINER_LOCAL);

      mIn.set(m1.mIn);
      mOut1.set(m2.mOut);
      mOut2.set(o1.out1);
    }

    public int getLevel2ModuleAProp1()
    {
      return level2ModuleAProp1;
    }

    public void setLevel2ModuleAProp1(int level2ModuleAProp1)
    {
      this.level2ModuleAProp1 = level2ModuleAProp1;
    }

    public int getLevel2ModuleAProp2()
    {
      return level2ModuleAProp2;
    }

    public void setLevel2ModuleAProp2(int level2ModuleAProp2)
    {
      this.level2ModuleAProp2 = level2ModuleAProp2;
    }

    public int getLevel2ModuleAProp3()
    {
      return level2ModuleAProp3;
    }

    public void setLevel2ModuleAProp3(int level2ModuleAProp3)
    {
      this.level2ModuleAProp3 = level2ModuleAProp3;
    }
  }

  public static class Level2ModuleB implements Module
  {
    private int level2ModuleBProp1 = 0;
    private int level2ModuleBProp2 = 0;
    private int level2ModuleBProp3 = 0;

    @InputPortFieldAnnotation(optional = true)
    public final transient ProxyInputPort<Integer> mIn = new ProxyInputPort<>();

    @OutputPortFieldAnnotation(optional = true)
    public final transient ProxyOutputPort<Integer> mOut1 = new ProxyOutputPort<>();

    @OutputPortFieldAnnotation(optional = true)
    public final transient ProxyOutputPort<Integer> mOut2 = new ProxyOutputPort<>();

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      DummyOperator o1 = dag.addOperator("O1", new DummyOperator());
      o1.setOperatorProp(level2ModuleBProp1);

      Level1Module m1 = dag.addModule("M1", new Level1Module());
      m1.setMemory(4096);
      m1.setPortMemory(3);
      m1.setLevel1ModuleProp(level2ModuleBProp2);

      DummyOperator o2 = dag.addOperator("O2", new DummyOperator());
      o2.setOperatorProp(level2ModuleBProp3);

      dag.addStream("O1_M1", o1.out1, m1.mIn).setLocality(DAG.Locality.THREAD_LOCAL);
      dag.addStream("O1_O2", o1.out2, o2.in).setLocality(DAG.Locality.RACK_LOCAL);

      mIn.set(o1.in);
      mOut1.set(m1.mOut);
      mOut2.set(o2.out1);
    }

    public int getLevel2ModuleBProp1()
    {
      return level2ModuleBProp1;
    }

    public void setLevel2ModuleBProp1(int level2ModuleBProp1)
    {
      this.level2ModuleBProp1 = level2ModuleBProp1;
    }

    public int getLevel2ModuleBProp2()
    {
      return level2ModuleBProp2;
    }

    public void setLevel2ModuleBProp2(int level2ModuleBProp2)
    {
      this.level2ModuleBProp2 = level2ModuleBProp2;
    }

    public int getLevel2ModuleBProp3()
    {
      return level2ModuleBProp3;
    }

    public void setLevel2ModuleBProp3(int level2ModuleBProp3)
    {
      this.level2ModuleBProp3 = level2ModuleBProp3;
    }
  }

  public static class Level3Module implements Module
  {

    public final transient ProxyInputPort<Integer> mIn = new ProxyInputPort<>();
    public final transient ProxyOutputPort<Integer> mOut1 = new ProxyOutputPort<>();
    public final transient ProxyOutputPort<Integer> mOut2 = new ProxyOutputPort<>();

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      DummyOperator op = dag.addOperator("O1", new DummyOperator());
      Level2ModuleB m1 = dag.addModule("M1", new Level2ModuleB());
      Level1Module m2 = dag.addModule("M2", new Level1Module());

      dag.addStream("s1", op.out1, m1.mIn);
      dag.addStream("s2", op.out2, m2.mIn);

      mIn.set(op.in);
      mOut1.set(m1.mOut1);
      mOut2.set(m2.mOut);
    }
  }

  public static class NestedModuleApp implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      DummyInputOperator o1 = dag.addOperator("O1", new DummyInputOperator());
      o1.setInputOperatorProp(1);

      DummyOperator o2 = dag.addOperator("O2", new DummyOperator());
      o2.setOperatorProp(2);

      Level2ModuleA ma = dag.addModule("Ma", new Level2ModuleA());
      ma.setLevel2ModuleAProp1(11);
      ma.setLevel2ModuleAProp2(12);
      ma.setLevel2ModuleAProp3(13);

      Level2ModuleB mb = dag.addModule("Mb", new Level2ModuleB());
      mb.setLevel2ModuleBProp1(21);
      mb.setLevel2ModuleBProp2(22);
      mb.setLevel2ModuleBProp3(23);

      Level2ModuleA mc = dag.addModule("Mc", new Level2ModuleA());
      mc.setLevel2ModuleAProp1(31);
      mc.setLevel2ModuleAProp2(32);
      mc.setLevel2ModuleAProp3(33);

      Level2ModuleB md = dag.addModule("Md", new Level2ModuleB());
      md.setLevel2ModuleBProp1(41);
      md.setLevel2ModuleBProp2(42);
      md.setLevel2ModuleBProp3(43);

      Level3Module me = dag.addModule("Me", new Level3Module());

      dag.addStream("O1_O2", o1.out, o2.in, me.mIn);
      dag.addStream("O2_Ma", o2.out1, ma.mIn);
      dag.addStream("Ma_Mb", ma.mOut1, mb.mIn);
      dag.addStream("Ma_Md", ma.mOut2, md.mIn);
      dag.addStream("Mb_Mc", mb.mOut2, mc.mIn);
    }
  }

  @Test
  public void testModuleExtreme()
  {
    StreamingApplication app = new NestedModuleApp();
    Configuration conf = new Configuration(false);
    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(conf);
    LogicalPlan dag = new LogicalPlan();
    lpc.prepareDAG(dag, app, "ModuleApp");

    dag.validate();
    validateTopLevelOperators(dag);
    validateTopLevelStreams(dag);
    validatePublicMethods(dag);
  }

  private void validateTopLevelStreams(LogicalPlan dag)
  {
    List<String> streamNames = new ArrayList<>();
    for (LogicalPlan.StreamMeta streamMeta : dag.getAllStreams()) {
      streamNames.add(streamMeta.getName());
    }

    Assert.assertTrue(streamNames.contains(componentName("Mb", "O1_M1")));
    Assert.assertTrue(streamNames.contains("O2_Ma"));
    Assert.assertTrue(streamNames.contains("Mb_Mc"));
    Assert.assertTrue(streamNames.contains(componentName("Mb", "O1_O2")));
    Assert.assertTrue(streamNames.contains(componentName("Ma", "M1_M2&O1")));
    Assert.assertTrue(streamNames.contains(componentName("Md", "O1_M1")));
    Assert.assertTrue(streamNames.contains(componentName("Ma_Md")));
    Assert.assertTrue(streamNames.contains(componentName("Mc", "M1_M2&O1")));
    Assert.assertTrue(streamNames.contains(componentName("Md", "O1_O2")));
    Assert.assertTrue(streamNames.contains("Ma_Mb"));
    Assert.assertTrue(streamNames.contains("O1_O2"));

    validateSeperateStream(dag, componentName("Mb", "O1_M1"), componentName("Mb", "O1"),
        componentName("Mb", "M1", "O1"));
    validateSeperateStream(dag, "O2_Ma", "O2", componentName("Ma", "M1", "O1"));
    validateSeperateStream(dag, "Mb_Mc", componentName("Mb", "O2"), componentName("Mc", "M1", "O1"));
    validateSeperateStream(dag, componentName("Mb", "O1_O2"), componentName("Mb", "O1"), componentName("Mb", "O2"));
    validateSeperateStream(dag, componentName("Ma", "M1_M2&O1"), componentName("Ma", "M1", "O1"),
        componentName("Ma", "O1"), componentName("Ma", "M2", "O1"));
    validateSeperateStream(dag, componentName("Md", "O1_M1"), componentName("Md", "O1"),
        componentName("Md", "M1", "O1"));
    validateSeperateStream(dag, "Ma_Md", componentName("Ma", "O1"), componentName("Md", "O1"));
    validateSeperateStream(dag, componentName("Mc", "M1_M2&O1"), componentName("Mc", "M1", "O1"),
        componentName("Mc", "O1"), componentName("Mc", "M2", "O1"));
    validateSeperateStream(dag, componentName("Md", "O1_O2"), componentName("Md", "O1"), componentName("Md", "O2"));
    validateSeperateStream(dag, "Ma_Mb", componentName("Ma", "M2", "O1"), componentName("Mb", "O1"));
    validateSeperateStream(dag, "O1_O2", "O1", "O2", componentName("Me", "O1"));

    /* Verify that stream locality is set correctly in top level dag */
    validateStreamLocality(dag, componentName("Mc", "M1_M2&O1"), DAG.Locality.CONTAINER_LOCAL);
    validateStreamLocality(dag, componentName("Mb", "O1_M1"), DAG.Locality.THREAD_LOCAL);
    validateStreamLocality(dag, componentName("Mb", "O1_O2"), DAG.Locality.RACK_LOCAL);
    validateStreamLocality(dag, componentName("Mc", "M1_M2&O1"), DAG.Locality.CONTAINER_LOCAL);
    validateStreamLocality(dag, componentName("Md", "O1_M1"), DAG.Locality.THREAD_LOCAL);
    validateStreamLocality(dag, componentName("Me", "s1"), null);

  }

  private void validateSeperateStream(LogicalPlan dag, String streamName, String inputOperatorName,
      String... outputOperatorNames)
  {
    LogicalPlan.StreamMeta streamMeta = dag.getStream(streamName);
    String sourceName = streamMeta.getSource().getOperatorMeta().getName();

    List<String> sinksName = new ArrayList<>();
    for (LogicalPlan.InputPortMeta inputPortMeta : streamMeta.getSinks()) {
      sinksName.add(inputPortMeta.getOperatorMeta().getName());
    }

    Assert.assertTrue(inputOperatorName.equals(sourceName));
    Assert.assertEquals(outputOperatorNames.length, sinksName.size());

    for (String outputOperatorName : outputOperatorNames) {
      Assert.assertTrue(sinksName.contains(outputOperatorName));
    }
  }

  private void validateTopLevelOperators(LogicalPlan dag)
  {
    List<String> operatorNames = new ArrayList<>();
    for (LogicalPlan.OperatorMeta operatorMeta : dag.getAllOperators()) {
      operatorNames.add(operatorMeta.getName());
    }
    Assert.assertTrue(operatorNames.contains("O1"));
    Assert.assertTrue(operatorNames.contains("O2"));
    Assert.assertTrue(operatorNames.contains(componentName("Ma", "M1", "O1")));
    Assert.assertTrue(operatorNames.contains(componentName("Ma", "M2", "O1")));
    Assert.assertTrue(operatorNames.contains(componentName("Ma", "O1")));
    Assert.assertTrue(operatorNames.contains(componentName("Mb", "O1")));
    Assert.assertTrue(operatorNames.contains(componentName("Mb", "M1", "O1")));
    Assert.assertTrue(operatorNames.contains(componentName("Mb", "O2")));
    Assert.assertTrue(operatorNames.contains(componentName("Mc", "M1", "O1")));
    Assert.assertTrue(operatorNames.contains(componentName("Mc", "M2", "O1")));
    Assert.assertTrue(operatorNames.contains(componentName("Mc", "O1")));
    Assert.assertTrue(operatorNames.contains(componentName("Md", "O1")));
    Assert.assertTrue(operatorNames.contains(componentName("Md", "M1", "O1")));
    Assert.assertTrue(operatorNames.contains(componentName("Md", "O2")));

    validateOperatorPropertyValue(dag, "O1", 1);
    validateOperatorPropertyValue(dag, "O2", 2);
    validateOperatorPropertyValue(dag, componentName("Ma", "M1", "O1"), 11);
    validateOperatorPropertyValue(dag, componentName("Ma", "M2", "O1"), 12);
    validateOperatorPropertyValue(dag, componentName("Ma", "O1"), 13);
    validateOperatorPropertyValue(dag, componentName("Mb", "O1"), 21);
    validateOperatorPropertyValue(dag, componentName("Mb", "M1", "O1"), 22);
    validateOperatorPropertyValue(dag, componentName("Mb", "O2"), 23);
    validateOperatorPropertyValue(dag, componentName("Mc", "M1", "O1"), 31);
    validateOperatorPropertyValue(dag, componentName("Mc", "M2", "O1"), 32);
    validateOperatorPropertyValue(dag, componentName("Mc", "O1"), 33);
    validateOperatorPropertyValue(dag, componentName("Md", "O1"), 41);
    validateOperatorPropertyValue(dag, componentName("Md", "M1", "O1"), 42);
    validateOperatorPropertyValue(dag, componentName("Md", "O2"), 43);

    validateOperatorParent(dag, "O1", null);
    validateOperatorParent(dag, "O2", null);
    validateOperatorParent(dag, componentName("Ma", "M1", "O1"), componentName("Ma", "M1"));
    validateOperatorParent(dag, componentName("Ma", "M2", "O1"), componentName("Ma", "M2"));
    validateOperatorParent(dag, componentName("Ma", "O1"), "Ma");
    validateOperatorParent(dag, componentName("Mb", "O1"), "Mb");
    validateOperatorParent(dag, componentName("Mb", "M1", "O1"), componentName("Mb", "M1"));
    validateOperatorParent(dag, componentName("Mb", "O2"), "Mb");
    validateOperatorParent(dag, componentName("Mc", "M1", "O1"), componentName("Mc", "M1"));
    validateOperatorParent(dag, componentName("Mc", "M2", "O1"), componentName("Mc", "M2"));
    validateOperatorParent(dag, componentName("Mc", "O1"), "Mc");
    validateOperatorParent(dag, componentName("Md", "O1"), "Md");
    validateOperatorParent(dag, componentName("Md", "M1", "O1"), componentName("Md", "M1"));
    validateOperatorParent(dag, componentName("Md", "O2"), "Md");

    validateOperatorAttribute(dag, componentName("Ma", "M1", "O1"), 1024);
    validateOperatorAttribute(dag, componentName("Ma", "M2", "O1"), 2048);
    validateOperatorAttribute(dag, componentName("Mb", "M1", "O1"), 4096);
    validateOperatorAttribute(dag, componentName("Mc", "M1", "O1"), 1024);
    validateOperatorAttribute(dag, componentName("Mc", "M2", "O1"), 2048);

    validatePortAttribute(dag, componentName("Ma", "M1", "O1"), 1);
    validatePortAttribute(dag, componentName("Ma", "M2", "O1"), 2);
    validatePortAttribute(dag, componentName("Mb", "M1", "O1"), 3);
    validatePortAttribute(dag, componentName("Mc", "M1", "O1"), 1);
    validatePortAttribute(dag, componentName("Mc", "M2", "O1"), 2);
  }

  private void validateOperatorParent(LogicalPlan dag, String operatorName, String parentModuleName)
  {
    LogicalPlan.OperatorMeta operatorMeta = dag.getOperatorMeta(operatorName);
    if (parentModuleName == null) {
      Assert.assertNull(operatorMeta.getModuleName());
    } else {
      Assert.assertTrue(parentModuleName.equals(operatorMeta.getModuleName()));
    }
  }

  private void validateOperatorPropertyValue(LogicalPlan dag, String operatorName, int expectedValue)
  {
    LogicalPlan.OperatorMeta oMeta = dag.getOperatorMeta(operatorName);
    if (operatorName.equals("O1")) {
      DummyInputOperator operator = (DummyInputOperator)oMeta.getOperator();
      Assert.assertEquals(expectedValue, operator.getInputOperatorProp());
    } else {
      DummyOperator operator = (DummyOperator)oMeta.getOperator();
      Assert.assertEquals(expectedValue, operator.getOperatorProp());
    }
  }

  private void validatePublicMethods(LogicalPlan dag)
  {
    // Logical dag contains 4 modules added on top level.
    List<String> moduleNames = new ArrayList<>();
    for (LogicalPlan.ModuleMeta moduleMeta : dag.getAllModules()) {
      moduleNames.add(moduleMeta.getName());
    }
    Assert.assertTrue(moduleNames.contains("Ma"));
    Assert.assertTrue(moduleNames.contains("Mb"));
    Assert.assertTrue(moduleNames.contains("Mc"));
    Assert.assertTrue(moduleNames.contains("Md"));
    Assert.assertTrue(moduleNames.contains("Me"));
    Assert.assertEquals("Number of modules are 5", 5, dag.getAllModules().size());

  }

  private static String componentName(String... names)
  {
    if (names.length == 0) {
      return "";
    }
    StringBuilder sb = new StringBuilder(names[0]);
    for (int i = 1; i < names.length; i++) {
      sb.append(LogicalPlan.MODULE_NAMESPACE_SEPARATOR);
      sb.append(names[i]);
    }
    return sb.toString();
  }

  /**
   * Generate a conflict, Add a top level operator with name "m1_O1",
   * and add a module "m1" which will populate operator "O1", causing name conflict with
   * top level operator.
   */
  @Test(expected = java.lang.IllegalArgumentException.class)
  public void conflictingNamesWithExpandedModule()
  {
    Configuration conf = new Configuration(false);
    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(conf);
    LogicalPlan dag = new LogicalPlan();
    DummyInputOperator in = dag.addOperator(componentName("m1", "O1"), new DummyInputOperator());
    Level2ModuleA module = dag.addModule("m1", new Level2ModuleA());
    dag.addStream("s1", in.out, module.mIn);
    lpc.prepareDAG(dag, null, "ModuleApp");
    dag.validate();
  }

  /**
   * Module and Operator with same name is not allowed in a DAG, to prevent properties
   * conflict.
   */
  @Test(expected = java.lang.IllegalArgumentException.class)
  public void conflictingNamesWithOperator1()
  {
    Configuration conf = new Configuration(false);
    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(conf);
    LogicalPlan dag = new LogicalPlan();
    DummyInputOperator in = dag.addOperator("M1", new DummyInputOperator());
    Level2ModuleA module = dag.addModule("M1", new Level2ModuleA());
    dag.addStream("s1", in.out, module.mIn);
    lpc.prepareDAG(dag, null, "ModuleApp");
    dag.validate();
  }

  /**
   * Module and Operator with same name is not allowed in a DAG, to prevent properties
   * conflict.
   */
  @Test(expected = java.lang.IllegalArgumentException.class)
  public void conflictingNamesWithOperator2()
  {
    Configuration conf = new Configuration(false);
    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(conf);
    LogicalPlan dag = new LogicalPlan();
    Level2ModuleA module = dag.addModule("M1", new Level2ModuleA());
    DummyInputOperator in = dag.addOperator("M1", new DummyInputOperator());
    dag.addStream("s1", in.out, module.mIn);
    lpc.prepareDAG(dag, null, "ModuleApp");
    dag.validate();
  }

  /**
   * Verify attributes populated on DummyOperator from Level1 module
   */
  private void validateOperatorAttribute(LogicalPlan dag, String name, int memory)
  {
    LogicalPlan.OperatorMeta oMeta = dag.getOperatorMeta(name);
    Attribute.AttributeMap attrs = oMeta.getAttributes();
    Assert.assertEquals((int)attrs.get(OperatorContext.MEMORY_MB), memory);
    Assert.assertEquals("Application window id is 2 ", (int)attrs.get(OperatorContext.APPLICATION_WINDOW_COUNT), 2);
    Assert.assertEquals("Locality host is host1", attrs.get(OperatorContext.LOCALITY_HOST), "host1");
    Assert.assertEquals(attrs.get(OperatorContext.PARTITIONER).getClass(), TestPartitioner.class);
    Assert.assertEquals("Checkpoint window count ", (int)attrs.get(OperatorContext.CHECKPOINT_WINDOW_COUNT), 120);
    Assert.assertEquals("Operator is stateless ", attrs.get(OperatorContext.STATELESS), true);
    Assert.assertEquals("SPIN MILLIS is set to 20 ", (int)attrs.get(OperatorContext.SPIN_MILLIS), 20);

  }

  /**
   * Validate attribute set on the port of DummyOperator in Level1Module
   */
  private void validatePortAttribute(LogicalPlan dag, String name, int memory)
  {
    LogicalPlan.InputPortMeta imeta = dag.getOperatorMeta(name).getInputStreams().keySet().iterator().next();
    Assert.assertEquals(memory, (int)imeta.getAttributes().get(Context.PortContext.BUFFER_MEMORY_MB));
  }

  /**
   * validate if stream attributes are copied or not
   */
  private void validateStreamLocality(LogicalPlan dag, String name, DAG.Locality locality)
  {
    LogicalPlan.StreamMeta meta = dag.getStream(name);
    Assert.assertTrue("Metadata for stream is available ", meta != null);
    Assert.assertEquals("Locality is " + locality, meta.getLocality(), locality);
  }

  @Test
  public void testLoadFromPropertiesFile() throws IOException
  {
    Properties props = new Properties();
    String resourcePath = "/testModuleTopology.properties";
    InputStream is = this.getClass().getResourceAsStream(resourcePath);
    if (is == null) {
      throw new RuntimeException("Could not load " + resourcePath);
    }
    props.load(is);
    LogicalPlanConfiguration pb = new LogicalPlanConfiguration(new Configuration(false))
        .addFromProperties(props, null);

    LogicalPlan dag = new LogicalPlan();
    pb.populateDAG(dag);
    pb.prepareDAG(dag, null, "testApplication");
    dag.validate();
    validateTopLevelOperators(dag);
    validateTopLevelStreams(dag);
    validatePublicMethods(dag);
  }

  @Test
  public void testLoadFromJson() throws Exception
  {
    String resourcePath = "/testModuleTopology.json";
    InputStream is = this.getClass().getResourceAsStream(resourcePath);
    if (is == null) {
      throw new RuntimeException("Could not load " + resourcePath);
    }
    StringWriter writer = new StringWriter();

    IOUtils.copy(is, writer);
    JSONObject json = new JSONObject(writer.toString());

    Configuration conf = new Configuration(false);
    conf.set(StreamingApplication.APEX_PREFIX + "operator.operator3.prop.myStringProperty", "o3StringFromConf");

    LogicalPlanConfiguration planConf = new LogicalPlanConfiguration(conf);
    LogicalPlan dag = planConf.createFromJson(json, "testLoadFromJson");
    planConf.prepareDAG(dag, null, "testApplication");
    dag.validate();
    validateTopLevelOperators(dag);
    validateTopLevelStreams(dag);
    validatePublicMethods(dag);
  }

}
