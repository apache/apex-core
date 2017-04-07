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

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Module;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

/**
 * Unit tests for testing Dag expansion with modules and proxy port substitution
 */
public class ModuleAppTest
{

  /*
   * Input Operator - 1
   */
  static class DummyInputOperator extends BaseOperator implements InputOperator
  {

    Random r = new Random();
    public transient DefaultOutputPort<Integer> output = new DefaultOutputPort<>();

    @Override
    public void emitTuples()
    {
      output.emit(r.nextInt());
    }
  }

  /*
   * Input Operator - 1.1
   */
  static class DummyOperatorAfterInput extends BaseOperator
  {

    public transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
    {
      @Override
      public void process(Integer tuple)
      {
        output.emit(tuple);
      }
    };
    public transient DefaultOutputPort<Integer> output = new DefaultOutputPort<>();
  }

  /*
   * Operator - 2
   */
  static class DummyOperator extends BaseOperator
  {
    int prop;

    public transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
    {
      @Override
      public void process(Integer tuple)
      {
        LOG.debug(tuple.intValue() + " processed");
        output.emit(tuple);
      }
    };
    public transient DefaultOutputPort<Integer> output = new DefaultOutputPort<>();
  }

  /*
   * Output Operator - 3
   */
  static class DummyOutputOperator extends BaseOperator
  {
    int prop;

    public transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
    {
      @Override
      public void process(Integer tuple)
      {
        LOG.debug(tuple.intValue() + " processed");
      }
    };
  }

  /*
   * Module Definition
   */
  static class TestModule implements Module
  {

    public transient ProxyInputPort<Integer> moduleInput = new ProxyInputPort<>();
    public transient ProxyOutputPort<Integer> moduleOutput = new Module.ProxyOutputPort<>();

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      LOG.debug("Module - PopulateDAG");
      DummyOperator dummyOperator = dag.addOperator("DummyOperator", new DummyOperator());
      moduleInput.set(dummyOperator.input);
      moduleOutput.set(dummyOperator.output);
    }
  }

  static class Application implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      LOG.debug("Application - PopulateDAG");
      DummyInputOperator dummyInputOperator = dag.addOperator("DummyInputOperator", new DummyInputOperator());
      DummyOperatorAfterInput dummyOperatorAfterInput = dag.addOperator("DummyOperatorAfterInput",
          new DummyOperatorAfterInput());
      Module m1 = dag.addModule("TestModule1", new TestModule());
      Module m2 = dag.addModule("TestModule2", new TestModule());
      DummyOutputOperator dummyOutputOperator = dag.addOperator("DummyOutputOperator", new DummyOutputOperator());
      dag.addStream("Operator To Operator", dummyInputOperator.output, dummyOperatorAfterInput.input);
      dag.addStream("Operator To Module", dummyOperatorAfterInput.output, ((TestModule)m1).moduleInput);
      dag.addStream("Module To Module", ((TestModule)m1).moduleOutput, ((TestModule)m2).moduleInput);
      dag.addStream("Module To Operator", ((TestModule)m2).moduleOutput, dummyOutputOperator.input);
    }
  }

  @Test
  public void validateTestApplication()
  {
    Configuration conf = new Configuration(false);
    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(conf);
    LogicalPlan dag = new LogicalPlan();
    lpc.prepareDAG(dag, new Application(), "TestApp");

    Assert.assertEquals(2, dag.getAllModules().size(), 2);
    Assert.assertEquals(5, dag.getAllOperators().size());
    Assert.assertEquals(4, dag.getAllStreams().size());
    dag.validate();
  }

  private static Logger LOG = LoggerFactory.getLogger(ModuleAppTest.class);
}
