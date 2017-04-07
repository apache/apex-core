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
package com.datatorrent.stram.stream;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;

/**
 *
 */
public class OiOEndWindowTest
{
  public OiOEndWindowTest()
  {
  }

  public static class TestInputOperator extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<Long> output = new DefaultOutputPort<>();

    @Override
    public void emitTuples()
    {
      BaseOperator.shutdown();
    }

  }

  public static class FirstGenericOperator extends BaseOperator
  {
    public static long endwindowCount;
    public final transient DefaultOutputPort<Long> output = new DefaultOutputPort<>();
    public final transient DefaultInputPort<Number> input = new DefaultInputPort<Number>()
    {
      @Override
      public void process(Number tuple)
      {
      }
    };

    @Override
    public void endWindow()
    {
      endwindowCount++;
      logger.info("in end window of 1st generic operator");
    }

  }

  public static class SecondGenericOperator extends BaseOperator
  {
    public static long endwindowCount;
    public final transient DefaultInputPort<Number> input = new DefaultInputPort<Number>()
    {
      @Override
      public void process(Number tuple)
      {
      }

    };

    @Override
    public void endWindow()
    {
      endwindowCount++;
      logger.info("in end window of 2nd generic operator");

    }

  }

  @Test
  public void validateOiOImplementation() throws Exception
  {
    LogicalPlan lp = new LogicalPlan();
    String workingDir = new File("target/validateOiOImplementation").getAbsolutePath();
    lp.setAttribute(Context.OperatorContext.STORAGE_AGENT, new AsyncFSStorageAgent(workingDir, null));
    TestInputOperator io = lp.addOperator("Input Operator", new TestInputOperator());
    FirstGenericOperator go = lp.addOperator("First Generic Operator", new FirstGenericOperator());
    SecondGenericOperator out = lp.addOperator("Second Generic Operator", new SecondGenericOperator());

    /*
     * This tests make sure that even if the application_window_count is different the endWindow() is called for
     * end_stream
     */
    lp.getOperatorMeta("Second Generic Operator").getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 2);
    StreamMeta stream = lp.addStream("Stream", io.output, go.input);
    StreamMeta stream1 = lp.addStream("Stream1", go.output, out.input);

    stream1.setLocality(Locality.THREAD_LOCAL);
    lp.validate();
    StramLocalCluster slc = new StramLocalCluster(lp);
    slc.run();
    Assert.assertEquals("End Window Count", FirstGenericOperator.endwindowCount, SecondGenericOperator.endwindowCount);
  }

  private static final Logger logger = LoggerFactory.getLogger(OiOEndWindowTest.class);
}
