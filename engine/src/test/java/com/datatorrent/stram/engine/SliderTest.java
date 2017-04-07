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
package com.datatorrent.stram.engine;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.support.StramTestSupport;

public class SliderTest
{
  public static class Input extends BaseOperator implements InputOperator
  {
    private boolean emit;
    private int val = 1;

    @Override
    public void beginWindow(long windowId)
    {
      emit = true;
    }

    public final transient DefaultOutputPort<Integer> defaultOutputPort = new DefaultOutputPort<>();

    @Override
    public void emitTuples()
    {
      if (emit) {
        emit = false;
        defaultOutputPort.emit(val);
        val++;
      }

    }
  }

  public static class Sum extends BaseOperator implements Operator.Unifier<Integer>
  {
    int sum;

    @Override
    public void beginWindow(long windowId)
    {
      sum = 0;
    }

    public final transient DefaultInputPort<Integer> inputPort = new DefaultInputPort<Integer>()
    {
      @Override
      public void process(Integer tuple)
      {
        Sum.this.process(tuple);
      }
    };

    public final transient DefaultOutputPort<Integer> outputPort = new DefaultOutputPort<Integer>()
    {
      @Override
      public Unifier<Integer> getUnifier()
      {
        return new Sum();
      }
    };

    @Override
    public void process(Integer tuple)
    {
      sum += tuple;
    }

    @Override
    public void endWindow()
    {
      if (sum > 0) {
        outputPort.emit(sum);
      }
    }
  }

  public static class Validator extends BaseOperator
  {
    public static int numbersValidated;
    public int numberOfIntegers;
    private int startingInteger = 1;
    public int slideByNumbers;
    private int staticSum;

    @Override
    public void setup(Context.OperatorContext context)
    {
      staticSum = (numberOfIntegers * (numberOfIntegers - 1)) / 2;
    }

    @Override
    public void beginWindow(long windowId)
    {

    }

    public final transient DefaultInputPort<Integer> validate = new DefaultInputPort<Integer>()
    {
      @Override
      public void process(Integer tuple)
      {
        int sum = staticSum + numberOfIntegers * startingInteger;
        if (sum != tuple.intValue()) {
          throw new RuntimeException("numbers not matching " + sum + " " + tuple + " " + startingInteger + " " + numberOfIntegers);
        }
        numbersValidated++;
        startingInteger += slideByNumbers;
      }
    };
  }

  private void test(int applicationWindowCount, int slideByWindowCount) throws Exception
  {
    LogicalPlan dag = new LogicalPlan();
    String workingDir = new File("target/sliderTest").getAbsolutePath();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new AsyncFSStorageAgent(workingDir, null));
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 100);
    Input input = dag.addOperator("Input", new Input());
    Sum sum = dag.addOperator("Sum", new Sum());
    dag.setOperatorAttribute(sum, OperatorContext.APPLICATION_WINDOW_COUNT, applicationWindowCount);
    dag.setOperatorAttribute(sum, OperatorContext.SLIDE_BY_WINDOW_COUNT, slideByWindowCount);
    Validator validate = dag.addOperator("validator", new Validator());
    Validator.numbersValidated = 0;
    validate.numberOfIntegers = applicationWindowCount;
    validate.slideByNumbers = slideByWindowCount;
    dag.addStream("input-sum", input.defaultOutputPort, sum.inputPort);
    dag.addStream("sum-validator", sum.outputPort, validate.validate);
    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.runAsync();

    long startTms = System.currentTimeMillis();
    while (StramTestSupport.DEFAULT_TIMEOUT_MILLIS > System.currentTimeMillis() - startTms) {
      if (validate.numbersValidated > 5) {
        break;
      }
      Thread.sleep(100);
    }
    lc.shutdown();
    Assert.assertTrue("numbers validated more than zero ", validate.numbersValidated > 0);
  }

  @Test
  public void testSlider() throws Exception
  {
    test(5, 1);
  }

  @Test
  public void testSliderWithPrimeNumbers() throws Exception
  {
    test(5, 2);
  }

  @Test
  public void testSliderWithProperDivisor() throws Exception
  {
    test(4, 2);
  }
}
