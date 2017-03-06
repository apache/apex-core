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

import java.util.concurrent.Callable;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.ControlAwareDefaultInputPort;
import org.apache.apex.api.ControlAwareDefaultOutputPort;
import org.apache.apex.api.operator.ControlTuple;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.common.util.BaseOperator;

public class CustomControlTupleTest
{
  public static final Logger LOG = LoggerFactory.getLogger(CustomControlTupleTest.class);
  private static long controlIndex = 0;
  private static int numControlTuples = 0;
  private static boolean done = false;
  private static boolean endApp = false;
  private static long endingWindowId = 0;
  private static boolean immediate = false;

  @Before
  public void starting()
  {
    controlIndex = 0;
    numControlTuples = 0;
    done = false;
    endApp = false;
    endingWindowId = 0;
  }

  public static class Generator extends BaseOperator implements InputOperator
  {
    private long currentWindowId;
    public final transient ControlAwareDefaultOutputPort<Double> out = new ControlAwareDefaultOutputPort<>();

    @Override
    public void beginWindow(long windowId)
    {
      if (!done) {
        currentWindowId = windowId;
        out.emitControl(new TestControlTuple(controlIndex++, immediate));
      }
    }

    @Override
    public void emitTuples()
    {
      if (!done) {
        out.emitControl(new TestControlTuple(controlIndex++, immediate));
      }
    }

    @Override
    public void endWindow()
    {
      if (!done) {
        out.emitControl(new TestControlTuple(controlIndex++, immediate));
        endingWindowId = currentWindowId;
        done = true;
      }
    }
  }

  public static class DefaultProcessor extends BaseOperator
  {
    public final transient DefaultInputPort<Double> input = new DefaultInputPort<Double>()
    {
      @Override
      public void process(Double tuple)
      {
        output.emit(tuple);
      }
    };

    public final transient DefaultOutputPort<Double> output = new DefaultOutputPort<>();
  }

  public static class ControlAwareProcessor extends BaseOperator
  {
    public final transient ControlAwareDefaultInputPort<Double> input = new ControlAwareDefaultInputPort<Double>()
    {
      @Override
      public void process(Double tuple)
      {
        output.emit(tuple);
      }

      @Override
      public boolean processControl(ControlTuple tuple)
      {
        output.emitControl(tuple);
        return true;
      }
    };

    public final transient ControlAwareDefaultOutputPort<Double> output = new ControlAwareDefaultOutputPort<>();
  }

  public static class ControlAwareReceiver extends BaseOperator
  {
    private long currentWindowId;

    @Override
    public void beginWindow(long windowId)
    {
      currentWindowId = windowId;
    }

    public final transient ControlAwareDefaultInputPort<Double> input = new ControlAwareDefaultInputPort<Double>()
    {
      @Override
      public boolean processControl(ControlTuple payload)
      {
        numControlTuples++;
        return false;
      }

      @Override
      public void process(Double tuple)
      {
      }
    };

    @Override
    public void endWindow()
    {
      if (done && currentWindowId > endingWindowId) {
        endApp = true;
      }
    }
  }

  @ApplicationAnnotation(name = "TestDefaultPropagation")
  public static class Application1 implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      Generator randomGenerator = dag.addOperator("randomGenerator", Generator.class);
      DefaultProcessor processor = dag.addOperator("process", DefaultProcessor.class);
      ControlAwareReceiver receiver = dag.addOperator("receiver", ControlAwareReceiver.class);
      dag.addStream("genToProcessor", randomGenerator.out, processor.input);
      dag.addStream("ProcessorToReceiver", processor.output, receiver.input);
    }
  }

  @ApplicationAnnotation(name = "TestExplicitPropagation")
  public static class Application2 implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      Generator randomGenerator = dag.addOperator("randomGenerator", Generator.class);
      ControlAwareProcessor processor = dag.addOperator("process", ControlAwareProcessor.class);
      ControlAwareReceiver receiver = dag.addOperator("receiver", ControlAwareReceiver.class);
      dag.addStream("genToProcessor", randomGenerator.out, processor.input);
      dag.addStream("ProcessorToReceiver", processor.output, receiver.input);
    }
  }

  @ApplicationAnnotation(name = "TestDuplicateControlTuples")
  public static class Application3 implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      Generator randomGenerator = dag.addOperator("randomGenerator", Generator.class);
      DefaultProcessor processor = dag.addOperator("process", DefaultProcessor.class);
      ControlAwareReceiver receiver = dag.addOperator("receiver", ControlAwareReceiver.class);
      dag.addStream("genToProcessor", randomGenerator.out, processor.input);
      dag.addStream("ProcessorToReceiver", processor.output, receiver.input);
      dag.setOperatorAttribute(processor, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<>(2));
    }
  }

  @ApplicationAnnotation(name = "TestThreadLocal")
  public static class Application4 implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      Generator randomGenerator = dag.addOperator("randomGenerator", Generator.class);
      DefaultProcessor processor = dag.addOperator("process", DefaultProcessor.class);
      ControlAwareReceiver receiver = dag.addOperator("receiver", ControlAwareReceiver.class);
      dag.addStream("genToProcessor", randomGenerator.out, processor.input).setLocality(DAG.Locality.THREAD_LOCAL);
      dag.addStream("ProcessorToReceiver", processor.output, receiver.input).setLocality(DAG.Locality.THREAD_LOCAL);
    }
  }

  @ApplicationAnnotation(name = "TestContainerLocal")
  public static class Application5 implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      Generator randomGenerator = dag.addOperator("randomGenerator", Generator.class);
      DefaultProcessor processor = dag.addOperator("process", DefaultProcessor.class);
      ControlAwareReceiver receiver = dag.addOperator("receiver", ControlAwareReceiver.class);
      dag.addStream("genToProcessor", randomGenerator.out, processor.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
      dag.addStream("ProcessorToReceiver", processor.output, receiver.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    }
  }

  public void testApp(StreamingApplication app) throws Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(app, conf);
      LocalMode.Controller lc = lma.getController();
      ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
      {
        @Override
        public Boolean call() throws Exception
        {
          return endApp;
        }
      });

      lc.run(200000); // runs for 20 seconds and quits if terminating condition not reached

      LOG.info("Control Tuples received {} expected {}", numControlTuples, controlIndex);
      Assert.assertTrue("Incorrect Control Tuples", numControlTuples == controlIndex);
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  @Test
  public void testDefaultPropagation() throws Exception
  {
    immediate = false;
    testApp(new Application1());
  }

  @Test
  public void testExplicitPropagation() throws Exception
  {
    immediate = false;
    testApp(new Application2());
  }

  @Test
  public void testDuplicateControlTuples() throws Exception
  {
    immediate = false;
    testApp(new Application3());
  }

  @Test
  public void testThreadLocal() throws Exception
  {
    immediate = false;
    testApp(new Application4());
  }

  @Test
  public void testContainerLocal() throws Exception
  {
    immediate = false;
    testApp(new Application5());
  }

  @Test
  public void testDefaultPropagationImmediate() throws Exception
  {
    immediate = true;
    testApp(new Application1());
  }

  @Test
  public void testExplicitPropagationImmediate() throws Exception
  {
    immediate = true;
    testApp(new Application2());
  }

  @Test
  public void testDuplicateControlTuplesImmediate() throws Exception
  {
    immediate = true;
    testApp(new Application3());
  }

  @Test
  public void testThreadLocalImmediate() throws Exception
  {
    immediate = true;
    testApp(new Application4());
  }

  @Test
  public void testContainerLocalImmediate() throws Exception
  {
    immediate = true;
    testApp(new Application5());
  }

  public static class TestControlTuple implements ControlTuple
  {
    public long data;
    public boolean immediate;

    public TestControlTuple()
    {
      data = 0;
    }

    public TestControlTuple(long data, boolean immediate)
    {
      this.data = data;
      this.immediate = immediate;
    }

    @Override
    public boolean equals(Object t)
    {
      if (t instanceof TestControlTuple && ((TestControlTuple)t).data == this.data) {
        return true;
      }
      return false;
    }

    @Override
    public String toString()
    {
      return data + "";
    }

    @Override
    public DeliveryType getDeliveryType()
    {
      if (immediate) {
        return DeliveryType.IMMEDIATE;
      } else {
        return DeliveryType.END_WINDOW;
      }
    }
  }
}
