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

import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

public class TestGeneratorInputOperator implements InputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(TestGeneratorInputOperator.class);
  public static final String OUTPUT_PORT = "outport";
  private String myConfigProperty;
  private int maxTuples = -1;
  private int generatedTuples = 0;
  private int remainingSleepTime;
  private int emitInterval = 1000;
  private final int spinMillis = 50;
  private String myStringProperty;
  private final ConcurrentLinkedQueue<String> externallyAddedTuples = new ConcurrentLinkedQueue<>();
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Object> outport = new DefaultOutputPort<>();

  public int getMaxTuples()
  {
    return maxTuples;
  }

  public void setEmitInterval(int emitInterval)
  {
    this.emitInterval = emitInterval;
  }

  public void setMaxTuples(int maxNumbers)
  {
    LOG.debug("setting max tuples to {}", maxNumbers);
    this.maxTuples = maxNumbers;
  }

  public String getMyConfigProperty()
  {
    return myConfigProperty;
  }

  public void setMyConfigProperty(String myConfigProperty)
  {
    this.myConfigProperty = myConfigProperty;
  }

  @Override
  public void emitTuples()
  {
    Object tuple;
    while ((tuple = this.externallyAddedTuples.poll()) != null) {
      outport.emit(tuple);
    }

    if (remainingSleepTime > 0) {
      try {
        Thread.sleep(spinMillis);
        remainingSleepTime -= spinMillis;
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    } else if (maxTuples != 0) {
      generatedTuples++;
      LOG.debug("sending tuple " + generatedTuples);
      outport.emit(String.valueOf(generatedTuples));
      if (maxTuples > 0 && maxTuples <= generatedTuples) {
        BaseOperator.shutdown();
        throw new RuntimeException(new InterruptedException("done emitting all."));
      }
      remainingSleepTime = emitInterval;
    } else {
      remainingSleepTime = emitInterval;
    }
  }

  /**
   * Manually add a tuple to emit.
   *
   * @param s tuple which you want to send through this operator
   */
  public void addTuple(String s)
  {
    externallyAddedTuples.add(s);
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  public String getMyStringProperty()
  {
    return myStringProperty;
  }

  public void setMyStringProperty(String myStringProperty)
  {
    this.myStringProperty = myStringProperty;
  }

  public static class InvalidInputOperator extends TestGeneratorInputOperator implements InputOperator
  {
    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
      }
    };
  }

  public static class ValidGenericOperator extends TestGeneratorInputOperator
  {
    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
      }
    };
  }

  public static class ValidInputOperator extends ValidGenericOperator implements InputOperator
  {
  }

}
