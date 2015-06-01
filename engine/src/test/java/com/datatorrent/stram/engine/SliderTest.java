/**
 * Copyright (c) 2012-2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.engine;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.*;

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

    public final transient DefaultOutputPort<Integer> defaultOutputPort = new DefaultOutputPort<Integer>();

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
      if(sum > 0) {
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
    private int windowsSeen;

    @Override
    public void beginWindow(long windowId)
    {
      windowsSeen++;
    }

    public final transient DefaultInputPort<Integer> validate = new DefaultInputPort<Integer>()
    {
      @Override
      public void process(Integer tuple)
      {
        if (windowsSeen >= numberOfIntegers) {
          int sum = 0;
          for (int i = 0; i < numberOfIntegers; i++) {
            sum = sum + startingInteger + i;
          }
          if (sum != tuple.intValue()) {
            throw new RuntimeException("numbers not matching " + sum + " " + tuple + " " + startingInteger + " " + numberOfIntegers);
          }
          numbersValidated++;
          startingInteger += slideByNumbers;
          windowsSeen -= slideByNumbers;
        }
      }
    };
  }

  @Test
  public void testSlider() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 100);
    Input input = dag.addOperator("Input", new Input());
    Sum sum = dag.addOperator("Sum", new Sum());
    dag.setAttribute(sum, OperatorContext.APPLICATION_WINDOW_COUNT, 5);
    dag.setAttribute(sum, OperatorContext.SLIDE_BY_WINDOW_COUNT, 1);
    Validator validate = dag.addOperator("validator", new Validator());
    validate.numberOfIntegers = 5;
    validate.slideByNumbers = 1;
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
  public void testSliderWithPrimeNumbers() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 100);
    Input input = dag.addOperator("Input", new Input());
    Sum sum = dag.addOperator("Sum", new Sum());
    dag.setAttribute(sum, OperatorContext.APPLICATION_WINDOW_COUNT, 5);
    dag.setAttribute(sum, OperatorContext.SLIDE_BY_WINDOW_COUNT, 2);
    Validator validate = dag.addOperator("validator", new Validator());
    validate.numberOfIntegers = 5;
    validate.slideByNumbers = 2;
    Validator.numbersValidated = 0;
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
  public void testSliderWithProperDivisor() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 100);
    Input input = dag.addOperator("Input", new Input());
    Sum sum = dag.addOperator("Sum", new Sum());
    dag.setAttribute(sum, OperatorContext.APPLICATION_WINDOW_COUNT, 4);
    dag.setAttribute(sum, OperatorContext.SLIDE_BY_WINDOW_COUNT, 2);
    Validator validate = dag.addOperator("validator", new Validator());
    validate.numberOfIntegers = 4;
    validate.slideByNumbers = 2;
    Validator.numbersValidated = 0;
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
}
