package com.datatorrent.stram.moduleexperiment.testModule;

import java.util.Random;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * Toy Random Input Operator.
 * Generates random integers
 */
public class RandomInputOperator implements InputOperator
{

  Random r;
  public transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();
  long sentAt = System.currentTimeMillis();

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
    r = new Random();
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void emitTuples()
  {
    if(System.currentTimeMillis() - sentAt > 100){
      output.emit(r.nextInt());
      sentAt = System.currentTimeMillis();
    }
  }
}
