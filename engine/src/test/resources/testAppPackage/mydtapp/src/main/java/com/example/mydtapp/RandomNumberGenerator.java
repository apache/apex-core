package com.example.mydtapp;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is a simple operator that emits random number
 */

public class RandomNumberGenerator extends BaseOperator implements InputOperator
{
  public final transient DefaultOutputPort<Double> out = new DefaultOutputPort<Double>();

  @Override
  public void emitTuples()
  {
      out.emit(Math.random());
  }

}
