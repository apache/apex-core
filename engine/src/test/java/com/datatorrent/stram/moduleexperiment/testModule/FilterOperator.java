package com.datatorrent.stram.moduleexperiment.testModule;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Toy Filter Operator. Removes negative values
 */
public class FilterOperator extends BaseOperator
{
  public transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>() {
    
    @Override
    public void process(Integer tuple)
    {
      if(tuple.intValue() >= 0)
      {
        output.emit(tuple);
      }
    }
  };

  public transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();
}

