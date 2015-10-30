package com.datatorrent.stram.moduleexperiment.testModule;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Toy OddEven Operator. Separates the stream into Odd and Even integers
 */
public class OddEvenOperator extends BaseOperator
{
  public transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>() {
    
    @Override
    public void process(Integer tuple)
    {
      if(tuple.intValue() % 2 == 0)
      {
        even.emit(tuple);
      }
      else
      {
        odd.emit(tuple);
      }
    }
  };

  public transient DefaultOutputPort<Integer> odd = new DefaultOutputPort<Integer>();
  public transient DefaultOutputPort<Integer> even = new DefaultOutputPort<Integer>();

}
