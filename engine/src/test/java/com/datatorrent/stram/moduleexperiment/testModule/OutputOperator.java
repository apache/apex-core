package com.datatorrent.stram.moduleexperiment.testModule;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Output Operator.
 * Outputs the stream onto the console
 */
public class OutputOperator extends BaseOperator
{
  String prefix;

  public OutputOperator()
  {
  }

  public OutputOperator(String prefix)
  {
    this.prefix = prefix;
  }

  public transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>() {
    
    @Override
    public void process(Integer tuple)
    {
      System.out.println(prefix+" : "+tuple.intValue());
    }
  };
}
