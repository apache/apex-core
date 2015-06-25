package com.example.mydtapp;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is a simple output operator that prints everything to stdout
 */

public class StdoutOperator extends BaseOperator
{
  public final transient DefaultInputPort<Object> in = new DefaultInputPort<Object>()
  {
    @Override
    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    public void process(Object t)
    {
      System.out.println(t.toString());
    }
  };

}
