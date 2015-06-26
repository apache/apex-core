package com.datatorrent.stram.engine;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.experimental.AppData;
import com.datatorrent.common.util.BaseOperator;

public class TestAppDataSourceOperator extends BaseOperator
{
  @AppData.QueryPort
  public final transient InputPort<Object> query = new DefaultInputPort<Object>()
  {
    @Override
    final public void process(Object payload)
    {
    }
  };

  @AppData.ResultPort
  public final transient DefaultOutputPort<Object> result = new DefaultOutputPort<Object>();


  public final transient InputPort<Object> inport1 = new DefaultInputPort<Object>()
  {
    @Override
    final public void process(Object payload)
    {
    }
  };


}
