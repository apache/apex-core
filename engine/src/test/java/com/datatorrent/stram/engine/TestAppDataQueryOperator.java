package com.datatorrent.stram.engine;

import com.datatorrent.common.experimental.AppData;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

public class TestAppDataQueryOperator implements InputOperator, AppData.ConnectionInfoProvider
{
  public final transient DefaultOutputPort<Object> outport = new DefaultOutputPort<Object>();

  private String appDataUrl;
  private String topic;

  @Override
  public void emitTuples()
  {
  }

  @Override
  public void beginWindow(long l)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(Context.OperatorContext cntxt)
  {
  }

  @Override
  public void teardown()
  {
  }

  public void setAppDataUrl(String appDataUrl)
  {
    this.appDataUrl = appDataUrl;
  }

  @Override
  public String getAppDataURL()
  {
    return appDataUrl;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  @Override
  public String getTopic()
  {
    return topic;
  }

}
