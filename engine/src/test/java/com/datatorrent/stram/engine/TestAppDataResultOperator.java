package com.datatorrent.stram.engine;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.experimental.AppData;
import com.datatorrent.common.util.BaseOperator;

public class TestAppDataResultOperator extends BaseOperator implements AppData.ConnectionInfoProvider
{
  public final transient InputPort<Object> inport = new DefaultInputPort<Object>()
  {
    @Override
    final public void process(Object payload)
    {
    }
  };

  private String appDataUrl;
  private String topic;

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

  @AppData.AppendQueryIdToTopic(value = true)
  public static class ResultOperator1 extends TestAppDataResultOperator
  {
  }

  @AppData.AppendQueryIdToTopic(value = false)
  public static class ResultOperator2 extends TestAppDataResultOperator
  {
  }

  public static class ResultOperator3 extends TestAppDataResultOperator
  {
  }

}
