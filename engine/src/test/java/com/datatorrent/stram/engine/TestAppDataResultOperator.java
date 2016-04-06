/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.engine;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.experimental.AppData;
import com.datatorrent.common.util.BaseOperator;

public class TestAppDataResultOperator extends BaseOperator implements AppData.ConnectionInfoProvider
{
  public final transient InputPort<Object> inport = new DefaultInputPort<Object>()
  {
    @Override
    public final void process(Object payload)
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
