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

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.experimental.AppData;

public class TestAppDataQueryOperator implements InputOperator, AppData.ConnectionInfoProvider
{
  public final transient DefaultOutputPort<Object> outport = new DefaultOutputPort<>();

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
