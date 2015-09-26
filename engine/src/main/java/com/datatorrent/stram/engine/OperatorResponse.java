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

import java.io.Serializable;

import com.datatorrent.api.StatsListener;

/**
 * <p>OperatorResponse class.</p>
 *
 * @since 2.1.0
 */
public class OperatorResponse implements StatsListener.OperatorResponse, Serializable
{
  private static final long serialVersionUID = -95162161527528335L;
  /*
   * The unique responseId
   */
  private Long responseId;

  /*
   * The data payload that needs to be sent back
   */
  private Object data;

  public OperatorResponse(long responseId, Object data)
  {
    this.responseId = responseId;
    this.data = data;
  }

  @Override
  public Object getResponseId()
  {
    return responseId;
  }

  @Override
  public Object getResponse()
  {
    return data;
  }

  @Override
  public String toString()
  {
    return "{ responseId: " + responseId + ", data :" + data + "}";
  }
}
