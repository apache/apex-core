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
package com.datatorrent.stram.api;

import java.io.Serializable;

/**
 * <p>StramToNodeChangeLoggersRequest class.</p>
 *
 * @since 1.0.4
 */
public class StramToNodeStartRecordingRequest extends StreamingContainerUmbilicalProtocol.StramToNodeRequest implements Serializable
{
  private long numWindows = 0;
  private String id;

  public StramToNodeStartRecordingRequest()
  {
    requestType = RequestType.START_RECORDING;
  }

  public long getNumWindows()
  {
    return numWindows;
  }

  public String getId()
  {
    return id;
  }

  public void setNumWindows(long numWindows)
  {
    this.numWindows = numWindows;
  }

  public void setId(String id)
  {
    this.id = id;
  }

  private static final long serialVersionUID = 201405271034L;

}
