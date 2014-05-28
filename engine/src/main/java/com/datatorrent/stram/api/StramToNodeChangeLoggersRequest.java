/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.stram.api;

import java.io.Serializable;
import java.util.Map;

public class StramToNodeChangeLoggersRequest extends StreamingContainerUmbilicalProtocol.StramToNodeRequest implements Serializable
{
  private Map<String, String> targetChanges;

  public StramToNodeChangeLoggersRequest()
  {
    requestType = RequestType.SET_LOG_LEVEL;
  }

  public void setTargetChanges(Map<String, String> targetChanges)
  {
    this.targetChanges = targetChanges;
  }

  public Map<String, String> getTargetChanges()
  {
    return this.targetChanges;
  }

  private static final long serialVersionUID = 201405271034L;

}
