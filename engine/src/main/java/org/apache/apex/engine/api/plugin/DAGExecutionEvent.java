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
package org.apache.apex.engine.api.plugin;

import org.apache.apex.api.plugin.Event;

import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol;

import static org.apache.apex.engine.api.plugin.DAGExecutionEvent.Type.COMMIT_EVENT;
import static org.apache.apex.engine.api.plugin.DAGExecutionEvent.Type.HEARTBEAT_EVENT;
import static org.apache.apex.engine.api.plugin.DAGExecutionEvent.Type.STRAM_EVENT;

/**
 * @since 3.6.0
 */
public class DAGExecutionEvent extends Event.BaseEvent<DAGExecutionEvent.Type>
{
  public enum Type implements Event.Type
  {
    HEARTBEAT_EVENT, STRAM_EVENT, COMMIT_EVENT
  }

  public static class HeartbeatExecutionEvent extends DAGExecutionEvent
  {
    private final StreamingContainerUmbilicalProtocol.ContainerHeartbeat heartbeat;

    public HeartbeatExecutionEvent(StreamingContainerUmbilicalProtocol.ContainerHeartbeat heartbeat)
    {
      super(HEARTBEAT_EVENT);
      this.heartbeat = heartbeat;
    }

    public StreamingContainerUmbilicalProtocol.ContainerHeartbeat getHeartbeat()
    {
      return heartbeat;
    }
  }

  public static class StramExecutionEvent extends DAGExecutionEvent
  {
    private final StramEvent stramEvent;

    public StramExecutionEvent(StramEvent stramEvent)
    {
      super(STRAM_EVENT);
      this.stramEvent = stramEvent;
    }

    public StramEvent getStramEvent()
    {
      return stramEvent;
    }
  }

  public static class CommitExecutionEvent extends DAGExecutionEvent
  {
    private final long commitWindow;

    public CommitExecutionEvent(long commitWindow)
    {
      super(COMMIT_EVENT);
      this.commitWindow = commitWindow;
    }

    public long getCommitWindow()
    {
      return commitWindow;
    }
  }

  protected DAGExecutionEvent(DAGExecutionEvent.Type eventType)
  {
    super(eventType);
  }
}
