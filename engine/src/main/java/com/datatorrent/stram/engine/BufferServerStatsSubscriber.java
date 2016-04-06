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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.Stats.OperatorStats.PortStats;

import com.datatorrent.stram.ComponentContextPair;
import com.datatorrent.stram.api.ContainerEvent.ContainerStatsEvent;
import com.datatorrent.stram.api.ContainerEvent.StreamActivationEvent;
import com.datatorrent.stram.api.ContainerEvent.StreamDeactivationEvent;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerStats;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat;

import net.engio.mbassy.listener.Handler;

/**
 * Subscribes to event bus and listens to buffer server events to collect buffer server stats
 *
 * @since 0.9.1
 */
public class BufferServerStatsSubscriber
{
  // key: streamId, value: byte counter stream
  private HashMap<String, ByteCounterStream> inputStreams = new HashMap<>();
  private HashMap<String, List<ByteCounterStream>> outputStreams = new HashMap<>();

  @Handler
  public void handleStreamActivation(StreamActivationEvent sae)
  {
    ComponentContextPair<Stream, StreamContext> stream = sae.getStream();
    String portId = stream.context.getPortId();
    String sinkId = stream.context.getSinkId();
    if (stream.component instanceof ByteCounterStream) {
      if (sinkId.startsWith("tcp:")) {
        List<ByteCounterStream> portStreams = outputStreams.get(portId);
        if (portStreams == null) {
          portStreams = new ArrayList<>();
          outputStreams.put(portId, portStreams);
        }
        portStreams.add((ByteCounterStream)stream.component);
      } else {
        inputStreams.put(portId, (ByteCounterStream)stream.component);
      }
    }
  }

  @Handler
  public void handleStreamDeactivation(StreamDeactivationEvent sde)
  {
    ComponentContextPair<Stream, StreamContext> stream = sde.getStream();
    String portId = stream.context.getPortId();
    String sinkId = stream.context.getSinkId();
    if (stream.component instanceof ByteCounterStream) {
      if (sinkId.startsWith("tcp:")) {
        List<ByteCounterStream> portStreams = outputStreams.get(portId);
        if (portStreams != null) {
          portStreams.remove(stream);
          if (portStreams.size() == 0) {
            outputStreams.remove(portId);
          }
        }
      } else {
        inputStreams.remove(portId);
      }
    }
  }

  @Handler
  public void handleContainerStats(ContainerStatsEvent cse)
  {
    ContainerStats stats = cse.getContainerStats();

    for (OperatorHeartbeat node : stats.operators) {
      for (OperatorStats os : node.windowStats) {
        if (os.inputPorts != null) {
          for (PortStats ps : os.inputPorts) {
            ByteCounterStream stream = inputStreams.get(ps.id);
            if (stream != null) {
              ps.bufferServerBytes = stream.getByteCount(true);
            }
          }
        }

        if (os.outputPorts != null) {
          for (PortStats ps : os.outputPorts) {
            List<ByteCounterStream> portStreams = outputStreams.get(ps.id);
            if (outputStreams != null) {
              ps.bufferServerBytes = 0;
              for (ByteCounterStream stream : portStreams) {
                ps.bufferServerBytes = stream.getByteCount(true);
              }
            }
          }
        }
      }
    }
  }

}
