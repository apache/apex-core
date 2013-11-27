/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.debug;

import java.util.HashMap;
import net.engio.mbassy.listener.Handler;

import com.datatorrent.api.Component;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.Stats.OperatorStats.PortStats;

import com.datatorrent.stram.ComponentContextPair;
import com.datatorrent.stram.api.ContainerEvent.ContainerStatsEvent;
import com.datatorrent.stram.api.ContainerEvent.StreamActivationEvent;
import com.datatorrent.stram.api.ContainerEvent.StreamDeactivationEvent;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerStats;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat;
import com.datatorrent.stram.engine.ByteCounterStream;
import com.datatorrent.stram.engine.Stream;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.stream.BufferServerPublisher;
import com.datatorrent.stram.stream.BufferServerSubscriber;

/**
 * Subscribes to event bus and listens to buffer server events to collect buffer server stats
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 * @since 0.9.0
 */
public class BufferServerStatsSubscriber
{
  // key: streamId, value: byte counter stream
  private HashMap<String, ByteCounterStream> streams = new HashMap<String, ByteCounterStream>();

  @Handler
  public void handleStreamActivation(StreamActivationEvent sae){
    ComponentContextPair<Stream, StreamContext> stream = sae.getStream();
    if (stream.component instanceof BufferServerSubscriber) {
      streams.put(stream.context.getSinkId(), (ByteCounterStream)stream.component);
    }
    else if (stream.component instanceof BufferServerPublisher) {
      streams.put(stream.context.getSourceId(), (ByteCounterStream)stream.component);
    }
  }

  @Handler
  public void handleStreamDeactivation(StreamDeactivationEvent sde){
    ComponentContextPair<Stream, StreamContext> stream = sde.getStream();
    if (stream.component instanceof BufferServerSubscriber) {
      streams.remove(stream.context.getSinkId());
    }
    else if (stream.component instanceof BufferServerPublisher) {
      streams.remove(stream.context.getSourceId());
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
            String streamId = ((Integer)node.getNodeId()).toString().concat(Component.CONCAT_SEPARATOR).concat(ps.id);
            ByteCounterStream stream = streams.get(streamId);
            if (stream != null) {
              ps.bufferServerBytes = stream.getByteCount(true);
            }
          }
        }

        if (os.outputPorts != null) {
          for (PortStats ps : os.outputPorts) {
            String streamId = ((Integer)node.getNodeId()).toString().concat(Component.CONCAT_SEPARATOR).concat(ps.id);
            ByteCounterStream stream = streams.get(streamId);
            if (stream != null) {
              ps.bufferServerBytes = stream.getByteCount(true);
            }
          }
        }
      }
    }
  }
}
