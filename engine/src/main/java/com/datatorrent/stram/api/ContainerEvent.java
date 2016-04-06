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

import com.datatorrent.stram.ComponentContextPair;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerStats;
import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.engine.Stream;
import com.datatorrent.stram.engine.StreamContext;

/**
 * Interface for all container events that can be published to and subscribed from an event bus
 * Currently using mbassador event bus
 *
 * @since 0.9.1
 */
public interface ContainerEvent
{
  Class<?>[] CONTAINER_EVENTS_LISTENERS = new Class<?>[]{
      com.datatorrent.stram.engine.BufferServerStatsSubscriber.class,
      com.datatorrent.stram.debug.TupleRecorderCollection.class
  };

  /**
   * Node event used for various events associated with nodes.
   */
  abstract class NodeEvent implements ContainerEvent
  {
    private Node<?> node;

    public NodeEvent(Node<?> node)
    {
      this.node = node;
    }

    public Node<?> getNode()
    {
      return node;
    }

  }

  /**
   * Event representing node activation.
   */
  class NodeActivationEvent extends NodeEvent
  {
    public NodeActivationEvent(Node<?> node)
    {
      super(node);
    }

  }

  /**
   * Event representing node deactivation.
   */
  class NodeDeactivationEvent extends NodeEvent
  {
    public NodeDeactivationEvent(Node<?> node)
    {
      super(node);
    }

  }

  /**
   * Event representing stats in the container from the ContainerStats object.
   */
  class ContainerStatsEvent implements ContainerEvent
  {
    private ContainerStats containerStats;

    public ContainerStatsEvent(ContainerStats containerStats)
    {
      this.containerStats = containerStats;
    }

    public ContainerStats getContainerStats()
    {
      return containerStats;
    }

  }

  /**
   * Event representing streams.
   */
  abstract class StreamEvent implements ContainerEvent
  {
    private ComponentContextPair<Stream, StreamContext> stream;

    public StreamEvent(ComponentContextPair<Stream, StreamContext> stream)
    {
      this.stream = stream;
    }

    public ComponentContextPair<Stream, StreamContext> getStream()
    {
      return stream;
    }

  }

  /**
   * Event representing stream activation.
   */
  class StreamActivationEvent extends StreamEvent
  {
    public StreamActivationEvent(ComponentContextPair<Stream, StreamContext> stream)
    {
      super(stream);
    }

  }

  /**
   * Event representing stream deactivation.
   */
  class StreamDeactivationEvent extends StreamEvent
  {
    public StreamDeactivationEvent(ComponentContextPair<Stream, StreamContext> stream)
    {
      super(stream);
    }

  }

}
