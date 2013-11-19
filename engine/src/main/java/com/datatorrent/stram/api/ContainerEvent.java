/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
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
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public interface ContainerEvent
{
  /**
   * Node event used for various events associated with nodes.
   */
  public static abstract class NodeEvent implements ContainerEvent
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
  public static class NodeActivationEvent extends NodeEvent
  {
    public NodeActivationEvent(Node<?> node)
    {
      super(node);
    }
  }

  /**
   * Event representing node deactivation.
   */
  public static class NodeDeactivationEvent extends NodeEvent
  {
    public NodeDeactivationEvent(Node<?> node)
    {
      super(node);
    }
  }

  /**
   * Event representing stats in the container from the ContainerStats object.
   */
  public static class ContainerStatsEvent implements ContainerEvent
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
  public static abstract class StreamEvent implements ContainerEvent
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
  public static class StreamActivationEvent extends StreamEvent
  {
    public StreamActivationEvent(ComponentContextPair<Stream, StreamContext> stream)
    {
      super(stream);
    }
  }

  /**
   * Event representing stream deactivation.
   */
  public static class StreamDeactivationEvent extends StreamEvent
  {
    public StreamDeactivationEvent(ComponentContextPair<Stream, StreamContext> stream)
    {
      super(stream);
    }
  }
}
