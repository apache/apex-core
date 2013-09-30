/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

import java.io.Serializable;
import java.util.ArrayList;

import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.StreamingNodeHeartbeat;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public interface Stats extends Serializable
{
  public static final long INVALID_TIME_MILLIS = -1;

  public static class ContainerStats implements Stats
  {
    private static final long serialVersionUID = 201309131904L;
    public final String id;
    //public ArrayList<OperatorStats> operators;
    public ArrayList<StreamingNodeHeartbeat> nodes;

    public ContainerStats(String id)
    {
      this.id = id;
      nodes = new ArrayList<StreamingNodeHeartbeat>();
    }

    @Override
    public String toString()
    {
      return "ContainerStats{" + "id=" + id + ", operators=" + nodes + '}';
    }

    public void addNodeStats(StreamingNodeHeartbeat sn)
    {
      nodes.add(sn);
    }

    /**
     * Operator stats, which forms the root of a tree like structure.
     * Can be serialized as object or marshaled as JSON for easy consumption by client.
     *
     * @since 0.3.2
     */
    public static class OperatorStats implements Stats
    {
      private static final long serialVersionUID = 201309131905L;
      public final String id;

      public OperatorStats(int id)
      {
        this.id = String.valueOf(id);
      }

      public static class PortStats implements Stats
      {
        private static final long serialVersionUID = 201309131906L;
        public final String id;
        public int tupleCount;
        public long endWindowTimestamp;
        /**
         * Time in milliseconds returned by System.currentTimeMillis() if recording has started on this component.
         * INVALID_TIME_MILLIS otherwise.
         */
        public long recordingStartTime = INVALID_TIME_MILLIS;

        public PortStats(String id)
        {
          this.id = id;
        }

        @Override
        public String toString()
        {
          return "PortStats{" + "portname=" + id + ", processedCount=" + tupleCount + ", endWindowTimestamp=" + endWindowTimestamp + '}';
        }

      }

      @Override
      public String toString()
      {
        return "OperatorStats{" + "windowId=" + windowId + ", checkpointedWindowId=" + checkpointedWindowId + ", inputPorts=" + inputPorts + ", outputPorts=" + outputPorts + ", cpuTimeUsed=" + cpuTimeUsed + '}';
      }

      public long windowId;
      public long checkpointedWindowId;
      public ArrayList<PortStats> inputPorts;
      public ArrayList<PortStats> outputPorts;
      public long cpuTimeUsed;
      /**
       * Time in milliseconds returned by System.currentTimeMillis() if recording has started on this component.
       * INVALID_TIME_MILLIS otherwise.
       */
      public long recordingStartTime = INVALID_TIME_MILLIS;
    }

  }

}
