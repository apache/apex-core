package com.datatorrent.stram.engine;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Operator stats, which forms the root of a tree like structure.
 * Can be serialized as object or marshaled as JSON for easy consumption by client.
 *
 * @since 0.3.2
 */
public class OperatorStats implements Serializable
{
  private static final long serialVersionUID = 1L;

  public static class PortStats implements Serializable
  {
    private static final long serialVersionUID = 1L;
    public final String portname;
    public final int processedCount;
    public final long endWindowTimestamp;

    PortStats(String name, int count, long endWindowTimeStamp)
    {
      this.portname = name;
      this.processedCount = count;
      this.endWindowTimestamp = endWindowTimeStamp;
    }

    @Override
    public String toString()
    {
      return "PortStats{" + "portname=" + portname + ", processedCount=" + processedCount + ", endWindowTimestamp=" + endWindowTimestamp + '}';
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
}
