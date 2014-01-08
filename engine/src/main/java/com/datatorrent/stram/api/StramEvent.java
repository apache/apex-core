/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.api;

import com.datatorrent.stram.plan.logical.LogicalPlanRequest;

/**
 * <p>Abstract StramEvent class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.9.2
 */
public abstract class StramEvent
{
  private long timestamp = System.currentTimeMillis();
  private String reason;

  public abstract String getType();

  public long getTimestamp()
  {
    return timestamp;
  }

  public void setTimestamp(long timestamp)
  {
    this.timestamp = timestamp;
  }

  public String getReason()
  {
    return reason;
  }

  public void setReason(String reason)
  {
    this.reason = reason;
  }

  public abstract static class OperatorEvent extends StramEvent
  {
    private String operatorName;

    public OperatorEvent(String operatorName)
    {
      this.operatorName = operatorName;
    }

    public String getOperatorName()
    {
      return operatorName;
    }

    public void setOperatorName(String operatorName)
    {
      this.operatorName = operatorName;
    }

  }

  public static class SetOperatorPropertyEvent extends OperatorEvent
  {
    private String propertyName;
    private String propertyValue;

    public SetOperatorPropertyEvent(String operatorName, String propertyName, String propertyValue)
    {
      super(operatorName);
      this.propertyName = propertyName;
      this.propertyValue = propertyValue;
    }

    @Override
    public String getType()
    {
      return "SetOperatorProperty";
    }

    public String getPropertyName()
    {
      return propertyName;
    }

    public void setPropertyName(String propertyName)
    {
      this.propertyName = propertyName;
    }

    public String getPropertyValue()
    {
      return propertyValue;
    }

    public void setPropertyValue(String propertyValue)
    {
      this.propertyValue = propertyValue;
    }

  }

  public static class PartitionEvent extends OperatorEvent
  {
    private int oldNumPartitions;
    private int newNumPartitions;

    public PartitionEvent(String operatorName, int oldNumPartitions, int newNumPartitions)
    {
      super(operatorName);
      this.oldNumPartitions = oldNumPartitions;
      this.newNumPartitions = newNumPartitions;
    }

    @Override
    public String getType()
    {
      return "Partition";
    }

    public int getOldNumPartitions()
    {
      return oldNumPartitions;
    }

    public void setOldNumPartitions(int oldNumPartitions)
    {
      this.oldNumPartitions = oldNumPartitions;
    }

    public int getNewNumPartitions()
    {
      return newNumPartitions;
    }

    public void setNewNumPartitions(int newNumPartitions)
    {
      this.newNumPartitions = newNumPartitions;
    }

  }

  public abstract static class PhysicalOperatorEvent extends OperatorEvent
  {
    private int operatorId;

    public PhysicalOperatorEvent(String operatorName, int operatorId)
    {
      super(operatorName);
      this.operatorId = operatorId;
    }

    public int getOperatorId()
    {
      return operatorId;
    }

  }

  public static class CreateOperatorEvent extends PhysicalOperatorEvent
  {
    public CreateOperatorEvent(String operatorName, int operatorId)
    {
      super(operatorName, operatorId);
    }

    @Override
    public String getType()
    {
      return "CreateOperator";
    }

  }

  public static class RemoveOperatorEvent extends PhysicalOperatorEvent
  {
    public RemoveOperatorEvent(String operatorName, int operatorId)
    {
      super(operatorName, operatorId);
    }

    @Override
    public String getType()
    {
      return "RemoveOperator";
    }

  }

  public static class StartOperatorEvent extends PhysicalOperatorEvent
  {
    private String containerId;

    public StartOperatorEvent(String operatorName, int operatorId, String containerId)
    {
      super(operatorName, operatorId);
      this.containerId = containerId;
    }

    @Override
    public String getType()
    {
      return "StartOperator";
    }

    public String getContainerId()
    {
      return containerId;
    }

    public void setContainerId(String containerId)
    {
      this.containerId = containerId;
    }

  }

  public static class StopOperatorEvent extends PhysicalOperatorEvent
  {
    private String containerId;

    public StopOperatorEvent(String operatorName, int operatorId, String containerId)
    {
      super(operatorName, operatorId);
      this.containerId = containerId;
    }

    @Override
    public String getType()
    {
      return "StopOperator";
    }

    public String getContainerId()
    {
      return containerId;
    }

    public void setContainerId(String containerId)
    {
      this.containerId = containerId;
    }

  }

  public static class SetPhysicalOperatorPropertyEvent extends PhysicalOperatorEvent
  {
    private String propertyName;
    private String propertyValue;

    public SetPhysicalOperatorPropertyEvent(String operatorName, int operatorId, String propertyName, String propertyValue)
    {
      super(operatorName, operatorId);
      this.propertyName = propertyName;
      this.propertyValue = propertyValue;
    }

    @Override
    public String getType()
    {
      return "SetPhysicalOperatorProperty";
    }

    public String getPropertyName()
    {
      return propertyName;
    }

    public void setPropertyName(String propertyName)
    {
      this.propertyName = propertyName;
    }

    public String getPropertyValue()
    {
      return propertyValue;
    }

    public void setPropertyValue(String propertyValue)
    {
      this.propertyValue = propertyValue;
    }

  }

  public static class StartContainerEvent extends StramEvent
  {
    String containerId;
    String containerNodeId;

    public StartContainerEvent(String containerId, String containerNodeId)
    {
      this.containerId = containerId;
      this.containerNodeId = containerNodeId;
    }

    @Override
    public String getType()
    {
      return "StartContainer";
    }

    public String getContainerId()
    {
      return containerId;
    }

    public void setContainerId(String containerId)
    {
      this.containerId = containerId;
    }

    public String getContainerNodeId()
    {
      return containerNodeId;
    }

    public void setContainerNodeId(String containerNodeId)
    {
      this.containerNodeId = containerNodeId;
    }

  }

  public static class StopContainerEvent extends StramEvent
  {
    String containerId;
    int exitStatus;

    public StopContainerEvent(String containerId, int exitStatus)
    {
      this.containerId = containerId;
      this.exitStatus = exitStatus;
    }

    @Override
    public String getType()
    {
      return "StopContainer";
    }

    public String getContainerId()
    {
      return containerId;
    }

    public void setContainerId(String containerId)
    {
      this.containerId = containerId;
    }

    public int getExitStatus()
    {
      return exitStatus;
    }

    public void setExitStatus(int exitStatus)
    {
      this.exitStatus = exitStatus;
    }

  }

  public static class ChangeLogicalPlanEvent extends StramEvent
  {
    private LogicalPlanRequest request;

    public ChangeLogicalPlanEvent(LogicalPlanRequest request)
    {
      this.request = request;
    }

    @Override
    public String getType()
    {
      return "ChangeLogicalPlan";
    }

    public LogicalPlanRequest getRequest()
    {
      return request;
    }

    public void setRequest(LogicalPlanRequest request)
    {
      this.request = request;
    }

  }

}
