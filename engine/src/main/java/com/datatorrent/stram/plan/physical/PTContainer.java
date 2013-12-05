/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan.physical;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;


/**
 *
 * Representation of a container for physical objects of DAG to be placed in
 * <p>
 * <br>
 * References the actual container assigned by the resource manager which
 * hosts the streaming operators in the execution layer.<br>
 * The container reference may change throughout the lifecycle of the
 * application due to failure/recovery or scheduler decisions in general. <br>
 *
 * @since 0.3.5
 */
public class PTContainer {
  public enum State {
    NEW,
    ALLOCATED,
    ACTIVE,
    TIMEDOUT,
    KILLED
  }

  volatile PTContainer.State state = State.NEW;
  private int requiredMemoryMB;
  private int allocatedMemoryMB;
  private int resourceRequestPriority;

  private final PhysicalPlan plan;
  private final int seq;
  List<PTOperator> operators = new ArrayList<PTOperator>();
  Set<PTOperator> pendingUndeploy = Collections.newSetFromMap(new ConcurrentHashMap<PTOperator, Boolean>());
  Set<PTOperator> pendingDeploy = Collections.newSetFromMap(new ConcurrentHashMap<PTOperator, Boolean>());

  // execution layer properties
  String containerId; // assigned yarn container id
  public String host;
  public InetSocketAddress bufferServerAddress;
  public String nodeHttpAddress;
  int restartAttempts;

  PTContainer(PhysicalPlan plan) {
    this.plan = plan;
    this.seq = plan.containerSeq.incrementAndGet();
  }

  public PhysicalPlan getPlan() {
    return plan;
  }

  public PTContainer.State getState() {
    return this.state;
  }

  public void setState(PTContainer.State state) {
    this.state = state;
  }

  public int getRequiredMemoryMB() {
    return requiredMemoryMB;
  }

  public void setRequiredMemoryMB(int requiredMemoryMB) {
    this.requiredMemoryMB = requiredMemoryMB;
  }

  public int getAllocatedMemoryMB() {
    return allocatedMemoryMB;
  }

  public void setAllocatedMemoryMB(int allocatedMemoryMB) {
    this.allocatedMemoryMB = allocatedMemoryMB;
  }

  public int getResourceRequestPriority() {
    return resourceRequestPriority;
  }

  public void setResourceRequestPriority(int resourceRequestPriority) {
    this.resourceRequestPriority = resourceRequestPriority;
  }

  public List<PTOperator> getOperators() {
    return operators;
  }

  public String getExternalId() {
    return this.containerId;
  }

  public void setExternalId(String id) {
    this.containerId = id;
  }

  public Set<PTOperator> getPendingUndeploy()
  {
    return pendingUndeploy;
  }

  public Set<PTOperator> getPendingDeploy()
  {
    return pendingDeploy;
  }

  /**
   *
   * @return String
   */
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
        append("id", ""+seq + "(" + this.containerId + ")").
        append("state", this.getState()).
        append("operators", this.operators).
        toString();
  }
}
