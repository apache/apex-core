/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.google.common.collect.Maps;

/**
 * Write ahead log for DAG changes.
 * Operations need to be registered with the journal instance before writing.
 * Registered prototype instances will be used to apply changes on read.
 */
public class StramJournal
{
  interface RecoverableOperation
  {
    void read(DataInput in) throws IOException;
    void write(DataOutput out) throws IOException;
  }

  private final ConcurrentMap<Integer, RecoverableOperation> operations = Maps.newConcurrentMap();
  private final ConcurrentMap<Class<?>, Integer> classToId = Maps.newConcurrentMap();
  private final DataOutputStream out;

  public StramJournal(DataOutputStream out)
  {
    this.out = out;
  }

  public void register(int opId, RecoverableOperation op)
  {
    if (operations.put(opId, op) != null) {
      throw new IllegalStateException(String.format("Prior mapping for %s %s", opId));
    }
    classToId.put(op.getClass(), opId);
  }

  public void writeLog(RecoverableOperation op) throws IOException
  {
    Integer classId = classToId.get(op.getClass());
    if (classId == null) {
      throw new IllegalArgumentException("Class not registered " + op.getClass());
    }
    out.writeInt(classId);
    op.write(out);
    out.flush();
  }

  public void replay(DataInputStream in) throws IOException
  {
    int opId;
    while (true) {
      try {
        opId = in.readInt();
      } catch (java.io.EOFException ex) {
        break;
      }
      RecoverableOperation op = operations.get(opId);
      if (op == null) {
        throw new IOException("No reader registered for id " + opId);
      }
      op.read(in);
    }
  }

  public static class SetOperatorState implements RecoverableOperation
  {
    final StreamingContainerManager scm;
    public int operatorId;
    public PTOperator.State state;

    public SetOperatorState(StreamingContainerManager scm)
    {
      this.scm = scm;
    }

    @Override
    public void read(DataInput in) throws IOException
    {
      operatorId = in.readInt();
      int stateOrd = in.readInt();
      state = PTOperator.State.values()[stateOrd];
      scm.getPhysicalPlan().getAllOperators().get(operatorId).setState(state);
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      out.writeInt(operatorId);
      out.writeInt(state.ordinal());
    }

  }

  /**
   * Resource priority is logged so that on restore, pending resource requests can be matched to the containers.
   */
  public static class SetContainerResourcePriority implements RecoverableOperation
  {
    final StreamingContainerManager scm;
    public PTContainer c;

    public SetContainerResourcePriority(StreamingContainerManager scm)
    {
      this.scm = scm;
    }

    @Override
    public void read(DataInput in) throws IOException
    {
      int containerId = in.readInt();
      int priority = in.readInt();
      for (PTContainer c : scm.getPhysicalPlan().getContainers())
      {
         if (c.getId() == containerId) {
           c.setResourceRequestPriority(priority);
           break;
         }
      }
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      out.writeInt(c.getId());
      out.writeInt(c.getResourceRequestPriority());
    }
  }

}
