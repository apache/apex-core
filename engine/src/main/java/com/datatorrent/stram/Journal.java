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
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;

import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.google.common.collect.Maps;

/**
 * Write ahead log for DAG changes.
 * Operations need to be registered with the journal instance before writing.
 * Registered prototype instances will be used to apply changes on read.
 *
 * @since 0.9.2
 */
public class Journal
{
  public interface RecoverableOperation
  {
    void read(DataInput in) throws IOException;
    void write(DataOutput out) throws IOException;
  }

  private final ConcurrentMap<Integer, RecoverableOperation> operations = Maps.newConcurrentMap();
  private final ConcurrentMap<Class<?>, Integer> classToId = Maps.newConcurrentMap();
  private DataOutputStream out;

  public Journal()
  {
  }

  public DataOutputStream getOutputStream()
  {
    return this.out;
  }

  public synchronized void setOutputStream(DataOutputStream out) throws IOException
  {
    this.out = out;
  }

  public synchronized void register(int opId, RecoverableOperation op)
  {
    if (operations.put(opId, op) != null) {
      throw new IllegalStateException(String.format("Prior mapping for %s %s", opId));
    }
    classToId.put(op.getClass(), opId);
  }

  public synchronized void write(RecoverableOperation op) throws IOException
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

  private static void writeLPString(DataOutput out, String s) throws IOException
  {
    if (s != null) {
      byte[] bytes = s.getBytes();
      out.writeInt(bytes.length);
      out.write(bytes);
    } else {
      out.writeInt(-1);
    }
  }

  private static String readLPString(DataInput in) throws IOException
  {
    int len = in.readInt();
    if (len == -1) {
      return null;
    }
    byte[] bytes = new byte[len];
    in.readFully(bytes);
    return new String(bytes);
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

    public static RecoverableOperation newInstance(int operatorId, PTOperator.State state)
    {
      SetOperatorState op = new SetOperatorState(null);
      op.operatorId = operatorId;
      op.state = state;
      return op;
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
  public static class SetContainerState implements RecoverableOperation
  {
    final StreamingContainerManager scm;
    public PTContainer container;

    public SetContainerState(StreamingContainerManager scm)
    {
      this.scm = scm;
    }

    public static RecoverableOperation newInstance(PTContainer container)
    {
      SetContainerState op = new SetContainerState(null);
      op.container = container;
      return op;
    }

    @Override
    public void read(DataInput in) throws IOException
    {
      int containerId = in.readInt();

      for (PTContainer c : scm.getPhysicalPlan().getContainers())
      {
         if (c.getId() == containerId) {
           int stateOrd = in.readInt();
           c.setState(PTContainer.State.values()[stateOrd]);
           c.setExternalId(readLPString(in));
           c.setResourceRequestPriority(in.readInt());
           c.setRequiredMemoryMB(in.readInt());
           c.setAllocatedMemoryMB(in.readInt());
           String bufferServerHost = readLPString(in);
           if (bufferServerHost != null) {
             c.bufferServerAddress = InetSocketAddress.createUnresolved(bufferServerHost, in.readInt());
           }
           c.host = readLPString(in);
           c.nodeHttpAddress = readLPString(in);
           break;
         }
      }
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      out.writeInt(container.getId());
      // state
      out.writeInt(container.getState().ordinal());
      // external id
      writeLPString(out, container.getExternalId());
      // resource priority
      out.writeInt(container.getResourceRequestPriority());
      // memory required
      out.writeInt(container.getRequiredMemoryMB());
      // memory allocated
      out.writeInt(container.getAllocatedMemoryMB());
      // buffer server address
      InetSocketAddress addr = container.bufferServerAddress;
      if (addr != null) {
        writeLPString(out, addr.getHostName());
        out.writeInt(addr.getPort());
      } else {
        writeLPString(out, null);
      }
      // host
      writeLPString(out, container.host);
      writeLPString(out, container.nodeHttpAddress);
    }
  }

}
