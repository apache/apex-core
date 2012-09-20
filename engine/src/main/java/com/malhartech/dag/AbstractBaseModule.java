/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import java.util.HashMap;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractBaseModule implements Node
{
  protected transient String id;
  protected final transient HashMap<String, Sink> outputs = new HashMap<String, Sink>();
  protected transient int spinMillis = 10;
  protected transient int bufferCapacity = 1024 * 1024;
  protected transient int processedTupleCount;
  @SuppressWarnings(value = "VolatileArrayField")
  protected volatile transient Sink[] sinks = NO_SINKS;

  // optimize the performance of this method.
  protected PortAnnotation getPort(String id)
  {
    Class<? extends Node> clazz = this.getClass();
    NodeAnnotation na = clazz.getAnnotation(NodeAnnotation.class);
    if (na != null) {
      PortAnnotation[] ports = na.ports();
      for (PortAnnotation pa: ports) {
        if (id.equals(pa.name())) {
          return pa;
        }
      }
    }

    return null;
  }

  @SuppressWarnings("SillyAssignment")
  protected void activateSinks()
  {
    sinks = new Sink[outputs.size()];

    int i = 0;
    for (Sink s: outputs.values()) {
      sinks[i++] = s;
    }
    sinks = sinks;
  }

  @Override
  public void deactivate()
  {
    sinks = NO_SINKS;
    outputs.clear();
  }

  @Override
  public void beginWindow()
  {
  }

  /**
   * An opportunity for the derived node to use the connected dagcomponents.
   *
   * Motivation is that the derived node can tie the dagparts to class fields and use them for efficiency reasons instead of asking this class to do lookup.
   *
   * @param id
   * @param dagpart
   */
  public void connected(String id, Sink dagpart)
  {
    /* implementation to be optionally overridden by the user */
  }

  /**
   * Emit the payload to all active output ports
   *
   * @param payload
   */
  public void emit(final Object payload)
  {
    for (int i = sinks.length; i-- > 0;) {
      sinks[i].process(payload);
    }
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final AbstractBaseModule other = (AbstractBaseModule)obj;
    if ((this.id == null) ? (other.id != null) : !this.id.equals(other.id)) {
      return false;
    }
    return true;
  }

  /**
   * @return the bufferCapacity
   */
  public int getBufferCapacity()
  {
    return bufferCapacity;
  }

  public String getId()
  {
    return id;
  }

  /**
   * @return the spinMillis
   */
  public int getSpinMillis()
  {
    return spinMillis;
  }

  @Override
  public int hashCode()
  {
    return id == null ? super.hashCode() : id.hashCode();
  }

  /**
   * @param bufferCapacity the bufferCapacity to set
   */
  public void setBufferCapacity(int bufferCapacity)
  {
    this.bufferCapacity = bufferCapacity;
  }

  public void setId(String id)
  {
    this.id = id;
  }

  /**
   * @param spinMillis the spinMillis to set
   */
  public void setSpinMillis(int spinMillis)
  {
    this.spinMillis = spinMillis;
  }

  @Override
  public void setup(NodeConfiguration config) throws FailedOperationException
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "{id=" + id + '}';
  }

  /**
   * @return the processedTupleCount
   */
  public int getProcessedTupleCount()
  {
    return processedTupleCount;
  }
}
