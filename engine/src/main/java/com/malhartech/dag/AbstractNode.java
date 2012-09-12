/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.util.CircularBuffer;
import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.commons.lang.UnhandledException;
import org.slf4j.LoggerFactory;

/**
 *
 * The base class for node implementation<p>
 * <br>
 * Implements the base interface {@link com.malhartech.dag.Node}<br>
 * <br>
 * This is the basic functional block of the DAG. It is responsible for the following<br>
 * It emits and consumes tuples<br>
 * Upon window boundary it does house cleaning, state sync up etc<br>
 * Interacts with Stram with a heartbeat protocol<br>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractNode implements Node
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(AbstractNode.class);
  private transient String id;
  private transient CompoundSink activePort;
  private transient final HashMap<String, CompoundSink> inputs = new HashMap<String, CompoundSink>();
  private transient final HashMap<String, Sink> outputs = new HashMap<String, Sink>();
  @SuppressWarnings("VolatileArrayField")
  private transient volatile Sink[] sinks = NO_SINKS;
  private transient volatile int consumedTupleCount;
  private transient volatile boolean alive;
  private transient volatile int spinMillis = 10;
  private transient volatile int bufferCapacity = 1024 * 1024;

  // optimize the performance of this method.
  private PortAnnotation getPort(String id)
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

  public final String getActivePort()
  {
    return activePort.id;
  }

  @Override
  public void setup(NodeConfiguration config) throws FailedOperationException
  {
  }

  @Override
  public void beginWindow()
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void teardown()
  {
  }

  public void handleIdleTimeout()
  {
  }

  @SuppressWarnings("SillyAssignment")
  private void activateSinks()
  {
    sinks = new Sink[outputs.size()];
    int i = 0;
    for (Sink s: outputs.values()) {
      sinks[i++] = s;
    }
    sinks = sinks;
  }

  class CompoundSink extends CircularBuffer<Object> implements Sink
  {
    final String id;
    Sink dagpart;

    public CompoundSink(String id, Sink dagpart)
    {
      super(bufferCapacity);
      this.id = id;
      this.dagpart = dagpart;
    }

    @Override
    @SuppressWarnings("SleepWhileInLoop")
    public final void process(Object payload)
    {
//      logger.debug(AbstractNode.this + "::" + this + " got payload " + payload);
      try {
        while (true) {
          try {
            add(payload);
            break;
          }
          catch (BufferOverflowException boe) {
            Thread.sleep(spinMillis);
          }
        }
      }
      catch (InterruptedException ex) {
        /**
         * if we got interrupted while we were sleeping, then there must be emergency. so exit.
         */
      }
    }

    @Override
    public final String toString()
    {
      return id;
    }

    public final Sink getSink()
    {
      return dagpart;
    }
  }

  /**
   *
   * Connect a dagpart to this node on a port identified by id.
   *
   * @return if the port is input port, Sink object is returned.
   *
   * @param id the value of id
   * @param dagpart the value of stream
   */
  @Override
  public Sink connect(String id, Sink dagpart)
  {
    PortAnnotation pa = getPort(id);
    if (pa == null) {
      throw new IllegalArgumentException("Unrecognized Port " + id);
    }

    Sink s;
    switch (pa.type()) {
      case BIDI:
        logger.info("stream is connected to a bidi port, can we have a bidi stream?");
        if (dagpart == null) {
          outputs.remove(pa.name());
        }
        else {
          outputs.put(pa.name(), dagpart);
        }
        if (sinks != NO_SINKS) {
          activateSinks();
        }

      case INPUT:
        if (dagpart == null) {
          inputs.remove(pa.name());
          s = null;
        }
        else {
          CompoundSink cs = inputs.get(pa.name());
          if (cs == null) {
            cs = new CompoundSink(pa.name(), dagpart);
            inputs.put(pa.name(), cs);
          }
          else {
            cs.dagpart = dagpart;
          }
          s = cs;
        }
        break;

      case OUTPUT:
        if (dagpart == null) {
          outputs.remove(pa.name());
        }
        else {
          outputs.put(pa.name(), dagpart);
        }
        if (sinks != NO_SINKS) {
          activateSinks();
        }
        s = null;
        break;

      case DEAD:
      default:
        logger.warn("stream is connected to a dead port!");
        s = null;
        break;
    }

    // irrelevant place : think about window generator logic as well

    connected(pa.name(), dagpart);
    return s;
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
   *
   * A hook for user to do specific checking on a given configuration<p>
   * Basic checking like port connectivity, properties that have to be specified, their ranges etc. would be checked
   * by basic checker<br>
   *
   * @param config
   * @return boolean
   */
  public boolean checkConfiguration(NodeConfiguration config)
  {
    return true;
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

  /**
   * Emit the payload to the specified output port.
   *
   * It's expected that the output port is active, otherwise NullPointerException is thrown.
   *
   * @param id
   * @param payload
   */
  public final void emit(String id, Object payload)
  {
    outputs.get(id).process(payload);
  }

  @Override
  public final void deactivate()
  {
    alive = false;
  }

  /**
   * Originally this method was defined in an attempt to implement the interface Runnable.
   *
   * Although it seems that it's called from another thread which implements Runnable, so we take this
   * opportunity to pass the NodeContext through the run method. Note that activate does not return as
   * long as there is useful workload for the node.
   */
  @Override
  @SuppressWarnings({ "SleepWhileInLoop", "incomplete-switch" })
  public final void activate(NodeContext ctx)
  {
    activateSinks();

    int totalQueues = inputs.size();

    ArrayList<CompoundSink> activeQueues = new ArrayList<CompoundSink>();
    activeQueues.addAll(inputs.values());

    int expectingBeginWindow = activeQueues.size();
    int resetTuples = activeQueues.size();
    int receivedEndWindow = 0;

    boolean shouldWait;
    long currentWindowId = 0;
    alive = true;

    do {
      shouldWait = true;

      Iterator<CompoundSink> buffers = activeQueues.iterator();
      activequeue:
      while (buffers.hasNext()) {
        activePort = buffers.next();

        Object payload;
        while ((payload = activePort.peek()) != null) {
          if (payload instanceof Tuple) {
            final Tuple t = (Tuple)payload;
            switch (t.getType()) {
              case BEGIN_WINDOW:
                if (expectingBeginWindow == totalQueues) {
                  shouldWait = false;
                  activePort.get();
                  expectingBeginWindow--;
                  currentWindowId = t.getWindowId();
                  beginWindow();
                  for (int s = sinks.length; s-- > 0;) {
                    sinks[s].process(t);
                  }
                  receivedEndWindow = 0;
                }
                else if (t.getWindowId() == currentWindowId) {
                  shouldWait = false;
                  activePort.get();
                  expectingBeginWindow--;
                }
                else {
                  buffers.remove();
                }
                break;

              case END_WINDOW:
                if (t.getWindowId() == currentWindowId) {
                  shouldWait = false;
                  activePort.get();
                  if (++receivedEndWindow == totalQueues) {
                    endWindow();
                    for (final Sink output: outputs.values()) {
                      output.process(t);
                    }

                    ctx.report(consumedTupleCount, 0L, currentWindowId);
                    consumedTupleCount = 0;
                    /*
                     * we prefer to do quite a few operations at the end of the window boundary.
                     */
                    // I wanted to take this opportunity to do multiple tasks at the same time
                    // Java recommends using EnumSet. EnumSet is inefficient since I can iterate
                    // over elements but cannot remove them without access to iterator.

                    // the default is UNSPECIFIED which we ignore anyways as we ignore everything
                    // that we do not understand!
                    try {
                      switch (ctx.getRequestType()) {
                        case BACKUP:
                          ctx.backup(this, currentWindowId);
                          break;

                        case RESTORE:
                          logger.info("restore requests are not implemented");
                          break;

                        case TERMINATE:
                          alive = false;
                          break;
                      }
                    }
                    catch (Exception e) {
                      logger.warn("Exception while catering to external request", e.getLocalizedMessage());
                    }

                    expectingBeginWindow = activeQueues.size();

                    buffers.remove();
                    assert (activeQueues.isEmpty());
                    activeQueues.addAll(inputs.values());
                    break activequeue;
                  }
                  else {
                    buffers.remove();
                  }
                }
                else {
                  buffers.remove();
                }
                break;

              case RESET_WINDOW:
                /**
                 * we will receive tuples which are equal to the number of input streams.
                 */
                shouldWait = false;
                activePort.get();
                if (--resetTuples == 0) {
                  for (final Sink output: outputs.values()) {
                    output.process(t);
                  }
                  resetTuples = totalQueues;
                }
                break;

              case END_STREAM:
                shouldWait = false;
                activePort.get();
                /**
                 * Since one of the nodes we care about it gone, we should relook at our nodes.
                 * We need to make sure that the END_STREAM comes outside of the window.
                 */
                totalQueues--;
                inputs.remove(activePort.id); // should i do this, since the stream said that there is nothing more to expect on it.
                buffers.remove();
                if (totalQueues == 0) {
                  alive = false;
                  break activequeue;
                }
                break;

              default:
                throw new UnhandledException("Unrecognized Control Tuple", new IllegalArgumentException(t.toString()));
            }
          }
          else {
            process(activePort.get());
            consumedTupleCount++;
            shouldWait = false;
          }
        }
      }

      if (shouldWait) {
        if (activeQueues.isEmpty()) {
          logger.error("Invalid State - the node is blocked forever!!!");
        }

        int oldCount = 0;
        for (CircularBuffer<?> cb: activeQueues) {
          oldCount += cb.size();
        }
        try {
          Thread.sleep(spinMillis);
          int newCount = 0;
          for (CircularBuffer<?> cb: activeQueues) {
            newCount += cb.size();
          }

          if (oldCount != newCount) {
            handleIdleTimeout();
          }
        }
        catch (InterruptedException ex) {
          /*
           * we got interrupted while we were checking if we need to call handleTimeout.
           * This is exceptional condition since someone is in too much hurry, so we
           * proceed further without actually giving node a chance to handle idle time.
           */
        }
      }
    }
    while (alive);

    logger.debug("{} sending EndOfStream", this);
    /*
     * since we are going away, we should let all the downstream nodes know that.
     */
    // we need to think about this as well.
    EndStreamTuple est = new EndStreamTuple();

    est.setWindowId(currentWindowId);
    for (final Sink output: outputs.values()) {
      output.process(est);
    }
  }

  @Override
  public int hashCode()
  {
    return id == null ? super.hashCode() : id.hashCode();
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
    final AbstractNode other = (AbstractNode)obj;
    if ((this.id == null) ? (other.id != null) : !this.id.equals(other.id)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "{id=" + id + '}';
  }
}
