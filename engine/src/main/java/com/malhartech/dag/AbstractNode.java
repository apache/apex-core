/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.util.CircularBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.commons.lang.UnhandledException;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.LoggerFactory;

/**
 *
 * The base class for node implementation<p>
 * <br>
 * Extends the base interface {@link com.malhartech.dag.InternalNode}<br>
 * <br>
 * This is the basic functional blog of the dag. It is responsible for the following<br>
 * It emits and consumes tuples<br>
 * Upon window boundary it does house cleaning, state sync up etc<br>
 * Interacts with Stram with a heartbeat protocol<br>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractNode implements Node
{
  private String port;
  //@SuppressWarnings("ProtectedField")
  protected transient NodeContext ctx;
  private transient static final org.slf4j.Logger logger = LoggerFactory.getLogger(AbstractNode.class);
  private transient final HashMap<String, CompoundSink> inputs = new HashMap<String, CompoundSink>();
  private transient final HashMap<String, Sink> outputs = new HashMap<String, Sink>();
  private transient int consumedTupleCount;
  private transient volatile boolean alive;

  // optimize the performance of this method.
  private PortType getPortType(String id)
  {
    Class<? extends Node> clazz = this.getClass();
    NodeAnnotation na = clazz.getAnnotation(NodeAnnotation.class);
    if (na != null) {
      PortAnnotation[] ports = na.ports();
      for (PortAnnotation pa: ports) {
        if (id.equals(pa.name())) {
          return pa.type();
        }
      }
    }

    throw new IllegalArgumentException("Port " + id + " not found!");
  }

  @Override
  public void setup(NodeConfiguration config)
  {
  }

  @Override
  public void beginWindow()
  {
  }

  @Override
  public abstract void process(Object payload);

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

  class CompoundSink extends CircularBuffer<Object> implements Sink
  {
    final String id;
    final Sink dagpart;

    public CompoundSink(String id, Sink dagpart)
    {
      super(1024);
      this.id = id;
      this.dagpart = dagpart;
    }

    @Override
    public final void process(Object payload)
    {
      add(payload);
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
    Sink s;
    switch (getPortType(id)) {
      case BIDI:
        logger.info("stream is connected to a bidi port, can we have a bidi stream?");
        outputs.put(id, dagpart);

      case INPUT:
        s = new CompoundSink(id, dagpart);
        inputs.put(id, ((CompoundSink)s));
        break;

      case OUTPUT:
        outputs.put(id, dagpart);
        s = null;
        break;

      case DEAD:
        logger.warn("stream is connected to a dead port!");
        s = null;
        break;

      default:
        throw new IllegalArgumentException("Unrecognized Port");
    }

    connected(id, dagpart);
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
   * Emit the payload to all active output ports
   *
   * @param o
   */
  public void emit(final Object payload)
  {
    for (final Sink d: outputs.values()) {
      d.process(payload);
    }
  }

  /**
   * Emit the payload to the specified output port.
   *
   * It's expected that the output port is active, otherwise NullPointerException is thrown.
   *
   * @param id
   * @param o
   */
  public final void emit(String id, Object payload)
  {
    try {
      outputs.get(id).process(payload);
    }
    catch (Exception e) {
      logger.warn(e.getLocalizedMessage());
    }
  }

  @Override
  final public void deactivate()
  {
    alive = false;
  }

  protected final String getActivePort()
  {
    return port;
  }

  protected final void setActivePort(String port)
  {
    this.port = port;
  }

  /**
   * Originally this method was defined in an attempt to implement the interface Runnable.
   *
   * Although it seems that it's called from another thread which implements Runnable, so we take this
   * opportunity to pass the NodeContext through the run method. Note that activate does not return as
   * long as there is useful workload for the node.
   */
  @Override
  @SuppressWarnings("SleepWhileInLoop")
  final public void activate(NodeContext ctx)
  {
    int totalQueues = inputs.size();
    this.ctx = ctx;

    ArrayList<CompoundSink> activeQueues = new ArrayList<CompoundSink>();
    activeQueues.addAll(inputs.values());

    int expectingBeginWindow = activeQueues.size();
    int resetTuples = activeQueues.size();
    int receivedEndWindow = 0;

    boolean shouldWait;
    ctx.setCurrentWindowId(0);
    alive = true;

    do {
      shouldWait = true;

      Iterator<CompoundSink> buffers = activeQueues.iterator();
      while (buffers.hasNext()) {
        CompoundSink cb = buffers.next();
        Object payload;
        while ((payload = cb.poll()) != null) {
          if (payload instanceof Tuple) {
            final Tuple t = (Tuple)payload;
            switch (t.getType()) {
              case BEGIN_WINDOW:
                if (expectingBeginWindow == totalQueues) {
                  cb.get();
                  expectingBeginWindow--;
                  ctx.setCurrentWindowId(t.getWindowId());
                  beginWindow();
                  for (final Sink output: outputs.values()) {
                    output.process(t);
                  }
                  receivedEndWindow = 0;
                }
                else if (t.getWindowId() == ctx.getCurrentWindowId()) {
                  cb.get();
                  expectingBeginWindow--;
                }
                else {
                  buffers.remove();
                }
                break;

              case END_WINDOW:
                if (t.getWindowId() == ctx.getCurrentWindowId()) {
                  cb.get();
                  if (++receivedEndWindow == totalQueues) {
                    endWindow();
                    for (final Sink output: outputs.values()) {
                      output.process(t);
                    }
                  }
                  ctx.report(consumedTupleCount, 0L);
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
                        ctx.backup(this);
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
                }
                else {
                  buffers.remove();
                }
                break;

              case RESET_WINDOW:
                /**
                 * we will receive tuples which are equal to the number of input streams.
                 */
                cb.get();
                if (--resetTuples == 0) {
                  for (final Sink output: outputs.values()) {
                    output.process(t);
                  }
                  resetTuples = totalQueues;
                }
                break;

              case END_STREAM:
                totalQueues--;
                inputs.remove(getActivePort());
                activeQueues.remove(cb);
                if (totalQueues == 0) {
                  alive = false;
                }
                break;

              default:
                throw new UnhandledException("Unrecognized Control Tuple", new IllegalArgumentException(t.toString()));
            }
          }
          else {
            process(cb.get());
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
          Thread.sleep(100);
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
           * This is excepitional condition since someone is in too much hurry, so we
           * proceed further without actually giving node a chance to handle idle time.
           */
        }
      }
    }
    while (alive);

    /*
     * since we are going away, we should let all the downstream nodes know that.
     */
    EndStreamTuple est = new EndStreamTuple();

    est.setWindowId(ctx.getCurrentWindowId());
    for (final Sink output: outputs.values()) {
      output.process(est);
    }
  }

  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("id", ctx == null ? "unassigned" : ctx.getId()).toString();
  }
}
