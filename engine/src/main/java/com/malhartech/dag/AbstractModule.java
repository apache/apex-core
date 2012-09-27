/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.annotation.PortAnnotation;
import com.malhartech.util.CircularBuffer;
import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.commons.lang.UnhandledException;
import org.slf4j.LoggerFactory;

// inflight changes to the port connections should be captured.
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
public abstract class AbstractModule extends AbstractBaseModule
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(AbstractModule.class);
  private transient CompoundSink activePort;
  private transient final HashMap<String, CompoundSink> inputs = new HashMap<String, CompoundSink>();
  private transient boolean alive;

  public final String getActivePort()
  {
    return activePort.id;
  }

  public void handleIdleTimeout()
  {
  }

  class CompoundSink extends CircularBuffer<Object> implements Sink
  {
    final String id;
    Sink dagpart;

    public CompoundSink(String id, Sink dagpart)
    {
      super(getBufferCapacity());
      this.id = id;
      this.dagpart = dagpart;
    }

    @Override
    @SuppressWarnings("SleepWhileInLoop")
    public final void process(Object payload)
    {
//      if (AbstractModule.this.id.endsWith("adviewsNode")) {
//        logger.debug(AbstractModule.this + "::" + this + " got payload " + payload);
//      }
      try {
        while (true) {
          try {
            add(payload);
            break;
          }
          catch (BufferOverflowException boe) {
            Thread.sleep(getSpinMillis());
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
      throw new IllegalArgumentException("Unrecognized Port " + id + " for " + this);
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
        CompoundSink cs = inputs.get(pa.name());
        if (dagpart == null) {
          /**
           * since there are tuples which are not yet processed downstream, rather than just removing
           * the sink, it makes sense to wait for all the data to be processed on this sink and then
           * remove it.
           */
          if (cs != null) {
            cs.process(new EndStreamTuple());
          }
          s = null;
        }
        else {
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
   *
   * A hook for user to do specific checking on a given configuration<p>
   * Basic checking like port connectivity, properties that have to be specified, their ranges etc. would be checked
   * by basic checker<br>
   *
   * @param config
   * @return boolean
   */
  public boolean checkConfiguration(ModuleConfiguration config)
  {
    return true;
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
    final Sink s = outputs.get(id);
    if (s != null) {
      outputs.get(id).process(payload);
    }
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
   * opportunity to pass the ModuleContext through the run method. Note that activate does not return as
   * long as there is useful workload for the node.
   */
  @Override
  @SuppressWarnings({"SleepWhileInLoop"})
  public final void activate(ModuleContext ctx)
  {
    activateSinks();

    int totalQueues = inputs.size();

    ArrayList<CompoundSink> activeQueues = new ArrayList<CompoundSink>();
    activeQueues.addAll(inputs.values());

    int expectingBeginWindow = activeQueues.size();
    int receivedResetTuples = 0;
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
        activeport:
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
                  break activeport;
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

                    ctx.report(getProcessedTupleCount(), 0L, currentWindowId);
                    processedTupleCount = 0;

                    /*
                     * we prefer to cater to requests at the end of the window boundary.
                     */
                    try {
                      CircularBuffer<ModuleContext.ModuleRequest> requests = ctx.getRequests();
                      for (int i = requests.size(); i-- > 0;) {
                        requests.get().execute(this, ctx.getId(), ((Tuple)payload).getWindowId());
                      }
                    }
                    catch (Exception e) {
                      logger.warn("Exception while catering to external request {}", e);
                    }

                    // report window as complete after control operations are completed
                    buffers.remove();
                    assert (activeQueues.isEmpty());
                    activeQueues.addAll(inputs.values());
                    expectingBeginWindow = activeQueues.size();
                    break activequeue;
                  }
                  else {
                    buffers.remove();
                    break activeport;
                  }
                }
                else {
                  buffers.remove();
                  break activeport;
                }

              case RESET_WINDOW:
                /**
                 * we will receive tuples which are equal to the number of input streams.
                 */
                shouldWait = false;
                activePort.get();

                if (receivedResetTuples++ == 0) {
                  for (int s = sinks.length; s-- > 0;) {
                    sinks[s].process(t);
                  }
                }
                else if (receivedResetTuples == activeQueues.size()) {
                  receivedResetTuples = 0;
                }

                break;

              case END_STREAM:
                shouldWait = false;
                activePort.get();
                /**
                 * Since one of the operators we care about it gone, we should relook at our operators.
                 * We need to make sure that the END_STREAM comes outside of the window.
                 */
                totalQueues--;
                inputs.remove(activePort.id); // should i do this, since the stream said that there is nothing more to expect on it.
                buffers.remove();
                if (totalQueues == 0) {
                  alive = false;
                  break activequeue;
                }
                break activeport;

              default:
                throw new UnhandledException("Unrecognized Control Tuple", new IllegalArgumentException(t.toString()));
            }
          }
          else {
            process(activePort.get());
            processedTupleCount++;
            shouldWait = false;
          }
        }
      }

      if (shouldWait) {
        if (activeQueues.isEmpty()) {
          logger.error("Invalid State - the node blocked forever!!!");
        }

        int oldCount = 0;
        for (CircularBuffer<?> cb: activeQueues) {
          oldCount += cb.size();
        }
        try {
          Thread.sleep(getSpinMillis());
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
     * since we are going away, we should let all the downstream operators know that.
     */
    // we need to think about this as well.
    EndStreamTuple est = new EndStreamTuple();

    est.setWindowId(currentWindowId);
    for (final Sink output: outputs.values()) {
      output.process(est);
    }

    deactivateSinks();
  }
}
