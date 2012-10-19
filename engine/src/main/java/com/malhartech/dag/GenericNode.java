/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.Operator;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.Operator.OutputPort;
import com.malhartech.api.Sink;
import com.malhartech.util.CircularBuffer;
import com.malhartech.util.UnsafeBlockingQueue;
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
public class GenericNode extends Node<Operator, Sink>
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(GenericNode.class);
  private final HashMap<String, Reservoir> inputs = new HashMap<String, Reservoir>();

  public GenericNode(String id, Operator operator)
  {
    super(id, operator);
  }

  public void handleIdleTimeout()
  {
  }

  class Reservoir extends CircularBuffer<Object> implements Sink
  {
    final Sink sink;
    final String id;

    public Reservoir(String id, Sink sink)
    {
      super(getBufferCapacity());
      this.id = id;
      this.sink = sink;
    }

    @Override
    public final void process(Object payload)
    {
      try {
        put(payload);
      }
      catch (InterruptedException ex) {
        logger.warn("Abandoning processing of the payload {} due to an interrupt", payload);
      }
    }

    final Tuple sweep()
    {
      for (int i = size(); i-- > 0;) {
        if (peekUnsafe() instanceof Tuple) {
          return (Tuple)peekUnsafe();
        }

        sink.process(pollUnsafe());
      }

      return null;
    }
  }

  /**
   *
   * Connect a sink to this node on a port identified by port.
   *
   * @return if the port is input port, Sink object is returned.
   *
   * @param port the value of port
   * @param sink the value of stream
   */
  @Override
  public Sink connect(String port, Sink sink)
  {
    Sink retvalue = null;

    InputPort inputport = descriptor.inputPorts.get(port);
    if (inputport != null) {
      Reservoir cs = inputs.get(port);
      if (sink == null) {
        /**
         * since there are tuples which are not yet processed downstream, rather than just removing
         * the sink, it makes sense to wait for all the data to be processed on this sink and then
         * remove it.
         */
        if (cs != null) {
          cs.process(new EndStreamTuple());
        }
        retvalue = null;
      }
      else {
        inputport.setConnected(true);
        if (cs == null) {
          cs = new Reservoir(port, inputport.getSink());
          inputs.put(port, cs);
        }
        retvalue = cs;
      }
    }

    OutputPort outputPort = descriptor.outputPorts.get(port);
    if (outputPort != null) {
      outputPort.setSink(sink);

      if (sink == null) {
        outputs.remove(port);
      }
      else {
        outputs.put(port, sink);
      }
    }

    return retvalue;
  }

  /**
   * Originally this method was defined in an attempt to implement the interface Runnable.
   *
   * Although it seems that it's called from another thread which implements Runnable, so we take this
   * opportunity to pass the OperatorContext through the run method. Note that activate does not return as
   * long as there is useful workload for the node.
   */
  @Override
  @SuppressWarnings({"SleepWhileInLoop"})
  public final void run()
  {
    int totalQueues = inputs.size();

    ArrayList<Reservoir> activeQueues = new ArrayList<Reservoir>();
    activeQueues.addAll(inputs.values());

    int expectingBeginWindow = activeQueues.size();
    int receivedResetTuples = 0;
    int receivedEndWindow = 0;

    Object lastEndWindow = null;
    try {
      do {
        Iterator<Reservoir> buffers = activeQueues.iterator();
        activequeue:
        while (buffers.hasNext()) {
          Reservoir activePort = buffers.next();
          Tuple t = activePort.sweep();
          if (t != null) {
            switch (t.getType()) {
              case BEGIN_WINDOW:
                if (expectingBeginWindow == totalQueues) {
                  activePort.remove();
                  expectingBeginWindow--;
                  currentWindowId = t.getWindowId();
                  for (int s = sinks.length; s-- > 0;) {
                    sinks[s].process(t);
                  }
                  operator.beginWindow();
                  receivedEndWindow = 0;
                }
                else if (t.getWindowId() == currentWindowId) {
                  activePort.remove();
                  expectingBeginWindow--;
                }
                else {
                  buffers.remove();
                }
                break;

              case END_WINDOW:
                if (t.getWindowId() == currentWindowId) {
                  lastEndWindow = activePort.remove();
                  if (++receivedEndWindow == totalQueues) {
                    operator.endWindow();
                    for (final Sink output: outputs.values()) {
                      output.process(t);
                    }

                    buffers.remove();
                    assert (activeQueues.isEmpty());
                    activeQueues.addAll(inputs.values());
                    expectingBeginWindow = activeQueues.size();

                    handleRequests(currentWindowId);
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
                activePort.remove();

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
                activePort.remove();
                /**
                 * We are not going to receive begin window on this ever!
                 */
                expectingBeginWindow--;
                /**
                 * Since one of the operators we care about it gone, we should relook at our operators.
                 * We need to make sure that the END_STREAM comes outside of the window.
                 */
                totalQueues--;
                inputs.remove(activePort.id);
                descriptor.inputPorts.get(activePort.id).setConnected(false);
                buffers.remove();
                if (totalQueues == 0) {
                  alive = false;
                  break activequeue;
                }
                else if (activeQueues.isEmpty()) {
                  assert (!inputs.isEmpty());
                  assert (lastEndWindow != null);
                  /*
                   * Do the same sequence as the end window since the current window is not ended.
                   */
                  operator.endWindow();
                  for (final Sink output: outputs.values()) {
                    output.process(lastEndWindow);
                  }

                  assert (activeQueues.isEmpty());
                  activeQueues.addAll(inputs.values());
                  expectingBeginWindow = activeQueues.size();

                  handleRequests(currentWindowId);
                  break activequeue;
                }
                break;

              default:
                throw new UnhandledException("Unrecognized Control Tuple", new IllegalArgumentException(t.toString()));
            }
          }
        }

        if (activeQueues.isEmpty()) {
          logger.error("Invalid State - the node blocked forever!!!");
        }
        else {
          int oldCount = 0;
          for (UnsafeBlockingQueue<?> cb: activeQueues) {
            oldCount += cb.size();
          }

          if (oldCount == 0) {
            Thread.sleep(getSpinMillis());

            boolean nodata = true;
            for (UnsafeBlockingQueue<?> cb: activeQueues) {
              if (cb.size() > 0) {
                nodata = false;
                break;
              }
            }

            if (nodata) {
              handleIdleTimeout();
            }
          }
        }
      }
      while (alive);
    }
    catch (InterruptedException ex) {
    }
  }
}
