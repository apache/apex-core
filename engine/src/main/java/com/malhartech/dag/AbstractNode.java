/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.dag.NodeContext.HeartbeatCounters;
import com.malhartech.util.StablePriorityQueue;
import java.util.Comparator;
import java.util.HashSet;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.LoggerFactory;

/**
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractNode implements Node, Runnable
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(AbstractNode.class);
  private final HashSet<StreamContext> outputStreams = new HashSet<StreamContext>();
  private final HashSet<StreamContext> inputStreams = new HashSet<StreamContext>();
  private final StablePriorityQueue<Tuple> inputQueue;
  final NodeContext ctx;
  private volatile boolean alive;

  public AbstractNode(NodeContext ctx)
  {
    // initial capacity should be some function of the window length
    this.inputQueue = new StablePriorityQueue<Tuple>(1024 * 1024, new TupleComparator());
    this.ctx = ctx;
  }

  final public NodeContext getContext()
  {
    return ctx;
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
  public void endWindow()
  {
  }

  @Override
  public abstract void process(Object payload);

  @Override
  public void teardown()
  {
  }

  public void handleIdleTimeout()
  {
  }

  /**
   * Return and reset counts for next heartbeat interval. This is called as part of the heartbeat processing. Providing this hook in node implementation so it
   * can be mocked for testing.
   *
   * @return
   */
  public HeartbeatCounters resetHeartbeatCounters()
  {
    return ctx.resetHeartbeatCounters();
  }
  private final Sink sink = new Sink()
  {
    @Override
    public void doSomething(Tuple t)
    {
      synchronized (inputQueue) {
//        logger.info(this + "::doSomething " + t);
        inputQueue.add(t);
        inputQueue.notify();
      }
    }

    @Override
    public String toString()
    {
      return AbstractNode.this.toString();
    }
  };

  public Sink getSink(StreamContext context)
  {
    inputStreams.add(context);
    return sink;
  }

  // this about object sharing among the nodes. it would be nice
  // to know if the object can be shared among multiple downstream
  // nodes. will save on serialization/deserialization etc.
  public void emit(Object o)
  {
    for (StreamContext context : outputStreams) {
      emitStream(o, context);
    }
  }

  public void emitStream(Object o, StreamContext output)
  {
    Tuple t = new Tuple(o);
    t.setWindowId(ctx.getCurrentWindowId());
    t.setType(DataType.SIMPLE_DATA);
    output.sink(t);
  }

  public void addOutputStream(StreamContext context)
  {
    outputStreams.add(context);
  }

  final private class TupleComparator implements Comparator<Tuple>
  {
    @Override
    public int compare(Tuple t1, Tuple t2)
    {
      long wid1 = t1.getWindowId();
      long wid2 = t2.getWindowId();
      if (wid1 < wid2) {
        return -1;
      }
      else if (wid1 > wid2) {
        return 1;
      }

      return t1.getType().compareTo(t2.getType());
    }
  }

  final public void stopSafely()
  {
    alive = false;

    /*
     * Since the thread may be waiting for data to come on the queue, we need to notify. We do not need notifyAll since the queue is not exposed outside.
     */
    synchronized (inputQueue) {
      inputQueue.notify();
    }
  }

  @Override
  final public void run()
  {
    /*
     * We use skip tuple to skip the loops efficiently.
     */
    final Tuple skipTuple = new Tuple(null);
    skipTuple.setType(DataType.NO_DATA);

    alive = true;
    ctx.setCurrentWindowId(0);

    int insideWindowStreamCount = 0;
    do {
      Tuple t;
      synchronized (inputQueue) {
        if ((t = inputQueue.peek()) == null) {
//          logger.info(this + "::run " + t);
          t = skipTuple;
        }
        else {
//          logger.info(this + "::run " + t + " windows = " + insideWindowStreamCount + " context = " + t.getContext());
          switch (t.getType()) {
            case BEGIN_WINDOW:
              if (t.getContext().getSinkState() == StreamContext.State.INSIDE_WINDOW) {
                logger.warn("Got BEGIN_WINDOW while expecting END_WINDOW on {0}", t.getContext());
                t = skipTuple;
              }
              else if (insideWindowStreamCount == 0) {
                t.getContext().setSinkState(StreamContext.State.INSIDE_WINDOW);
                insideWindowStreamCount = 1 - 2 * inputStreams.size();

                /*
                 * This tuple is starting a new window for the node.
                 */
                ctx.setCurrentWindowId(t.getWindowId());

                inputQueue.poll();
              }
              else if (t.getWindowId() == ctx.getCurrentWindowId()) {
                t.getContext().setSinkState(StreamContext.State.INSIDE_WINDOW);
                insideWindowStreamCount++;

                /*
                 * Some other tuple already started this window.
                 */
                t = skipTuple;

                inputQueue.poll();
              }
              else {
                /*
                 * This stream is not moving in synch with the rest of the streams. May be we should just wait for more data.
                 */
                t = skipTuple;
              }
              break;

            case END_WINDOW:
              if (t.getContext().getSinkState() == StreamContext.State.OUTSIDE_WINDOW) {
                logger.warn("Got END_WINDOW while expecting BEGIN_WINDOW on {0}", t.getContext());
                t = skipTuple;
              }
              else if (t.getWindowId() == ctx.getCurrentWindowId()) {
                t.getContext().setSinkState(StreamContext.State.OUTSIDE_WINDOW);
                if (++insideWindowStreamCount < 0) {
                  t = skipTuple;
                }

                inputQueue.poll();
              }
              else {
                t = skipTuple;
              }
              break;

            default:
              if (t.getWindowId() == ctx.getCurrentWindowId()) {
                inputQueue.poll();
              }
              else {
                t = skipTuple;
              }
              break;
          }
        }

        if (t == skipTuple) {
          try {
            int queueSize = inputQueue.size();
            inputQueue.wait(ctx.getIdleTimeout());
            if (inputQueue.size() == queueSize) {
              handleIdleTimeout();
            }
          }
          catch (InterruptedException ex) {
            logger.error("wait interrupted", ex);
          }
        }
      }
      /*
       * we process this outside to keep the critical region free.
       */

      switch (t.getType()) {
        case NO_DATA:
          // special packet which allows us to skip the data processing.
          break;

        case BEGIN_WINDOW:
          beginWindow();
          for (StreamContext stream : outputStreams) {
            stream.sink(t);
          }
          break;

        case PARTITIONED_DATA:
          logger.warn("partitioned data should not be called " + t);

        case SIMPLE_DATA:
          // process payload
          process(t.getObject());
          // update heartbeat counters;
          ctx.countProcessed(t);
          break;

        case END_WINDOW:
          endWindow();
          for (StreamContext stream : outputStreams) {
            stream.sink(t);
          }
          break;

          /*
           * our comparator function guarantees that we are always processing End of Stream 
           * tuple after the end window tuple of the window in which EoS tuple was received.
           */
        case END_STREAM:
          if (inputStreams.remove(t.getContext())) {
            if (inputStreams.isEmpty()) {
              alive = false;
            }
          }
          else {
            logger.error("Got EndOfStream on from a stream which is not registered");
          }
          // find out which stream the packet come on and then remove that from our collection.
          break;

        default:
          logger.warn("got an unhandled packet " + t);
          break;
      }

    } while (alive);

    teardown();
    
    /*
     * since we are going away... we should let all the downstream nodes know that.
     */
    EndStreamTuple est = new EndStreamTuple();
    est.setWindowId(ctx.getCurrentWindowId());
    for (StreamContext stream : outputStreams) {
      stream.sink(est);
    }
  }

  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("id", this.ctx.getId()).
      toString();
  }
}
