/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.dag.NodeContext.HeartbeatCounters;
import com.malhartech.util.StablePriorityQueue;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractNode implements Node, Sink, Runnable
{

  private static int gorder = 0;
  private final HashSet<Sink> outputStreams = new HashSet<Sink>();
  private final HashSet<StreamContext> inputStreams = new HashSet<StreamContext>();
  private final StablePriorityQueue<Tuple> inputQueue;
  final NodeContext ctx;

  public AbstractNode(NodeContext ctx)
  {
    // initial capacity should be some function of the window length
    this.inputQueue = new StablePriorityQueue<Tuple>(1024 * 1024, new DataComparator());
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
  public void beginWindow(NodeContext context)
  {
  }

  @Override
  public void endWidndow(NodeContext context)
  {
  }

  @Override
  public abstract void process(NodeContext context, StreamContext streamContext, Object payload);

  @Override
  public void teardown()
  {
  }

  /**
   * Return and reset counts for next heartbeat interval. This is called as part
   * of the heartbeat processing. Providing this hook in node implementation so
   * it can be mocked for testing.
   *
   * @return
   */
  public HeartbeatCounters resetHeartbeatCounters()
  {
    return ctx.resetHeartbeatCounters();
  }

  public void doSomething(Tuple t)
  {
    synchronized (inputQueue) {
      inputQueue.add(t);
      inputQueue.notify();
    }
  }

  public Sink getSink(StreamContext context)
  {
    inputStreams.add(context);
    return this;
  }

  public void emit(Object o)
  {
    for (Sink sink : outputStreams) {
      Tuple t = new Tuple(o);
      t.setData(ctx.getData());
      sink.doSomething(t);
    }
  }

  public void emitStream(Object o, Sink sink)
  {
    Tuple t = new Tuple(o);
    t.setData(ctx.getData()); /*
     * only wrapper is used; data is ignored
     */
    sink.doSomething(t);
  }

  public void addSink(Sink sink)
  {
    outputStreams.add(sink);
  }

  public void connectOutputStreams(Collection<? extends Sink> sinks)
  {
    for (Sink sink : sinks) {
      outputStreams.add(sink);
    }
  }

  public long getWindowId(Data d)
  {
    long windowId;

    switch (d.getType()) {
      case BEGIN_WINDOW:
        windowId = d.getWindowId();
        break;

      case END_WINDOW:
        windowId = d.getWindowId();
        break;

      case SIMPLE_DATA:
        windowId = d.getWindowId();
        break;

      case PARTITIONED_DATA:
        windowId = d.getWindowId();
        break;

      default:
        windowId = 0;
        break;
    }

    return windowId;
  }

  final private class DataComparator implements Comparator<Tuple>
  {

    public int compare(Tuple t, Tuple t1)
    {

      Data d = t.getData();
      Data d1 = t1.getData();
      if (d != d1) {
        long tid = getWindowId(d);
        long t1id = getWindowId(d1);
        if (tid < t1id) {
          return -1;
        }
        else if (tid > t1id) {
          return 1;
        }
        else if (d.getType() == Data.DataType.BEGIN_WINDOW) {
          return -1;
        }
        else if (d1.getType() == Data.DataType.BEGIN_WINDOW) {
          return 1;
        }
        else if (d.getType() == Data.DataType.END_WINDOW) {
          return 1;
        }
        else if (d1.getType() == Data.DataType.END_WINDOW) {
          return -1;
        }
      }

      return 0;
    }
  }
  private boolean alive;

  final public void stopSafely()
  {
    alive = false;

    /*
     * Since the thread may be waiting for data to come on the queue, we need to
     * notify. We do not need notifyAll since the queue is not exposed outside.
     */
    synchronized (inputQueue) {
      inputQueue.notify();
    }
  }

  /**
   * Hook for node implementation to define custom exit condition. Complementary
   * to external control provided by stopSafely(). For example, node may request
   * shutdown based on external condition unrelated to processing state. Used
   * for testing.
   */
  protected boolean shouldShutdown()
  {
    return false;
  }

  final public void run()
  {
    alive = true;

    int canStartNewWindow = 0;
    boolean shouldWait = false;
    long currentWindow = 0;
    int tupleCount = 0;

    while (alive && !shouldShutdown()) {
      Tuple t = null;
      synchronized (inputQueue) {
        if ((t = inputQueue.peek()) == null) {
          shouldWait = true;
        }
        else {
          Data d = t.getData();
          switch (d.getType()) {
            case BEGIN_WINDOW:
              if (canStartNewWindow == 0) {
                tupleCount = 0;
                canStartNewWindow = inputStreams.size();
                inputQueue.poll();
                currentWindow = d.getWindowId();
                shouldWait = false;
              }
              else if (d.getWindowId() == currentWindow) {
                shouldWait = false;
              }
              else {
                shouldWait = true;
              }
              break;

            case END_WINDOW:
              if (d.getWindowId() == currentWindow
                  && d.getEndwindow().getTupleCount() <= tupleCount) {
                tupleCount -= d.getEndwindow().getTupleCount();
                if (tupleCount == 0) {
                  canStartNewWindow--;
                  inputQueue.poll();
                  shouldWait = false;
                }
              }
              else {
                shouldWait = true;
              }
              break;

            default:
              if (d.getType() == Data.DataType.SIMPLE_DATA
                  && d.getWindowId() == currentWindow) {
                tupleCount++;
                inputQueue.poll();
                shouldWait = false;
              }
              else if (d.getType() == Data.DataType.PARTITIONED_DATA
                       && d.getWindowId() == currentWindow) {
                tupleCount++;
                inputQueue.poll();
                shouldWait = false;
              }
              else {
                shouldWait = true;
              }
              break;
          }
        }

        if (shouldWait) {
          try {
            inputQueue.wait();
          }
          catch (InterruptedException ex) {
            Logger.getLogger(AbstractNode.class.getName()).log(Level.SEVERE, null, ex);
          }
        }
        else {
          ctx.setData(t.getData());
          /*
           * we process this outside to keep the critical region free.
           */
          switch (t.getData().getType()) {
            case BEGIN_WINDOW:
//              beginWindow(ctx);
              emit(null);
              break;

            case END_WINDOW:
//              endWidndow(ctx);
              emit(null);
              break;

            default:
              // process payload
              process(ctx, t.getContext(), t.getObject());
              // update heartbeat counters;
              ctx.countProcessed(t);
              break;
          }
        }
      }
    }

  }

  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("id", this.ctx.getId()).
            toString();
  }
}
