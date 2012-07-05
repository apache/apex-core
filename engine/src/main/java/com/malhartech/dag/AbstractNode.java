/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer.Data;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractNode implements Node, Sink, Runnable
{

  final HashSet<Sink> outputStreams = new HashSet<Sink>();
  final HashSet<StreamContext> inputStreams = new HashSet<StreamContext>();
  private final PriorityQueue<Tuple> inputQueue;

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

  public AbstractNode()
  {
    this.inputQueue = new PriorityQueue<Tuple>(1024 * 1024, new DataComparator());
  }
  // i feel that we may just want to push the data out from here and depending upon
  // whether the data needs to flow on the stream (as per partitions), the streams
  // create tuples or drop the data on the floor.

  public void emit(Object o)
  {
    for (Sink sink : outputStreams) {
      Tuple t = new Tuple(o);
//      t.setData(ctx.getData());
      sink.doSomething(t);
    }
  }

  public void emitStream(Object o, Sink sink)
  {
    Tuple t = new Tuple(o);
//    t.setData(ctx.getData());
    sink.doSomething(t);
  }

  public void connectOutputStreams(Collection<Sink> sinks)
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

  public void stopSafely()
  {
    alive = false;
  }

  public void run()
  {
    alive = true;

    int canStartNewWindow = 0;
    boolean shouldWait = false;
    long currentWindow = 0;
    int tupleCount = 0;

    while (alive) {
      Tuple t;
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
//          ctx.setData(t.getData());
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
//              process(ctx);
              // this is where we increase the heartbeat counters;
              break;
          }
        }
      }
    }

  }
}
