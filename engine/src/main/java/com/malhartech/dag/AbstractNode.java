/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.stram.StreamingNodeContext;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author chetan
 */
public abstract class AbstractNode implements Runnable, Node
{

  final StreamingNodeContext ctx;
  final PriorityQueue<Tuple> inputQueue;
  int streamCount;
  boolean alive;

  public AbstractNode(StreamingNodeContext ctx)
  {
    // initial capacity should be some function of the window length
    this.ctx = ctx;
    this.inputQueue = new PriorityQueue<Tuple>(1024 * 1024, new DataComparator());
    this.alive = true;
  }
  private Data currentData = null;

  public void emit(Object o, byte[] partition)
  {
    // identify the output queue 
    Tuple t = null; //new Tuple(o, this);
    //t.setData(currentData);
    final Collection<Tuple> outputQueue = null;
    synchronized (outputQueue) {
      outputQueue.add(t);
      outputQueue.notify();
    }
  }

  public long getWindowId(Data d)
  {
    long windowId;

    switch (d.getType()) {
      case BEGIN_WINDOW:
        windowId = d.getBeginwindow().getWindowId();
        break;

      case END_WINDOW:
        windowId = d.getEndwindow().getWindowId();
        break;

      case SIMPLE_DATA:
        windowId = d.getSimpledata().getWindowId();
        break;

      case PARTITIONED_DATA:
        windowId = d.getPartitioneddata().getWindowId();
        break;

      default:
        windowId = 0;
        break;
    }

    return windowId;
  }

  final class DataComparator implements Comparator<Tuple>
  {

    public int compare(Tuple t, Tuple t1)
    {

      Data d = (Data) t.data;
      Data d1 = (Data) t1.data;
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

  public void run()
  {
    setup(ctx);

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
                canStartNewWindow = streamCount;
                inputQueue.poll();
                currentWindow = d.getBeginwindow().getWindowId();
                shouldWait = false;
              }
              else if (d.getBeginwindow().getWindowId() == currentWindow) {
                shouldWait = false;
              }
              else {
                shouldWait = true;
              }
              break;

            case END_WINDOW:
              if (d.getEndwindow().getWindowId() == currentWindow
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
                  && d.getSimpledata().getWindowId() == currentWindow) {
                tupleCount++;
                inputQueue.poll();
                shouldWait = false;
              }
              else if (d.getType() == Data.DataType.PARTITIONED_DATA
                       && d.getPartitioneddata().getWindowId() == currentWindow) {
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
          currentData = t.data;
          /*
           * we process this outside to keep the critical region free.
           */
          switch (currentData.getType()) {
            case BEGIN_WINDOW:
              beginWindow(currentWindow);
              break;

            case END_WINDOW:
              endWidndow(currentWindow);
              break;

            default:
              process(t);
              break;
          }
        }
      }
    }

    teardown(ctx);
  }
}
