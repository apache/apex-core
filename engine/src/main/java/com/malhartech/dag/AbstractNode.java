/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.dag;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.LoggerFactory;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.dag.NodeContext.HeartbeatCounters;
import com.malhartech.stram.StramUtils;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.util.StablePriorityQueue;

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
  public void endWindow(NodeContext context)
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
  private final Sink sink = new Sink()
  {
    public void doSomething(Tuple t)
    {
      synchronized (inputQueue) {
//        logger.info(this + " got data " + t.getData());
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

  public void addOutputStreams(Collection<? extends StreamContext> contexts)
  {
    for (StreamContext context : contexts) {
      outputStreams.add(context);
    }
  }

  final private class DataComparator implements Comparator<Tuple>
  {
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
        else if (t1.getType() == Data.DataType.BEGIN_WINDOW) {
          return -1;
        }
        else if (t2.getType() == Data.DataType.BEGIN_WINDOW) {
          return 1;
        }
        else if (t1.getType() == Data.DataType.END_WINDOW) {
          return 1;
        }
        else if (t2.getType() == Data.DataType.END_WINDOW) {
          return -1;
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
    ctx.setCurrentWindowId(0);

    int canStartNewWindow = 0;
    boolean shouldWait = false;
    int tupleCount = 0;

    while (alive && !shouldShutdown()) {
      Tuple t = null;
      synchronized (inputQueue) {
        if ((t = inputQueue.peek()) == null) {
          shouldWait = true;
        }
        else {
          switch (t.getType()) {
            case BEGIN_WINDOW:
              if (canStartNewWindow == 0) {
                tupleCount = 0;
                canStartNewWindow = inputStreams.size();
                inputQueue.poll();
                ctx.setCurrentWindowId(t.getWindowId());
                shouldWait = false;
              }
              else if (t.getWindowId() == ctx.getCurrentWindowId()) {
                shouldWait = false;
              }
              else {
                shouldWait = true;
              }
              break;

            case END_WINDOW:
              if (t.getWindowId() == ctx.getCurrentWindowId()
                  && ((EndWindowTuple) t).getTupleCount() <= tupleCount) {
                tupleCount -= ((EndWindowTuple) t).getTupleCount();
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
              if (t.getType() == Data.DataType.SIMPLE_DATA
                  && t.getWindowId() == ctx.getCurrentWindowId()) {
                tupleCount++;
                inputQueue.poll();
                shouldWait = false;
              }
              else if (t.getType() == Data.DataType.PARTITIONED_DATA
                       && t.getWindowId() == ctx.getCurrentWindowId()) {
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
            logger.error("wait interrupted", ex);
          }
        }
        else {
          /*
           * we process this outside to keep the critical region free.
           */
          switch (t.getType()) {
            case BEGIN_WINDOW:
              beginWindow(ctx);
              for (StreamContext stream : outputStreams) {
                stream.sink(t);
              }
              break;

            case END_WINDOW:
              endWindow(ctx);
              for (StreamContext stream : outputStreams) {
                stream.sink(t);
              }
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

  public static <T extends Stream> T initAdapterStream(StreamConfiguration streamConf, AbstractNode node)
  {
    Map<String, String> properties = streamConf.getDagProperties();
    String className = properties.get(TopologyBuilder.STREAM_CLASSNAME);
    if (className == null) {
      // should have been caught during submit validation
      throw new IllegalArgumentException(String.format("Stream class not configured (key '%s')", TopologyBuilder.STREAM_CLASSNAME));
    }
    try {
      Class<?> clazz = Class.forName(className);
      Class<? extends Stream> subClass = clazz.asSubclass(Stream.class);
      Constructor<? extends Stream> c = subClass.getConstructor();
      @SuppressWarnings("unchecked")
      T instance = (T) c.newInstance();
      // populate custom properties
      BeanUtils.populate(instance, properties);

      instance.setup(streamConf);

      com.malhartech.dag.StreamContext ctx = new com.malhartech.dag.StreamContext();
      if (node == null) {
        /*
         * This is output adapter so it needs to implement the Sink interface.
         */
        if (instance instanceof Sink) {
          ctx.setSink((Sink) instance);
        }
      }
      else {
        Sink sink = node.getSink(ctx);
//        LOG.info("setting sink for instance " + instance + " to " + sink);
        ctx.setSink(sink);
      }

      ctx.setSerde(StramUtils.getSerdeInstance(properties));
      instance.setContext(ctx);

      return instance;
    }
    catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Node class not found: " + className, e);
    }
    catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    }
    catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    }
    catch (SecurityException e) {
      throw new IllegalArgumentException("Error creating instance of class: " + className, e);
    }
    catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Constructor with NodeContext not found: " + className, e);
    }
    catch (InstantiationException e) {
      throw new IllegalArgumentException("Failed to instantiate: " + className, e);
    }
  }
  
  
  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("id", this.ctx.
      getId()).
      toString();
  }
}
