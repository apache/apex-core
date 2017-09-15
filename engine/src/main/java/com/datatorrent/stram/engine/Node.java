/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.engine;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.common.util.AsyncStorageAgent;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.math.IntMath;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.Sink;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StatsListener.OperatorRequest;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.common.util.Pair;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.api.OperatorDeployInfo;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerStats;
import com.datatorrent.stram.debug.MuxSink;
import com.datatorrent.stram.plan.logical.Operators;
import com.datatorrent.stram.plan.logical.Operators.PortContextPair;
import com.datatorrent.stram.plan.logical.Operators.PortMappingDescriptor;
import com.datatorrent.stram.tuple.EndStreamTuple;
import com.datatorrent.stram.tuple.EndWindowTuple;

/**
 * <p>
 * Abstract Node class.</p>
 *
 * @param <OPERATOR>
 * @since 0.3.2
 */
public abstract class Node<OPERATOR extends Operator> implements Component<OperatorContext>, Runnable
{
  /**
   * if the Component is capable of taking only 1 input, call it INPUT.
   */
  public static final String INPUT = "input";
  /**
   * if the Component is capable of providing only 1 output, call it OUTPUT.
   */
  public static final String OUTPUT = "output";
  protected int APPLICATION_WINDOW_COUNT; /* this is write once variable */

  protected int DAG_CHECKPOINT_WINDOW_COUNT; /* this is write once variable */

  protected int CHECKPOINT_WINDOW_COUNT; /* this is write once variable */

  protected boolean DATA_TUPLE_AWARE; /* this is write once variable */

  protected int id;
  protected final HashMap<String, Sink<Object>> outputs;
  @SuppressWarnings(value = "VolatileArrayField")
  protected volatile Sink<Object>[] sinks = Sink.NO_SINKS;
  protected boolean alive;
  protected final OPERATOR operator;
  protected final PortMappingDescriptor descriptor;
  public long currentWindowId;
  protected long endWindowEmitTime;
  protected long lastSampleCpuTime;
  protected ThreadMXBean tmb;
  protected HashMap<SweepableReservoir, Long> endWindowDequeueTimes; // end window dequeue time for input ports
  protected Checkpoint checkpoint;
  public int applicationWindowCount;
  public int checkpointWindowCount;
  public int nextCheckpointWindowCount;
  public int dagCheckpointOffsetCount;
  protected int controlTupleCount;
  public final OperatorContext context;
  public final BlockingQueue<StatsListener.OperatorResponse> commandResponse;
  private final List<Field> metricFields;
  private final Map<String, Method> metricMethods;
  private ExecutorService executorService;
  private Queue<Pair<FutureTask<Stats.CheckpointStats>, CheckpointWindowInfo>> taskQueue;
  protected Stats.CheckpointStats checkpointStats;
  public long firstWindowMillis;
  public long windowWidthMillis;

  protected boolean reuseOperator;

  public Node(OPERATOR operator, OperatorContext context)
  {
    this.operator = operator;
    this.context = context;
    executorService = Executors.newSingleThreadExecutor();
    taskQueue = new LinkedList<>();

    outputs = new HashMap<>();

    descriptor = new PortMappingDescriptor();
    Operators.describe(operator, descriptor);

    endWindowDequeueTimes = new HashMap<>();
    tmb = ManagementFactory.getThreadMXBean();
    commandResponse = new LinkedBlockingQueue<>();

    metricFields = Lists.newArrayList();
    for (Field field : ReflectionUtils.getDeclaredFieldsIncludingInherited(operator.getClass())) {
      if (field.isAnnotationPresent(AutoMetric.class)) {
        metricFields.add(field);
        field.setAccessible(true);
      }
    }

    metricMethods = Maps.newHashMap();
    try {
      for (PropertyDescriptor pd : Introspector.getBeanInfo(operator.getClass()).getPropertyDescriptors()) {
        Method readMethod = pd.getReadMethod();
        if (readMethod != null) {
          AutoMetric rfa = readMethod.getAnnotation(AutoMetric.class);
          if (rfa != null) {
            metricMethods.put(pd.getName(), readMethod);
          }
        }
      }
    } catch (IntrospectionException e) {
      throw new RuntimeException("introspecting {}", e);
    }
  }

  public Operator getOperator()
  {
    return operator;
  }

  public void setReuseOperator(boolean reuseOperator)
  {
    this.reuseOperator = reuseOperator;
  }

  @Override
  public void setup(OperatorContext context)
  {
    shutdown = false;
    if (!reuseOperator) {
      logger.debug("Operator Context = {}", context);
      operator.setup(context);
    }
//    this is where the ports should be setup but since the
//    portcontext is not available here, we are doing it in
//    StramChild. In future version, we should move that code here
//    for (InputPort<?> port : descriptor.inputPorts.values()) {
//      port.setup(null);
//    }
//
//    for (OutputPort<?> port : descriptor.outputPorts.values()) {
//      port.setup(null);
//    }
  }

  @Override
  public void teardown()
  {
    for (PortContextPair<InputPort<?>> pcpair : descriptor.inputPorts.values()) {
      pcpair.component.teardown();
    }

    for (PortContextPair<OutputPort<?>> pcpair : descriptor.outputPorts.values()) {
      pcpair.component.teardown();
    }

    if (executorService != null) {
      executorService.shutdown();
      boolean terminated = false;
      try {
        terminated = executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        logger.debug("Wait for graceful executor service {} shutdown interrupted for node {}", executorService, this, e);
      }
      if (!terminated) {
        logger.warn("Shutting down executor service {} for node {}", executorService, this);
        executorService.shutdownNow();
      }
    }
    operator.teardown();
  }

  public PortMappingDescriptor getPortMappingDescriptor()
  {
    return descriptor;
  }

  public void connectOutputPort(String port, final Sink<Object> sink)
  {
    PortContextPair<OutputPort<?>> outputPort = descriptor.outputPorts.get(port);
    if (outputPort != null) {
      if (sink == null) {
        outputPort.component.setSink(null);
        outputs.remove(port);
      } else {
        outputPort.component.setSink(sink);
        outputs.put(port, sink);
      }
    }
  }

  public abstract void connectInputPort(String port, final SweepableReservoir reservoir);

  @SuppressWarnings({"unchecked"})
  public void addSinks(Map<String, Sink<Object>> sinks)
  {
    boolean changes = false;
    for (Entry<String, Sink<Object>> e : sinks.entrySet()) {
      /* make sure that we ignore all the input ports */
      PortContextPair<OutputPort<?>> pcpair = descriptor.outputPorts.get(e.getKey());
      if (pcpair == null) {
        continue;
      }
      changes = true;

      Sink<Object> ics = outputs.get(e.getKey());
      if (ics == null) {
        pcpair.component.setSink(e.getValue());
        outputs.put(e.getKey(), e.getValue());
        changes = true;
      } else if (ics instanceof MuxSink) {
        ((MuxSink)ics).add(e.getValue());
      } else {
        MuxSink muxSink = new MuxSink(ics, e.getValue());
        pcpair.component.setSink(muxSink);
        outputs.put(e.getKey(), muxSink);
        changes = true;
      }
    }

    if (changes) {
      activateSinks();
    }
  }

  public void removeSinks(Map<String, Sink<Object>> sinks)
  {
    boolean changes = false;
    for (Entry<String, Sink<Object>> e : sinks.entrySet()) {
      /* make sure that we ignore all the input ports */
      PortContextPair<OutputPort<?>> pcpair = descriptor.outputPorts.get(e.getKey());
      if (pcpair == null) {
        continue;
      }

      Sink<Object> ics = outputs.get(e.getKey());
      if (ics == e.getValue()) {
        pcpair.component.setSink(null);
        outputs.remove(e.getKey());
        changes = true;
      } else if (ics instanceof MuxSink) {
        MuxSink ms = (MuxSink)ics;
        ms.remove(e.getValue());
        Sink<Object>[] sinks1 = ms.getSinks();
        if (sinks1.length == 0) {
          pcpair.component.setSink(null);
          outputs.remove(e.getKey());
          changes = true;
        } else if (sinks1.length == 1) {
          pcpair.component.setSink(sinks1[0]);
          outputs.put(e.getKey(), sinks1[0]);
          changes = true;
        }
      }
    }

    if (changes) {
      activateSinks();
    }
  }

  protected ProcessingMode PROCESSING_MODE;
  protected volatile boolean shutdown;

  /**
   * Shutdown the current node.
   * If processEndWindow is set to true, then after shutting down, endWindow is called and
   * END_STREAM tuple is sent to downstream operators.
   *
   * @param processEndWindow
   */
  public void shutdown(boolean processEndWindow)
  {
    if (!processEndWindow) {
      shutdown = true;
    }

    synchronized (this) {
      alive = false;
    }

    if (context == null) {
      logger.warn("Shutdown requested when context is not available!");
    } else {
      /*
       * Since alive is non-volatile this code explicitly unsets it in the operator lifecycle theread thereby notifying
       * it even when the thread is reading it from the cache
       */
      context.request(new OperatorRequest()
      {
        @Override
        public StatsListener.OperatorResponse execute(Operator operator, int operatorId, long windowId) throws IOException
        {
          alive = false;
          return null;
        }

      });
    }
  }

  public void shutdown()
  {
    shutdown(false);
  }

  @Override
  public String toString()
  {
    return String.valueOf(getId());
  }

  protected void emitEndStream()
  {
    logger.info("{} sending EndOfStream", this);
    /*
     * since we are going away, we should let all the downstream operators know that.
     */
    EndStreamTuple est = new EndStreamTuple(currentWindowId);
    for (final Sink<Object> output : outputs.values()) {
      output.put(est);
    }
    controlTupleCount++;
  }

  protected void emitEndWindow()
  {
    long windowId = (operator instanceof Operator.DelayOperator) ?
        WindowGenerator.getAheadWindowId(currentWindowId, firstWindowMillis, windowWidthMillis, 1) : currentWindowId;
    EndWindowTuple ewt = new EndWindowTuple(windowId);
    for (int s = sinks.length; s-- > 0; ) {
      sinks[s].put(ewt);
    }
    controlTupleCount++;
  }

  protected void handleRequests(long windowId)
  {
    /*
     * we prefer to cater to requests at the end of the window boundary.
     */
    try {
      BlockingQueue<OperatorRequest> requests = context.getRequests();
      int size;
      StatsListener.OperatorResponse response;
      if ((size = requests.size()) > 0) {
        while (size-- > 0) {
          //logger.debug("endwindow: " + t.getWindowId() + " lastprocessed: " + context.getLastProcessedWindowId());
          response = requests.remove().execute(operator, context.getId(), windowId);
          if (response != null) {
            commandResponse.add(response);
          }
        }
      }
    } catch (Error er) {
      throw er;
    } catch (RuntimeException re) {
      throw re;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected Map<String, Object> collectMetrics()
  {
    if (context.areMetricsListed() && (context.metricsToSend == null || context.metricsToSend.isEmpty())) {
      return null;
    }
    Map<String, Object> metricValues = Maps.newHashMap();
    try {
      for (Field field : metricFields) {
        if (context.metricsToSend != null && !context.metricsToSend.contains(field.getName())) {
          continue;
        }
        metricValues.put(field.getName(), field.get(operator));
      }

      for (Map.Entry<String, Method> methodEntry : metricMethods.entrySet()) {
        if (context.metricsToSend != null && !context.metricsToSend.contains(methodEntry.getKey())) {
          continue;
        }
        metricValues.put(methodEntry.getKey(), methodEntry.getValue().invoke(operator));
      }

      context.clearMetrics();
      return metricValues;
    } catch (IllegalAccessException | InvocationTargetException iae) {
      throw new RuntimeException(iae);
    }
  }

  protected void reportStats(ContainerStats.OperatorStats stats, long windowId)
  {
    stats.outputPorts = new ArrayList<>();
    for (Entry<String, Sink<Object>> e : outputs.entrySet()) {
      ContainerStats.OperatorStats.PortStats portStats = new ContainerStats.OperatorStats.PortStats(e.getKey());
      portStats.tupleCount = e.getValue().getCount(true) - controlTupleCount;
      portStats.endWindowTimestamp = endWindowEmitTime;
      stats.outputPorts.add(portStats);
    }
    controlTupleCount = 0;

    long currentCpuTime = tmb.getCurrentThreadCpuTime();
    stats.cpuTimeUsed = currentCpuTime - lastSampleCpuTime;
    lastSampleCpuTime = currentCpuTime;

    if (checkpoint != null) {
      stats.checkpoint = checkpoint;
      stats.checkpointStats = checkpointStats;
      checkpointStats = null;
      checkpoint = null;
    } else {
      Pair<FutureTask<Stats.CheckpointStats>, CheckpointWindowInfo> pair = taskQueue.peek();
      if (pair != null && pair.getFirst().isDone()) {
        taskQueue.poll();
        try {
          CheckpointWindowInfo checkpointWindowInfo = pair.getSecond();
          stats.checkpointStats = pair.getFirst().get();
          stats.checkpoint = new Checkpoint(checkpointWindowInfo.windowId, checkpointWindowInfo.applicationWindowCount,
              checkpointWindowInfo.checkpointWindowCount);
          if (operator instanceof Operator.CheckpointListener) {
            ((Operator.CheckpointListener)operator).checkpointed(checkpointWindowInfo.windowId);
          }
        } catch (Exception ex) {
          throw Throwables.propagate(ex);
        }
      }
    }

    context.report(stats, windowId);
  }

  protected void activateSinks()
  {
    int size = outputs.size();
    if (size == 0) {
      sinks = Sink.NO_SINKS;
    } else {
      @SuppressWarnings("unchecked")
      Sink<Object>[] newSinks = (Sink<Object>[])Array.newInstance(Sink.class, size);
      for (Sink<Object> s : outputs.values()) {
        newSinks[--size] = s;
      }

      sinks = newSinks;
    }
  }

  protected void deactivateSinks()
  {
    sinks = Sink.NO_SINKS;
  }

  void checkpoint(long windowId)
  {
    if (!context.stateless) {
      if (operator instanceof Operator.CheckpointNotificationListener) {
        ((Operator.CheckpointNotificationListener)operator).beforeCheckpoint(windowId);
      }

      StorageAgent ba = context.getValue(OperatorContext.STORAGE_AGENT);
      if (ba != null) {
        try {
          checkpointStats = new Stats.CheckpointStats();
          checkpointStats.checkpointStartTime = System.currentTimeMillis();
          ba.save(operator, id, windowId);
          if (ba instanceof AsyncStorageAgent) {
            AsyncStorageAgent asyncStorageAgent = (AsyncStorageAgent)ba;
            if (!asyncStorageAgent.isSyncCheckpoint()) {
              if (PROCESSING_MODE != ProcessingMode.EXACTLY_ONCE) {
                CheckpointWindowInfo checkpointWindowInfo = new CheckpointWindowInfo();
                checkpointWindowInfo.windowId = windowId;
                checkpointWindowInfo.applicationWindowCount = applicationWindowCount;
                checkpointWindowInfo.checkpointWindowCount = checkpointWindowCount;
                CheckpointHandler checkpointHandler = new CheckpointHandler();
                checkpointHandler.agent = asyncStorageAgent;
                checkpointHandler.operatorId = id;
                checkpointHandler.windowId = windowId;
                checkpointHandler.stats = checkpointStats;
                FutureTask<Stats.CheckpointStats> futureTask = new FutureTask<>(checkpointHandler);
                taskQueue.add(new Pair<>(futureTask, checkpointWindowInfo));
                executorService.submit(futureTask);
                checkpoint = null;
                checkpointStats = null;
                return;
              } else {
                asyncStorageAgent.flush(id, windowId);
              }
            }
          }
          checkpointStats.checkpointTime = System.currentTimeMillis() - checkpointStats.checkpointStartTime;
        } catch (IOException ie) {
          try {
            logger.warn("Rolling back checkpoint {} for Operator {} due to the exception {}",
                Codec.getStringWindowId(windowId), operator, ie);
            ba.delete(id, windowId);
          } catch (IOException ex) {
            logger.warn("Error while rolling back checkpoint", ex);
          }
          throw new RuntimeException(ie);
        }
      }
    }

    calculateNextCheckpointWindow();
    dagCheckpointOffsetCount = 0;
    checkpoint = new Checkpoint(windowId, applicationWindowCount, checkpointWindowCount);
    if (operator instanceof Operator.CheckpointListener) {
      ((Operator.CheckpointListener)operator).checkpointed(windowId);
    }
  }

  protected void calculateNextCheckpointWindow()
  {
    if (PROCESSING_MODE != ProcessingMode.EXACTLY_ONCE) {
      nextCheckpointWindowCount = ((DAG_CHECKPOINT_WINDOW_COUNT - dagCheckpointOffsetCount + CHECKPOINT_WINDOW_COUNT - 1) / CHECKPOINT_WINDOW_COUNT) * CHECKPOINT_WINDOW_COUNT;
    } else {
      nextCheckpointWindowCount = 1;
    }
  }

  @SuppressWarnings("unchecked")
  public static Node<?> retrieveNode(Object operator, OperatorContext context, OperatorDeployInfo.OperatorType type)
  {
    logger.debug("type={}, operator class={}", type, operator.getClass());

    Node<?> node;
    if (operator instanceof InputOperator && type == OperatorDeployInfo.OperatorType.INPUT) {
      node = new InputNode((InputOperator)operator, context);
    } else if (operator instanceof Unifier && type == OperatorDeployInfo.OperatorType.UNIFIER) {
      node = new UnifierNode((Unifier<Object>)operator, context);
    } else if (type == OperatorDeployInfo.OperatorType.OIO) {
      node = new OiONode((Operator)operator, context);
    } else {
      node = new GenericNode((Operator)operator, context);
    }

    return node;
  }

  /**
   * @return the id
   */
  public int getId()
  {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId(int id)
  {
    if (this.id == 0) {
      this.id = id;
    } else {
      throw new RuntimeException("Id cannot be changed from " + this.id + " to " + id);
    }
  }

  @SuppressWarnings("unchecked")
  public void activate()
  {
    alive = true;
    APPLICATION_WINDOW_COUNT = context.getValue(OperatorContext.APPLICATION_WINDOW_COUNT);
    if (context.getValue(OperatorContext.SLIDE_BY_WINDOW_COUNT) != null) {
      int slidingWindowCount = context.getValue(OperatorContext.SLIDE_BY_WINDOW_COUNT);
      APPLICATION_WINDOW_COUNT = IntMath.gcd(APPLICATION_WINDOW_COUNT, slidingWindowCount);
    }
    DAG_CHECKPOINT_WINDOW_COUNT = context.getValue(Context.DAGContext.CHECKPOINT_WINDOW_COUNT);
    CHECKPOINT_WINDOW_COUNT = context.getValue(OperatorContext.CHECKPOINT_WINDOW_COUNT);
    Collection<StatsListener> statsListeners = context.getValue(OperatorContext.STATS_LISTENERS);

    if (CHECKPOINT_WINDOW_COUNT % APPLICATION_WINDOW_COUNT != 0) {
      logger.warn("{} is not exact multiple of {} for operator {}. This may cause side effects such as processing to begin without beginWindow preceding it in the first window after activation.",
          OperatorContext.CHECKPOINT_WINDOW_COUNT,
          OperatorContext.APPLICATION_WINDOW_COUNT,
          operator);
    }

    PROCESSING_MODE = context.getValue(OperatorContext.PROCESSING_MODE);
    if (PROCESSING_MODE == ProcessingMode.EXACTLY_ONCE && CHECKPOINT_WINDOW_COUNT != 1) {
      logger.warn("Ignoring {} attribute in favor of {} processing mode", OperatorContext.CHECKPOINT_WINDOW_COUNT.getSimpleName(), ProcessingMode.EXACTLY_ONCE.name());
      CHECKPOINT_WINDOW_COUNT = 1;
    }

    activateSinks();
    if (operator instanceof Operator.ActivationListener) {
      ((Operator.ActivationListener<OperatorContext>)operator).activate(context);
    }

    if (statsListeners != null) {
      Iterator<StatsListener> iterator = statsListeners.iterator();
      while (iterator.hasNext()) {
        DATA_TUPLE_AWARE = iterator.next().getClass().isAnnotationPresent(StatsListener.DataQueueSize.class);
        if (DATA_TUPLE_AWARE) {
          break;
        }
      }
    }

    if (!DATA_TUPLE_AWARE && (operator instanceof StatsListener)) {
      DATA_TUPLE_AWARE = operator.getClass().isAnnotationPresent(StatsListener.DataQueueSize.class);
    }
    /*
     * If there were any requests which needed to be executed before the operator started
     * its normal execution, execute those requests now - e.g. Restarting the operator
     * recording for the operators which failed while recording and being replaced.
     */
    handleRequests(currentWindowId);
  }

  public void deactivate()
  {
    if (operator instanceof Operator.ActivationListener) {
      ((Operator.ActivationListener<?>)operator).deactivate();
    }

    if (!shutdown && !alive) {
      emitEndStream();
    }

    deactivateSinks();
  }

  private class CheckpointHandler implements Callable<Stats.CheckpointStats>
  {
    public AsyncStorageAgent agent;
    public int operatorId;
    public long windowId;
    public Stats.CheckpointStats stats;

    @Override
    public Stats.CheckpointStats call() throws Exception
    {
      agent.flush(id, windowId);
      stats.checkpointTime = System.currentTimeMillis() - stats.checkpointStartTime;
      return stats;
    }
  }

  private class CheckpointWindowInfo
  {
    public int applicationWindowCount;
    public int checkpointWindowCount;
    public long windowId;
  }

  private static final Logger logger = LoggerFactory.getLogger(Node.class);
}
