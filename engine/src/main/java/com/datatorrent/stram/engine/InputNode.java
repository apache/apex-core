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

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.IdleTimeHandler;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.Operator.ShutdownException;
import com.datatorrent.api.Sink;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerStats;
import com.datatorrent.stram.tuple.Tuple;

/**
 * <p>
 * InputNode class.</p>
 *
 * @since 0.3.2
 */
public class InputNode extends Node<InputOperator>
{
  private final ArrayList<SweepableReservoir> deferredInputConnections = new ArrayList<>();
  protected SweepableReservoir controlTuples;
  long lastCheckpointWindowId = Stateless.WINDOW_ID;

  public InputNode(InputOperator operator, OperatorContext context)
  {
    super(operator, context);
  }

  @Override
  public void connectInputPort(String port, SweepableReservoir reservoir)
  {
    if (Node.INPUT.equals(port)) {
      if (controlTuples == null) {
        controlTuples = reservoir;
      } else {
        deferredInputConnections.add(reservoir);
      }
    }
  }

  @Override
  @SuppressWarnings(value = {"SleepWhileInLoop", "BroadCatchBlock", "TooBroadCatch"})
  public final void run()
  {
    long maxSpinMillis = context.getValue(OperatorContext.SPIN_MILLIS);
    long spinMillis = 0;
    final boolean handleIdleTime = operator instanceof IdleTimeHandler;

    boolean insideApplicationWindow = applicationWindowCount != 0;
    boolean doCheckpoint = false;
    boolean insideStreamingWindow = false;

    calculateNextCheckpointWindow();

    try {
      while (alive) {
        Tuple t = controlTuples.sweep();
        if (t == null) {
          if (insideStreamingWindow) {
            int generatedTuples = 0;

            for (Sink<Object> cs : sinks) {
              generatedTuples -= cs.getCount(false);
            }

            operator.emitTuples();

            for (Sink<Object> cs : sinks) {
              generatedTuples += cs.getCount(false);
            }

            if (generatedTuples == 0) {
              if (handleIdleTime) {
                ((IdleTimeHandler)operator).handleIdleTime();
              } else {
                Thread.sleep(spinMillis);
                spinMillis = Math.min(spinMillis + 1, maxSpinMillis);
              }
            } else {
              spinMillis = 0;
            }
          } else {
            Thread.sleep(0);
          }
        } else {
          controlTuples.remove();
          switch (t.getType()) {
            case BEGIN_WINDOW:
              for (int i = sinks.length; i-- > 0; ) {
                sinks[i].put(t);
              }
              controlTupleCount++;
              currentWindowId = t.getWindowId();
              insideStreamingWindow = true;
              if (applicationWindowCount == 0) {
                insideApplicationWindow = true;
                operator.beginWindow(currentWindowId);
              }
              operator.emitTuples(); /* give at least one chance to emit the tuples */

              break;

            case END_WINDOW:
              insideStreamingWindow = false;
              if (++applicationWindowCount == APPLICATION_WINDOW_COUNT) {
                insideApplicationWindow = false;
                operator.endWindow();
                applicationWindowCount = 0;
              }
              endWindowEmitTime = System.currentTimeMillis();

              for (int i = sinks.length; i-- > 0;) {
                sinks[i].put(t);
              }
              controlTupleCount++;

              if (doCheckpoint) {
                dagCheckpointOffsetCount = (dagCheckpointOffsetCount + 1) % DAG_CHECKPOINT_WINDOW_COUNT;
              }

              if (++checkpointWindowCount == CHECKPOINT_WINDOW_COUNT) {
                checkpointWindowCount = 0;
                if (doCheckpoint) {
                  checkpoint(currentWindowId);
                  lastCheckpointWindowId = currentWindowId;
                  doCheckpoint = false;
                } else if (PROCESSING_MODE == ProcessingMode.EXACTLY_ONCE) {
                  checkpoint(currentWindowId);
                  lastCheckpointWindowId = currentWindowId;
                }
              }

              ContainerStats.OperatorStats stats = new ContainerStats.OperatorStats();
              reportStats(stats, currentWindowId);
              if (!insideApplicationWindow) {
                stats.metrics = collectMetrics();
              }
              handleRequests(currentWindowId);
              break;

            case CHECKPOINT:
              dagCheckpointOffsetCount = 0;
              if (lastCheckpointWindowId < currentWindowId) {
                if (checkpointWindowCount == 0 && PROCESSING_MODE != ProcessingMode.EXACTLY_ONCE) {
                  checkpoint(currentWindowId);
                  lastCheckpointWindowId = currentWindowId;
                } else {
                  doCheckpoint = true;
                }
              }
              for (int i = sinks.length; i-- > 0;) {
                sinks[i].put(t);
              }
              controlTupleCount++;
              break;

            case END_STREAM:
              if (deferredInputConnections.isEmpty()) {
                for (int i = sinks.length; i-- > 0;) {
                  sinks[i].put(t);
                }
                controlTupleCount++;
                alive = false;
              } else {
                controlTuples = deferredInputConnections.remove(0);
              }
              break;

            default:
              for (int i = sinks.length; i-- > 0;) {
                sinks[i].put(t);
              }
              controlTupleCount++;
              break;
          }
        }
      }
    } catch (ShutdownException se) {
      logger.debug("Shutdown requested by the operator when alive = {}.", alive);
      alive = false;
    } catch (Throwable cause) {
      synchronized (this) {
        if (alive) {
          throw Throwables.propagate(cause);
        }
      }

      Throwable rootCause = cause;
      while (rootCause != null) {
        if (rootCause instanceof InterruptedException) {
          break;
        }
        rootCause = rootCause.getCause();
      }

      if (rootCause == null) {
        throw Throwables.propagate(cause);
      } else {
        logger.debug("Ignoring InterruptedException after shutdown", cause);
      }
    }

    if (insideApplicationWindow) {
      operator.endWindow();
      endWindowEmitTime = System.currentTimeMillis();
      if (++applicationWindowCount == APPLICATION_WINDOW_COUNT) {
        applicationWindowCount = 0;
      }

      if (lastCheckpointWindowId < currentWindowId) {
        //This check is here because if the node is shutdown after a checkpoint is completed for a window,
        //but before the next window begins a double checkpoint could happen if the CheckpointWindowCount
        //is 1
        if (++checkpointWindowCount == CHECKPOINT_WINDOW_COUNT) {
          checkpointWindowCount = 0;
          if (doCheckpoint || PROCESSING_MODE == ProcessingMode.EXACTLY_ONCE) {
            checkpoint(currentWindowId);
            lastCheckpointWindowId = currentWindowId;
          }
        }
      }

      ContainerStats.OperatorStats stats = new ContainerStats.OperatorStats();
      reportStats(stats, currentWindowId);
      stats.metrics = collectMetrics();
      handleRequests(currentWindowId);
    }
  }

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(InputNode.class);
}
