/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

import com.datatorrent.api.IdleTimeHandler;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.Sink;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerStats;
import com.datatorrent.stram.tuple.Tuple;

/**
 * <p>InputNode class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class InputNode extends Node<InputOperator>
{
  private final ArrayList<SweepableReservoir> deferredInputConnections = new ArrayList<SweepableReservoir>();
  protected SweepableReservoir controlTuples;

  public InputNode(InputOperator operator)
  {
    super(operator);
  }

  @Override
  public void connectInputPort(String port, SweepableReservoir reservoir)
  {
    if (Node.INPUT.equals(port)) {
      if (controlTuples == null) {
        controlTuples = reservoir;
      }
      else {
        deferredInputConnections.add(reservoir);
      }
    }
  }

  @Override
  @SuppressWarnings(value = "SleepWhileInLoop")
  public final void run()
  {
    long spinMillis = context.getValue(OperatorContext.SPIN_MILLIS);
    final boolean handleIdleTime = operator instanceof IdleTimeHandler;

    boolean insideWindow = false;
    boolean checkpoint = false;

    try {
      while (alive) {
        Tuple t = controlTuples.sweep();
        if (t == null) {
          if (insideWindow) {
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
              }
              else {
                Thread.sleep(spinMillis);
              }
            }
          }
          else {
            Thread.sleep(0);
          }
        }
        else {
          controlTuples.remove();
          switch (t.getType()) {
            case BEGIN_WINDOW:
              for (int i = sinks.length; i-- > 0;) {
                sinks[i].put(t);
              }
              controlTupleCount++;
              currentWindowId = t.getWindowId();
              if (applicationWindowCount == 0) {
                insideWindow = true;
                operator.beginWindow(currentWindowId);
              }
              operator.emitTuples(); /* give at least one chance to emit the tuples */
              break;

            case END_WINDOW:
              endWindowEmitTime = System.currentTimeMillis();
              if (++applicationWindowCount == APPLICATION_WINDOW_COUNT) {
                insideWindow = false;
                operator.endWindow();
                applicationWindowCount = 0;
              }

              for (int i = sinks.length; i-- > 0;) {
                sinks[i].put(t);
              }
              controlTupleCount++;

              if (++checkpointWindowCount == CHECKPOINT_WINDOW_COUNT) {
                if (checkpoint && checkpoint(currentWindowId)) {
                  checkpoint = false;
                }
                else if (PROCESSING_MODE == ProcessingMode.EXACTLY_ONCE) {
                  checkpoint(currentWindowId);
                }
                checkpointWindowCount = 0;
              }

              ContainerStats.OperatorStats stats = new ContainerStats.OperatorStats();
              reportStats(stats, currentWindowId);
              handleRequests(currentWindowId);
              break;

            case CHECKPOINT:
              if (checkpointWindowCount == 0 && PROCESSING_MODE != ProcessingMode.EXACTLY_ONCE) {
                checkpoint(currentWindowId);
              }
              else {
                checkpoint = true;
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
              }
              else {
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
    }
    catch (InterruptedException ex) {
      alive = false;
    }
    catch (RuntimeException ex) {
      if (ex.getCause() instanceof InterruptedException) {
        alive = false;
      }
      else {
        throw ex;
      }
    }

    if (insideWindow) {
      endWindowEmitTime = System.currentTimeMillis();
      operator.endWindow();
      if (++applicationWindowCount == APPLICATION_WINDOW_COUNT) {
        applicationWindowCount = 0;
      }
      if (++checkpointWindowCount == CHECKPOINT_WINDOW_COUNT) {
        if (checkpoint || PROCESSING_MODE == ProcessingMode.EXACTLY_ONCE) {
          checkpoint(currentWindowId);
        }
        checkpointWindowCount = 0;
      }

      ContainerStats.OperatorStats stats = new ContainerStats.OperatorStats();
      reportStats(stats, currentWindowId);
      handleRequests(currentWindowId);
    }
  }

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(InputNode.class);
}
