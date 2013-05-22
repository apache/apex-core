/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.BackupAgent;
import com.malhartech.api.CheckpointListener;
import com.malhartech.api.InputOperator;
import com.malhartech.api.Sink;
import com.malhartech.tuple.Tuple;
import java.io.IOException;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class InputNode extends Node<InputOperator>
{
  private ArrayList<SweepableReservoir> deferredInputConnections = new ArrayList<SweepableReservoir>();
  protected SweepableReservoir controlTuples;

  public InputNode(int id, InputOperator operator)
  {
    super(id, operator);
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
    long spinMillis = context.getAttributes().attrValue(OperatorContext.SPIN_MILLIS, 10);
    boolean insideWindow = false;
    windowCount = 0;

    try {
      while (alive) {
        Tuple t = controlTuples.sweep();
        if (t == null) {
          if (insideWindow) {
            int generatedTuples = 0;

            for (Sink<Object> cs: sinks) {
              generatedTuples -= cs.getCount(false);
            }

            operator.emitTuples();

            for (Sink<Object> cs: sinks) {
              generatedTuples += cs.getCount(false);
            }

            if (generatedTuples == 0) {
              Thread.sleep(spinMillis);
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
              currentWindowId = t.getWindowId();
              if (windowCount == 0) {
                insideWindow = true;
                operator.beginWindow(currentWindowId);
              }
              operator.emitTuples(); /* give at least one chance to emit the tuples */
              break;

            case END_WINDOW:
              if (++windowCount == applicationWindowCount) {
                operator.endWindow();
                insideWindow = false;
                windowCount = 0;
              }
              for (int i = sinks.length; i-- > 0;) {
                sinks[i].put(t);
              }
              handleRequests(currentWindowId);
              break;

            case CHECKPOINT:
              BackupAgent ba = context.getAttributes().attr(OperatorContext.BACKUP_AGENT).get();
              if (ba != null) {
                try {
                  ba.backup(id, t.getWindowId(), operator);
                  checkpointedWindowId = t.getWindowId();
                  if (operator instanceof CheckpointListener) {
                    ((CheckpointListener)operator).checkpointed(checkpointedWindowId);
                  }
                }
                catch (IOException io) {
                  throw new RuntimeException(io);
                }
              }
              for (int i = sinks.length; i-- > 0;) {
                sinks[i].put(t);
              }
              break;

            case END_STREAM:
              if (deferredInputConnections.isEmpty()) {
                for (int i = sinks.length; i-- > 0;) {
                  sinks[i].put(t);
                }
              }
              else {
                controlTuples = deferredInputConnections.remove(0);
              }
              break;

            default:
              for (int i = sinks.length; i-- > 0;) {
                sinks[i].put(t);
              }
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
      operator.endWindow();
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(InputNode.class);
}
