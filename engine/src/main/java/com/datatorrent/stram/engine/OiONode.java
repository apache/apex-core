/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

import java.util.Iterator;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.UnhandledException;

import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Sink;
import com.datatorrent.stram.plan.logical.Operators.PortContextPair;
import com.datatorrent.stram.stream.OiOStream;
import com.datatorrent.stram.tuple.Tuple;

/**
 * OiONode is driver for the OiO (ThreadLocal) operator.
 *
 * It mostly replicates the functionality of the GenericNode but the logic here is a lot simpler as most of the
 * validation is already handled by the upstream operator.
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.5
 */
public class OiONode extends GenericNode
{
  private long lastResetWindowId = WindowGenerator.MIN_WINDOW_ID - 1;
  private long lastEndStreamWindowId = WindowGenerator.MAX_WINDOW_ID - 1;
  private int expectingEndWindows = 0;

  public OiONode(Operator operator)
  {
    super(operator);
  }

  class ControlSink implements Sink<Tuple>
  {
    final SweepableReservoir reservoir;

    ControlSink(SweepableReservoir sr)
    {
      reservoir = sr;
    }

    @Override
    public void put(Tuple t)
    {
      switch (t.getType()) {
        case BEGIN_WINDOW:
          expectingEndWindows++;
          if (t.getWindowId() != currentWindowId) {
            currentWindowId = t.getWindowId();
            for (int s = sinks.length; s-- > 0;) {
              sinks[s].put(t);
            }
            controlTupleCount++;

            if (applicationWindowCount == 0) {
              insideWindow = true;
              operator.beginWindow(currentWindowId);
            }
          }
          break;

        case END_WINDOW:
          if (--expectingEndWindows == 0) {
            endWindowDequeueTimes.put(reservoir, System.currentTimeMillis());
            processEndWindow(t);
          }
          break;

        case CHECKPOINT:
          if (lastCheckpointedWindowId < currentWindowId && !doCheckpoint) {
            if (checkpointWindowCount == 0) {
              checkpoint(currentWindowId);
              lastCheckpointedWindowId = currentWindowId;
            }
            else {
              doCheckpoint = true;
            }
            for (int s = sinks.length; s-- > 0;) {
              sinks[s].put(t);
            }
            controlTupleCount++;
          }
          break;

        case RESET_WINDOW:
          if (t.getWindowId() != lastResetWindowId) {
            lastResetWindowId = t.getWindowId();
            for (int s = sinks.length; s-- > 0;) {
              sinks[s].put(t);
            }
            controlTupleCount++;
          }
          break;

        case END_STREAM:
          if (lastEndStreamWindowId != t.getWindowId()) {
            lastEndStreamWindowId = t.getWindowId();
            for (Entry<String, SweepableReservoir> e : inputs.entrySet()) {
              PortContextPair<InputPort<?>> pcpair = descriptor.inputPorts.get(e.getKey());
              if (pcpair != null) {
                pcpair.component.setConnected(false);
              }
            }
            inputs.clear();

            Iterator<DeferredInputConnection> dici = deferredInputConnections.iterator();
            while (dici.hasNext()) {
              DeferredInputConnection dic = dici.next();
              if (!inputs.containsKey(dic.portname)) {
                dici.remove();
                connectInputPort(dic.portname, dic.reservoir);
              }
            }

            if (inputs.isEmpty()) {
              if (insideWindow) {
                applicationWindowCount = APPLICATION_WINDOW_COUNT - 1;
                expectingEndWindows = 0;
                endWindowDequeueTimes.put(reservoir, System.currentTimeMillis());
                processEndWindow(null);
              }
              emitEndStream();
            }
          }
          break;

        default:
          throw new UnhandledException("Unrecognized Control Tuple", new IllegalArgumentException(t.toString()));
      }
    }

    @Override
    public int getCount(boolean reset)
    {
      throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
    }

  }

  @Override
  public void connectInputPort(String port, SweepableReservoir reservoir)
  {
    ((OiOStream)reservoir).setControlSink(new ControlSink(reservoir));
    super.connectInputPort(port, reservoir);
  }

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(OiONode.class);
}
