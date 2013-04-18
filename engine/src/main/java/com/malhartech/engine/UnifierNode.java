/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.tuple.Tuple;
import com.malhartech.api.Context.PortContext;
import com.malhartech.api.Operator.Unifier;
import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.api.StreamCodec.DataStatePair;
import com.malhartech.netlet.Client.Fragment;
import com.malhartech.stream.BufferServerSubscriber;
import com.malhartech.tuple.EndStreamTuple;
import com.malhartech.tuple.EndWindowTuple;
import com.malhartech.tuple.ResetWindowTuple;
import com.malhartech.util.AttributeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class UnifierNode extends GenericNode
{
  final Unifier<Object> unifier;

  private class MergeReservoir extends AbstractReservoir
  {
    MergeReservoir(String portname, int bufferSize, int spinMillis)
    {
      super(portname, bufferSize, spinMillis);
    }

    @Override
    public Tuple sweep()
    {
      int size = size();
      for (int i = 1; i <= size; i++) {
        if (peekUnsafe() instanceof Tuple) {
          count += i;
          return (Tuple)peekUnsafe();
        }

        unifier.merge(pollUnsafe());
      }

      count += size;
      return null;
    }

    @Override
    public void consume(Object payload)
    {
      unifier.merge(payload);
    }

  }

  public UnifierNode(String id, Unifier<Object> unifier)
  {
    super(id, unifier);
    this.unifier = unifier;
  }

  @Override
  public Sink<Object> connectInputPort(String port, AttributeMap<PortContext> attributes, Sink<? extends Object> sink)
  {
    MergeReservoir retvalue;

    if (sink == null) {
      Reservoir reservoir = inputs.remove(port);
      if (reservoir != null) {
        inputs.put(port.concat(".").concat(String.valueOf(deletionId++)), reservoir);
        reservoir.process(new EndStreamTuple());
      }

      retvalue = null;
    }
    else {
      int bufferCapacity = attributes == null ? 16 * 1024 : attributes.attrValue(PortContext.BUFFER_SIZE, 16 * 1024);
      int spinMilliseconds = attributes == null ? 15 : attributes.attrValue(PortContext.SPIN_MILLIS, 15);
      if (sink instanceof BufferServerSubscriber) {
        final BufferServerSubscriber bss = (BufferServerSubscriber)sink;
        retvalue = new MergeReservoir(port, bufferCapacity, spinMilliseconds)
        {
          private DataStatePair dsp = new DataStatePair();
          private StreamCodec<Object> serde = bss.getSerde();
          private long baseSeconds = bss.getBaseSeconds();
          private int lastWindowId = WindowGenerator.MAX_WINDOW_ID;

          @Override
          public void process(Object payload)
          {
            add(payload);
          }

          @Override
          public Tuple sweep()
          {
            final int size = size();
            for (int i = 1; i <= size; i++) {
              Fragment fm = (Fragment)peekUnsafe();
              com.malhartech.bufferserver.packet.Tuple data = com.malhartech.bufferserver.packet.Tuple.getTuple(fm.buffer, fm.offset, fm.length);
              Tuple t;
              switch (data.getType()) {
                case CHECKPOINT:
                  pollUnsafe();
                  serde.resetState();
                  break;

                case CODEC_STATE:
                  pollUnsafe();
                  Fragment f = data.getData();
                  dsp.state = f;
                  break;

                case PAYLOAD:
                  pollUnsafe();
                  dsp.data = data.getData();
                  Object o = serde.fromByteArray(dsp);
                  unifier.merge(o);
                  break;

                case END_WINDOW:
                  //logger.debug("received {}", data);
                  t = new EndWindowTuple();
                  t.setWindowId(baseSeconds | (lastWindowId = data.getWindowId()));
                  count += i;
                  return t;

                case END_STREAM:
                  t = new EndStreamTuple();
                  t.setWindowId(baseSeconds | data.getWindowId());
                  count += i;
                  return t;

                case RESET_WINDOW:
                  baseSeconds = (long)data.getBaseSeconds() << 32;
                  if (lastWindowId < WindowGenerator.MAX_WINDOW_ID) {
                    break;
                  }
                  t = new ResetWindowTuple();
                  t.setWindowId(baseSeconds | data.getWindowWidth());
                  count += i;
                  return t;

                case BEGIN_WINDOW:
                  //logger.debug("received {}", data);
                  t = new Tuple(data.getType());
                  t.setWindowId(baseSeconds | data.getWindowId());
                  count += i;
                  return t;

                case NO_MESSAGE:
                  pollUnsafe();
                  break;

                default:
                  throw new IllegalArgumentException("Unhandled Message Type " + data.getType());
              }
            }

            count += size;
            return null;
          }

        };
      }
      else {
        retvalue = new MergeReservoir(port, bufferCapacity, spinMilliseconds);
      }
      inputs.put(port, retvalue);
    }

    return retvalue;
  }

  private static final Logger logger = LoggerFactory.getLogger(UnifierNode.class);
}
