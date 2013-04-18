/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.api.StreamCodec.DataStatePair;
import com.malhartech.netlet.Client.Fragment;
import com.malhartech.tuple.EndStreamTuple;
import com.malhartech.tuple.EndWindowTuple;
import com.malhartech.tuple.ResetWindowTuple;
import com.malhartech.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
class BufferServerReservoir extends AbstractReservoir
{
  private final Sink<Object> sink;
  private final DataStatePair dsp;
  private int lastWindowId;
  private long baseSeconds;
  private StreamCodec<Object> serde;

  BufferServerReservoir(Sink<Object> sink, String port, int bufferCapacity, int spinMillis, StreamCodec<Object> serde, long baseSeconds)
  {
    super(port, bufferCapacity, spinMillis);
    this.sink = sink;
    dsp = new DataStatePair();
    lastWindowId = WindowGenerator.MAX_WINDOW_ID;
    this.baseSeconds = baseSeconds;
    this.serde = serde;
  }

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
          sink.process(o);
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

  @Override
  public void consume(Object payload)
  {
    sink.process(payload);
  }

  private static final Logger logger = LoggerFactory.getLogger(BufferServerReservoir.class);
}
