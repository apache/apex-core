/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.tuple.Tuple;
import com.malhartech.tuple.ResetWindowTuple;
import com.malhartech.tuple.EndWindowTuple;
import com.malhartech.tuple.EndStreamTuple;
import com.malhartech.tuple.PayloadTuple;
import com.malhartech.api.StreamCodec;
import com.malhartech.api.StreamCodec.DataStatePair;
import com.malhartech.engine.*;
import com.malhartech.netlet.Client.Fragment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Fragment2Tuple
{
  private DataStatePair dsp = new DataStatePair();
  private int lastWindowId = WindowGenerator.MAX_WINDOW_ID;
  private final StreamCodec<Object> serde;
  private long baseSeconds;

  public Fragment2Tuple(StreamCodec<Object> serde, long baseSeconds)
  {
    this.serde = serde;
    this.baseSeconds = baseSeconds;
  }

  public Tuple convert(Fragment fm)
  {
    com.malhartech.bufferserver.packet.Tuple data = com.malhartech.bufferserver.packet.Tuple.getTuple(fm.buffer, fm.offset, fm.length);
    Tuple t;
    switch (data.getType()) {
      case CHECKPOINT:
        serde.resetState();
        t = null;
        break;

      case CODEC_STATE:
        Fragment f = data.getData();
        dsp.state = f;
        t = null;
        break;

      case PAYLOAD:
        dsp.data = data.getData();
        t = new PayloadTuple(serde.fromByteArray(dsp));
        t.setWindowId(baseSeconds | lastWindowId);
        break;

      case END_WINDOW:
        //logger.debug("received {}", data);
        t = new EndWindowTuple();
        t.setWindowId(baseSeconds | (lastWindowId = data.getWindowId()));
        break;

      case END_STREAM:
        t = new EndStreamTuple();
        t.setWindowId(baseSeconds | data.getWindowId());
        break;

      case RESET_WINDOW:
        baseSeconds = (long)data.getBaseSeconds() << 32;
        if (lastWindowId < WindowGenerator.MAX_WINDOW_ID) {
          t = null;
        }
        else {
          t = new ResetWindowTuple();
          t.setWindowId(baseSeconds | data.getWindowWidth());
        }
        break;

      case BEGIN_WINDOW:
        //logger.debug("received {}", data);
        t = new Tuple(data.getType());
        t.setWindowId(baseSeconds | data.getWindowId());
        break;

      case NO_MESSAGE:
        t = null;
        break;

      default:
        throw new IllegalArgumentException("Unhandled Message Type " + data.getType());
    }

    return t;
  }

  private static final Logger logger = LoggerFactory.getLogger(Fragment2Tuple.class);
}
