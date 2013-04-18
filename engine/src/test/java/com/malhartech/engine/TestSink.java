/**
 * Copyright (c) 2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.engine;

import com.malhartech.tuple.Tuple;
import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.api.StreamCodec.DataStatePair;
import com.malhartech.netlet.Client.Fragment;
import java.util.ArrayList;
import java.util.List;

/**
 * A sink implementation to collect expected test results.
 */
public class TestSink implements Sink<Object>
{
  final public List<Object> collectedTuples = new ArrayList<Object>();
  DataStatePair dsp = new DataStatePair();
  final StreamCodec<Object> serde = new DefaultStreamCodec<Object>();

  public void clear()
  {
    this.collectedTuples.clear();
  }

  @Override
  @SuppressWarnings("fallthrough")
  public void process(Object payload)
  {
    if (payload instanceof Fragment) {
      Fragment f = (Fragment)payload;
      com.malhartech.bufferserver.packet.Tuple t = com.malhartech.bufferserver.packet.Tuple.getTuple(f.buffer, f.offset, f.length);
      switch (t.getType()) {
        case PAYLOAD:
          dsp.data = t.getData();
          payload = serde.fromByteArray(dsp);
          break;

        case CODEC_STATE:
          dsp.state = t.getData();
        default:
          return;
      }
    }
    else if (payload instanceof Tuple) {
      return;
    }

    synchronized (collectedTuples) {
      collectedTuples.add(payload);
      collectedTuples.notifyAll();
    }
  }

  public void waitForResultCount(int count, long timeoutMillis) throws InterruptedException
  {
    while (collectedTuples.size() < count && timeoutMillis > 0) {
      timeoutMillis -= 20;
      synchronized (collectedTuples) {
        if (collectedTuples.size() < count) {
          collectedTuples.wait(20);
        }
      }
    }
  }

}
