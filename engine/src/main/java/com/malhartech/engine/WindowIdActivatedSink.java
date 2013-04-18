/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.tuple.Tuple;
import com.malhartech.api.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @param <T>
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class WindowIdActivatedSink<T> implements Sink<T>
{
  private final Sink<Object> sink;
  private final long windowId;
  private final Stream<Object> stream;
  private final String identifier;

  public WindowIdActivatedSink(Stream<Object> stream, String identifier, final Sink<Object> sink, final long windowId)
  {
    this.stream = stream;
    this.identifier = identifier;
    this.sink = sink;
    this.windowId = windowId;
  }

  @Override
  public void process(Object payload)
  {
    if (payload instanceof Tuple) {
      switch (((Tuple)payload).getType()) {
        case BEGIN_WINDOW:
          logger.debug("got begin widnow {} to compare against {}", payload, windowId);
          if (((Tuple)payload).getWindowId() > windowId) {
            sink.process(payload);
            stream.setSink(identifier, sink);
          }
          break;

        case CODEC_STATE:
          sink.process(payload);
          break;
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(WindowIdActivatedSink.class);
}
