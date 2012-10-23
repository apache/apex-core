/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.Component;
import com.malhartech.api.Sink;

/**
 *
 * Base interface for all streams in the streaming platform<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
/*
 * Provides basic interface for a stream object. Stram, StramChild work via this interface
 */
public interface Stream<T> extends Component<StreamConfiguration, StreamContext>, Sink<T>
{
  public boolean isMultiSinkCapable();

  public Sink setSink(String sinkId, Sink<T> sink);

  public long getProcessedCount();
}
