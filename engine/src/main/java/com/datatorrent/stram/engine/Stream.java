/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

import com.datatorrent.api.Component;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.api.Sink;

/**
 *
 * Base interface for all streams in the streaming platform<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
/*
 * Provides basic interface for a stream object. Stram, StramChild work via this interface
 */
public interface Stream extends Component<StreamContext>, ActivationListener<StreamContext>, Sink<Object>
{
  public interface MultiSinkCapableStream extends Stream
  {
    public void setSink(String id, Sink<Object> sink);
  }

}
