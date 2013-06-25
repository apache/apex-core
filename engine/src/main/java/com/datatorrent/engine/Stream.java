/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.engine;

import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.Component;
import com.datatorrent.api.Sink;

/**
 *
 * Base interface for all streams in the streaming platform<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
/*
 * Provides basic interface for a stream object. Stram, StramChild work via this interface
 */
public interface Stream extends Component<StreamContext>, ActivationListener<StreamContext>, Sink<Object>
{
  public boolean isMultiSinkCapable();

  public void setSink(String id, Sink<Object> sink);
}
