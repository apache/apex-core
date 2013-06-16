/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.ActivationListener;
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
public interface Stream extends Component<StreamContext>, ActivationListener<StreamContext>, Sink<Object>
{
  public boolean isMultiSinkCapable();

  public void setSink(String id, Sink<Object> sink);
}
