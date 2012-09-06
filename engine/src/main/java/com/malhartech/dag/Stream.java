/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

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
public interface Stream extends Component<StreamConfiguration, StreamContext>
{
  public boolean isMultiSinkCapable();
}
