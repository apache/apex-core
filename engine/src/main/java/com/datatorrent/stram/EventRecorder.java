/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.stram.api.StramEvent;

/**
 * <p>EventRecorder interface.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.4
 */
public interface EventRecorder
{
  public void recordEventAsync(StramEvent event);

}
