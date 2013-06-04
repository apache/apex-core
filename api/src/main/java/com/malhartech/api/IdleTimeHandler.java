/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

/**
 * Interface operator must implement if it's interested in being notified when it's idling.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface IdleTimeHandler
{
  public void handleIdleTime();

}
