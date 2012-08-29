/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

/**
 *
 * InputAdapter for streams that are inbound from outside (to be changed)<p>
 * <br>
 * Needs to be changed to a node :)
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface InputAdapter extends Stream
{

  public boolean hasFinished();

  public void resetWindow(int baseSeconds, int intervalMillis);

  public void beginWindow(int windowId);

  public void endWindow(int windowId);
}
