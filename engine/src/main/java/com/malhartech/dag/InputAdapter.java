/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface InputAdapter extends Stream
{
  /**
   * corresponds to 2^14 - 1 =>  maximum bytes needed for varint encoding is 2.
   */
  public static final int MAX_VALUE_WINDOW = 0x3fff - (0x3fff % 1000) - 1;
  
  public boolean hasFinished();

  public void resetWindow(int baseSeconds, int intervalMillis);

  public void beginWindow(int windowId);

  public void endWindow(int windowId);
}
