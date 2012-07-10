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
  public void beginWindow(long timemillis);
  public void endWindow(long timemillis);
}
