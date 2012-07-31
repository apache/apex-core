/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.malhartech.dag;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface Context {
  public enum State {
    UNDEFINED,
    OUTSIDE_WINDOW,
    INSIDE_WINDOW,
    TERMINATED
  }
}
