/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.Sink;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface DAGComponent<T2 extends Context>
{
  void activate(T2 context);

  void deactivate();

  public Sink connect(String port, Sink sink);
}
