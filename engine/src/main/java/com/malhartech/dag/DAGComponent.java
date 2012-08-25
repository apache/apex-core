/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface DAGComponent<T1 extends Configuration, T2 extends Context> extends Sink
{
  public void setup(T1 config);

  public void activate(T2 context);

  public void deactivate();

  public void teardown();

  public Sink connect(String id, DAGComponent component);
}
