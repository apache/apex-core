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
public interface Component<T1 extends Configuration, T2 extends Context>
{
  public void setup(T1 config) throws FailedOperationException;

  void activated(T2 context);

  void deactivated();

  public void teardown();
}
