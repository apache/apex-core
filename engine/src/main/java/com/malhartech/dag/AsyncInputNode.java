/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.AsyncInputOperator;
import com.malhartech.api.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// write recoverable AIN
/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class AsyncInputNode extends InputNode<AsyncInputOperator>
{
  private static final Logger logger = LoggerFactory.getLogger(AsyncInputNode.class);

  public AsyncInputNode(String id, AsyncInputOperator operator)
  {
    super(id, operator);
  }

  @Override
  protected final void injectTuples() throws InterruptedException
  {
    operator.injectTuples(currentWindowId);
  }
}
