/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.deprecated.api;

import com.malhartech.api.Operator;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
@Deprecated
public interface SyncInputOperator extends Operator
{
  public Runnable getDataPoller();
}
