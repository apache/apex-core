/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface InputOperator extends Operator
{
  public void injectTuples(long windowId);
}
