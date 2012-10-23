/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface ActivationListener<CONTEXT extends Context>
{
  public void postActivate(CONTEXT ctx);

  public void preDeactivate();
}
