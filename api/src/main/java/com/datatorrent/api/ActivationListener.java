/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.api;

/**
 * Interface operator must implement if they want the the engine to inform them as
 * they are activated or before they are deactivated.
 *
 * @param <CONTEXT> Context for the current run during which the operator is getting de/activated.
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface ActivationListener<CONTEXT extends Context>
{
  public void activate(CONTEXT ctx);

  public void deactivate();

}