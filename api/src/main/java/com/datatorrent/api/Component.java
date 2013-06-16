/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

/**
 * Basic interface which is implemented by almost all entities in the system.
 * This interface provides a convenient way of setting up and tearing down most
 * entities in the system.
 *
 * @param <T1> Context used for the current run of the component.
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface Component<T1 extends Context>
{
  public void setup(T1 context);

  public void teardown();

}
