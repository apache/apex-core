/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.api;

import com.datatorrent.stram.engine.Node;

/**
 * When the node active state changes, listeners are notified of the changes.
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public interface NodeActivationListener
{
  /**
   * Callback to notify the listner that the node has been activated.
   *
   * @param node node which got activated.
   */
  public void activated(Node<?> node);

  /**
   * Callback to notify the listner that the node has been activated.
   *
   * @param node node which got deactivated.
   */
  public void deactivated(Node<?> node);

}
