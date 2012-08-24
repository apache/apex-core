/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

/**
 *
 * @author chetan
 */
public interface Node extends DAGPart<NodeConfiguration, NodeContext>
{
  /**
   * This method gets called at the beginning of each window.
   *
   */
  public void beginWindow();

  /**
   * This method gets called for each tuple that is received by this node.
   *
   * @param payload
   */
  public void process(Object payload);

  /**
   * This method gets called at the end of each window.
   *
   */
  public void endWindow();
}
