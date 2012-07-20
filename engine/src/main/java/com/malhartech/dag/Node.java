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
   * This method gets called as soon as the node is allocated resources.
   * 
   * @param config - configuration object created from the topology.
   */
  public void setup(NodeConfiguration config);

  /**
   * This method gets called at the beginning of each window.
   *
   * @param context - currently unused, may go away.
   */
  public void beginWindow();

  /**
   * This method gets called at the end of each window.
   *
   * @param context - currently unused, may go away.
   */
  public void endWindow();

  /**
   * This method gets called for each tuple that is received by this node.
   *
   * @param payload
   */
  public void process(Object payload);

  /**
   * This method gets called just before the node is terminated.
   */
  public void teardown();
}
