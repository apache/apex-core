package com.malhartech.dag;

/*
 * Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */

/**
 *
 * Base interface for a node<p>
 * <br>
 * 
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface InternalNode extends Node
{
  public Sink getSink(StreamContext input);

  public void addOutputStream(StreamContext output);

  public void start(NodeContext nodeContext);

  public void stop();

  public NodeContext getContext();
}
