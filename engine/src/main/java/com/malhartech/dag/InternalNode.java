package com.malhartech.dag;

/*
 * Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface InternalNode extends Node
{
  public Sink getSink(StreamContext input);

  public void addOutputStream(StreamContext output);

  public NodeContext getContext();
}
