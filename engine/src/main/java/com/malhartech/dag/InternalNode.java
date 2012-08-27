package com.malhartech.dag;

//import java.util.Collection;

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
  public NodeContext getContext();
}
