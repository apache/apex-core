package com.malhartech.dag;

//import java.util.Collection;

/*
 * Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface InternalNode extends Node
{
  /**
   *
   * @param id the value of id
   * @param input the value of input
   */
  public void connectOutput(StreamContext output);

//  public void connect(int id, Stream stream);
//  public void connectPorts(Collection<Stream> streams);

  public NodeContext getContext();
}
