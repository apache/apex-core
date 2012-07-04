/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class StreamContext implements Context
{

  SerDe getSerDe()
  {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  Sink getSink()
  {
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
