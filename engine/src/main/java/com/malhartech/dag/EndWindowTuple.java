/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class EndWindowTuple extends Tuple
{
  private int tupleCount;

  public EndWindowTuple()
  {
    super(Buffer.Data.DataType.END_WINDOW);
  }

  /**
   * @return the tupleCount
   */
  public int getTupleCount()
  {
    return tupleCount;
  }

  /**
   * @param tupleCount the tupleCount to set
   */
  public void setTupleCount(int tupleCount)
  {
    this.tupleCount = tupleCount;
  }

  @Override
  public String toString()
  {
    return "tuples = " + tupleCount + " " + super.toString();
  }
}
