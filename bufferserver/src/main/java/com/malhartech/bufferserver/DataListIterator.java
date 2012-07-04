/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import java.util.Iterator;

/**
 *
 * @author chetan
 */
class DataListIterator implements Iterator<Buffer.Data>
{

  DataList.DataArray da;
  int count;

  public DataListIterator(DataList.DataArray da)
  {
    this.da = da;
    count = 0;
  }

  public boolean hasNext()
  {
    boolean retvalue;
    da.lockRead();
    try {
      retvalue = count < da.offset + da.count;
    }
    finally {
      da.unlockRead();
    }
    return retvalue;
  }

  public Buffer.Data next()
  {
    Buffer.Data retvalue;

    da.lockRead();
    try {
      if (count == da.offset + da.count) {
        retvalue = null;
      }
      else {
        retvalue = da.data[count++];

        if (count == da.offset + da.count) {
          if (da.next != null) {
            da = da.next;
            count = 0;
          }
        }
      }
    }
    finally {
      da.unlockRead();
    }
    return retvalue;
  }

  public void remove()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
