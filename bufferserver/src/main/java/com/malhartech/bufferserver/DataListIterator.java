/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.bufferserver.util.SerializedData;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author chetan
 */
class DataListIterator implements Iterator<SerializedData>
{
  private static final Logger logger = LoggerFactory.getLogger(DataListIterator.class);
  DataList.DataArray da;
  final DataIntrospector di;
  byte[] rollover;
  SerializedData previous = new SerializedData();
  SerializedData current = new SerializedData();

  public DataListIterator(DataList.DataArray da, DataIntrospector di)
  {
    this.da = da;
    this.di = di;
    
    previous.bytes = current.bytes = da.data;
  }

  public boolean hasNext()
  {
    da.getNextData(current);
    switch (current.size) {
      case -1:
        return false;

      case 0:
        if (da.next == null) {
          return false;
        }
        
        da = da.next;
        rollover = previous.bytes;
        current.offset = 0;
        previous.bytes = current.bytes = da.data;
        return hasNext();

      default:
        if (di.getType(current) == DataType.NO_DATA) {
          current.offset += current.size;
          return hasNext();
        }
        
        return true;
    }
  }

  public SerializedData next()
  {
    SerializedData temp = previous;
    previous = current;
    current = temp;
    current.offset = previous.offset + previous.size;
//    logger.debug("found object {}", previous);
    return previous;
  }

  /**
   * Removes from the underlying collection the last element returned by the iterator (optional operation).
   * This method can be called only once per call to next. The behavior of an iterator is unspecified if
   * the underlying collection is modified while the iteration is in progress in any way other than by 
   * calling this method.   
   */
  public void remove()
  {
    if (previous.size == 0) {
      throw new IllegalStateException("Nothing to remove");
    }
    
    if (rollover == null) {
      di.wipeData(previous);
    }
    else {
      previous.bytes = rollover;
      rollover = null;
      di.wipeData(previous);
      previous.bytes = current.bytes;
    }
    
    previous.size = 0;
  }

  DataType getType()
  {
    return di.getType(previous);
  }

  long getWindowId()
  {
    return di.getWindowId(previous);
  }

  ByteBuffer getPartitionedData()
  {
    return di.getPartitionedData(previous);
  }
}
