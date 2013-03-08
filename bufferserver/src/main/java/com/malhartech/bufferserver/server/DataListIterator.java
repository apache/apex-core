/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.server;

import com.malhartech.bufferserver.Buffer.Message.MessageType;
import com.malhartech.bufferserver.util.SerializedData;
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
  SerializedData previous = null;
  SerializedData current = new SerializedData();

  /**
   *
   * @param da
   * @param di
   */
  DataListIterator(DataList.DataArray da, DataIntrospector di)
  {
    da.acquire(true);
    this.da = da;
    this.di = di;

    current.bytes = da.data;
    current.offset = da.readingOffset;
  }

  /**
   *
   * @return boolean
   */
  public synchronized boolean hasNext()
  {
    while (true) {
      da.getNextData(current);
      switch (current.size) {
        case -1:
          return false;

        case 0:
          if (da.next == null) {
            return false;
          }

          da.release(false);
          da.next.acquire(true);
          da = da.next;
          current.bytes = da.data;
          current.offset = da.readingOffset;
          break;

        default:
          if (di.getType(current) != MessageType.NO_MESSAGE && di.getType(current) != MessageType.NO_MESSAGE_ODD) {
            return true;
          }
          current.offset += current.size;
          break;
      }
    }
  }

  @Override
  @SuppressWarnings("FinalizeDeclaration")
  protected void finalize() throws Throwable
  {
    da.release(false);
    super.finalize();
  }

  /**
   *
   * @return {@link com.malhartech.bufferserver.util.SerializedData}
   */
  public SerializedData next()
  {
    previous = current;
    current = new SerializedData();
    current.offset = previous.offset + previous.size;
    current.bytes = previous.bytes;
    return previous;
  }

  /**
   * Removes from the underlying collection the last element returned by the iterator (optional operation). This method can be called only once per call to
   * next. The behavior of an iterator is unspecified if the underlying collection is modified while the iteration is in progress in any way other than by
   * calling this method.
   */
  public void remove()
  {
    if (previous == null) {
      throw new IllegalStateException("Nothing to remove");
    }

    di.wipeData(previous);
  }

  /**
   *
   * @return MessageType
   */
  MessageType getType()
  {
    return di.getType(previous);
  }

  /**
   *
   * @return long
   */
  long getWindowId()
  {
    return di.getWindowId(previous);
  }

  /**
   *
   * @return Object
   */
  Object getData()
  {
    return di.getData(previous);
  }

}
