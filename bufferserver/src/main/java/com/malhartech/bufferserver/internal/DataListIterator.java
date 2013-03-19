/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.internal;

import com.malhartech.bufferserver.packet.MessageType;
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
  Block da;
  SerializedData previous = null;
  SerializedData current = new SerializedData();

  /**
   *
   * @param da
   */
  DataListIterator(Block da)
  {
    da.acquire(true);
    this.da = da;

    current.bytes = da.data;
    current.offset = da.readingOffset;
  }

  /**
   *
   * @return boolean
   */
  @Override
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
          switch (current.bytes[current.dataOffset]) {
            case MessageType.NO_MESSAGE_VALUE:
            case MessageType.NO_MESSAGE_ODD_VALUE:
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
  @Override
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
  @Override
  public void remove()
  {
    if (previous == null) {
      throw new IllegalStateException("Nothing to remove");
    }

    previous.bytes[previous.dataOffset] = MessageType.NO_MESSAGE_VALUE;
  }

}
