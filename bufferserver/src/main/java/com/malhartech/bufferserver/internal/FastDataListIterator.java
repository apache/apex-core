/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.internal;

import com.malhartech.bufferserver.storage.Storage;
import com.malhartech.bufferserver.util.SerializedData;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class FastDataListIterator extends DataListIterator
{
  FastDataListIterator(Block da, Storage storage)
  {
    super(da, storage);
  }

  @Override
  public synchronized boolean hasNext()
  {
    while (size == 0) {
      if (da.writingOffset - readOffset >= 2) {
        size = buffer[readOffset];
        size |= (buffer[readOffset + 1] << 16);
      }
      else {
        if (da.writingOffset == buffer.length) {
          if (da.next == null) {
            return false;
          }

          da.release(storage, false);
          da.next.acquire(storage, true);
          da = da.next;
          size = 0;
          buffer = da.data;
          readOffset = da.readingOffset;
        }
        else {
          return false;
        }
      }
    }

    while (true) {
      if (readOffset + size + 2 <= da.writingOffset) {
        current = new SerializedData(buffer, readOffset, size + 2);
        current.dataOffset = readOffset + 2;
        return true;
      }
      else {
        if (da.writingOffset == buffer.length) {
          if (da.next == null) {
            return false;
          }
          else {
            da.release(storage, false);
            da.next.acquire(storage, true);
            da = da.next;
            size = 0;
            readOffset = nextOffset.integer = da.readingOffset;
            buffer = da.data;
          }
        }
        else {
          return false;
        }
      }
    }
  }

}
