/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.internal;

import com.datatorrent.bufferserver.storage.Storage;
import com.datatorrent.bufferserver.util.SerializedData;

/**
 * <p>FastDataListIterator class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
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
        size |= (buffer[readOffset + 1] << 8);
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
