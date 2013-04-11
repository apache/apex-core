/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.internal;

import com.malhartech.bufferserver.packet.MessageType;
import com.malhartech.bufferserver.storage.Storage;
import com.malhartech.bufferserver.util.Codec;
import com.malhartech.bufferserver.util.Codec.MutableInt;
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
  Block da;
  SerializedData current;
  private final Storage storage;
  private byte[] buffer;
  private int readOffset;
  MutableInt nextOffset = new MutableInt();
  int size;

  /**
   *
   * @param da
   */
  DataListIterator(Block da, Storage storage)
  {
    da.acquire(storage, true);
    this.da = da;
    this.storage = storage;
    buffer = da.data;
    readOffset = da.readingOffset;
  }

  // this is a hack! Get rid of it.
  public int getBaseSeconds()
  {
    return da == null ? 0 : (int)(da.starting_window >> 32);
  }

  /**
   *
   * @return boolean
   */
  @Override
  public synchronized boolean hasNext()
  {
    while (size == 0) {
      size = Codec.readVarInt(buffer, readOffset, da.writingOffset, nextOffset);
      switch (nextOffset.integer) {
        case -5:
          throw new RuntimeException("problemo!");

        case -4:
        case -3:
        case -2:
        case -1:
        case 0:
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
      if (nextOffset.integer + size <= da.writingOffset) {
        current = new SerializedData(buffer, readOffset, size + nextOffset.integer - readOffset);
        current.dataOffset = nextOffset.integer;
        //if (buffer[current.dataOffset] == MessageType.BEGIN_WINDOW_VALUE || buffer[current.dataOffset] == MessageType.END_WINDOW_VALUE) {
        //  Tuple t = Tuple.getTuple(current.bytes, current.dataOffset, current.size - current.dataOffset + current.offset);
        //  logger.debug("next t = {}", t);
        //}
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

  @Override
  @SuppressWarnings("FinalizeDeclaration")
  protected void finalize() throws Throwable
  {
    da.release(storage, false);
    super.finalize();
  }

  /**
   *
   * @return {@link com.malhartech.bufferserver.util.SerializedData}
   */
  @Override
  public SerializedData next()
  {
    readOffset = current.offset + current.size;
    size = 0;
    return current;
  }

  /**
   * Removes from the underlying collection the last element returned by the iterator (optional operation). This method can be called only once per call to
   * next. The behavior of an iterator is unspecified if the underlying collection is modified while the iteration is in progress in any way other than by
   * calling this method.
   */
  @Override
  public void remove()
  {
    current.bytes[current.dataOffset] = MessageType.NO_MESSAGE_VALUE;
  }

  void rewind(int processingOffset)
  {
    readOffset = processingOffset;
    size = 0;
  }

  private static final Logger logger = LoggerFactory.getLogger(DataListIterator.class);
}
