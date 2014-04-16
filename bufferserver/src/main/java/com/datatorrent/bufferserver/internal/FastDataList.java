/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.packet.Tuple;
import com.datatorrent.common.util.SerializedData;

/**
 * <p>FastDataList class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class FastDataList extends DataList
{
  public FastDataList(String identifier)
  {
    super(identifier);
  }

  public FastDataList(String identifier, int blocksize, int numberOfCacheBlocks)
  {
    super(identifier, blocksize, numberOfCacheBlocks);
  }

  long item;

  @Override
  public void flush(final int writeOffset)
  {
    flush:
    do {
      while (size == 0) {
        if (writeOffset - processingOffset >= 2) {
          size = last.data[processingOffset];
          size |= (last.data[processingOffset + 1] << 8);
//          logger.debug("read item = {} of size = {} at offset = {}", item++, size, processingOffset);
        }
        else {
          if (writeOffset == last.data.length) {
            processingOffset = 0;
            size = 0;
          }
          break flush;
        }
      }

      processingOffset += 2;

      if (processingOffset + size <= writeOffset) {
        switch (last.data[processingOffset]) {
          case MessageType.BEGIN_WINDOW_VALUE:
            Tuple btw = Tuple.getTuple(last.data, processingOffset, size);
            if (last.starting_window == -1) {
              last.starting_window = baseSeconds | btw.getWindowId();
              last.ending_window = last.starting_window;
            }
            else {
              last.ending_window = baseSeconds | btw.getWindowId();
            }
            break;

          case MessageType.RESET_WINDOW_VALUE:
            Tuple rwt = Tuple.getTuple(last.data, processingOffset, size);
            baseSeconds = (long)rwt.getBaseSeconds() << 32;
            break;
        }
        processingOffset += size;
        size = 0;
      }
      else {
        if (writeOffset == last.data.length) {
          processingOffset = 0;
          size = 0;
        }
        break;
      }
    }
    while (true);

    last.writingOffset = writeOffset;

    autoflushExecutor.submit(new Runnable()
    {
      @Override
      public void run()
      {
        for (DataListener dl : all_listeners) {
          dl.addedData();
        }
      }

    });
  }

  @Override
  public FastDataListIterator getIterator(Block block)
  {
    return new FastDataListIterator(block);
  }


  /* TODO: Are these functions required?
   @Override
   public void purge(int baseSeconds, int windowId)
   {
   long longWindowId = (long)baseSeconds << 32 | windowId;

   Block prev = null;
   for (Block temp = first; temp != null && temp.starting_window <= longWindowId; temp = temp.next) {
   if (temp.ending_window > longWindowId || temp == last) {
   if (prev != null) {
   first = temp;
   }

   first.purge(longWindowId, true);
   break;
   }

   if (storage != null && temp.uniqueIdentifier > 0) {
   //        logger.debug("discarding {} {} in purge", identifier, temp.uniqueIdentifier);
   storage.discard(identifier, temp.uniqueIdentifier);
   }

   prev = temp;
   }
   }

   @Override
   public void rewind(int baseSeconds, int windowId) throws IOException
   {
   long longWindowId = (long)baseSeconds << 32 | windowId;

   for (Block temp = first; temp != null; temp = temp.next) {
   if (temp.starting_window >= longWindowId || temp.ending_window > longWindowId) {
   if (temp != last) {
   temp.next = null;
   last = temp;
   }

   if(temp.data == null){
   temp.acquire(storage, false);
   }

   this.baseSeconds = temp.rewind(longWindowId, true);
   processingOffset = temp.writingOffset;
   size = 0;
   }
   }

   for (DataListIterator dli: iterators.values()) {
   dli.rewind(processingOffset);
   }
   }
   */
  /**
   * <p>FastDataListIterator class.</p>
   *
   * @author Chetan Narsude <chetan@datatorrent.com>
   * @since 0.3.2
   */
  protected class FastDataListIterator extends DataListIterator
  {
    FastDataListIterator(Block da)
    {
      super(da);
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

            da.release(false);
            da.next.acquire(true);
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
              da.release(false);
              da.next.acquire(true);
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

  private static final Logger logger = LoggerFactory.getLogger(FastDataList.class);
}
