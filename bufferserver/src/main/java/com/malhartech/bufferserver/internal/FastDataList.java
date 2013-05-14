/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.internal;

import com.malhartech.bufferserver.packet.MessageType;
import com.malhartech.bufferserver.packet.Tuple;
import com.malhartech.bufferserver.util.Codec;
import com.malhartech.bufferserver.util.SerializedData;
import java.util.Iterator;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class FastDataList extends DataList
{
  public FastDataList(String identifier)
  {
    super(identifier);
  }

  public FastDataList(String identifier, int blocksize)
  {
    super(identifier, blocksize);
  }

  @Override
  public void flush(final int writeOffset)
  {
    flush:
    do {
      while (size == 0) {
        if (writeOffset - processingOffset >= 2) {
          size = last.data[processingOffset];
          size |= (last.data[processingOffset] << 16);
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

    executor.submit(new Runnable()
    {
      @Override
      public void run()
      {
        for (DataListener dl: all_listeners) {
          dl.addedData();
        }
      }

    });
  }

  @Override
  public Iterator<SerializedData> newIterator(String identifier, long windowId)
  {
    for (Block temp = first; temp != null; temp = temp.next) {
      if (true || temp.starting_window >= windowId || temp.ending_window > windowId) { // for now always send the first
        DataListIterator dli = new FastDataListIterator(temp, storage);
        iterators.put(identifier, dli);
        return dli;
      }
    }

    DataListIterator dli = new FastDataListIterator(last, storage);
    iterators.put(identifier, dli);
    return dli;
  }

}
