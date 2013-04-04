/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.internal;

import com.malhartech.bufferserver.packet.BeginWindowTuple;
import com.malhartech.bufferserver.packet.MessageType;
import com.malhartech.bufferserver.packet.ResetWindowTuple;
import com.malhartech.bufferserver.packet.Tuple;
import com.malhartech.bufferserver.storage.Storage;
import com.malhartech.bufferserver.util.Codec;
import com.malhartech.bufferserver.util.SerializedData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Block
{
  final String identifier;
  /**
   * actual data - stored as length followed by actual data.
   */
  byte data[];
  /**
   * readingOffset is the offset of the first valid byte in the array.
   */
  int readingOffset;
  /**
   * writingOffset is the offset of the first available byte to write into.
   */
  int writingOffset;
  /**
   * The starting window which is available in this data array.
   */
  long starting_window = -1;
  /**
   * the ending window which is available in this data array
   */
  long ending_window;
  /**
   * when the data is null, uniqueIdentifier is the identifier in the backup storage to retrieve the object.
   */
  int uniqueIdentifier;
  /**
   * the next in the chain.
   */
  Block next;

  /**
   * how count of references to this block.
   */
  int refCount;

  public Block(String id, int size)
  {
    this(id, new byte[size]);
  }

  public Block(String id, byte[] array)
  {
    identifier = id;
    data = array;
    refCount = 1;
  }

  void getNextData(SerializedData current)
  {
    if (current.offset < writingOffset) {
      Codec.readRawVarInt32(current);
      if (current.offset + current.size > writingOffset) {
        current.size = 0;
      }
    }
    else {
      current.size = 0;
    }
  }

  public long rewind(long windowId)
  {
    long bs = starting_window & 0x7fffffff00000000L;
    DataListIterator dli = new DataListIterator(this, null);
    done:
    while (dli.hasNext()) {
      SerializedData sd = dli.next();
      switch (sd.bytes[sd.dataOffset]) {
        case MessageType.RESET_WINDOW_VALUE:
          ResetWindowTuple rwt = (ResetWindowTuple)Tuple.getTuple(sd.bytes, sd.dataOffset, sd.size - sd.dataOffset + sd.offset);
          bs = (long)rwt.getBaseSeconds() << 32;
          if (bs > windowId) {
            writingOffset = sd.offset;
            break done;
          }
          break;

        case MessageType.BEGIN_WINDOW_VALUE:
          BeginWindowTuple bwt = (BeginWindowTuple)Tuple.getTuple(sd.bytes, sd.dataOffset, sd.size - sd.dataOffset + sd.offset);
          if ((bs | bwt.getWindowId()) >= windowId) {
            writingOffset = sd.offset;
            break done;
          }
          break;
      }
    }

    return bs;
  }

  public void purge(long longWindowId)
  {
    logger.debug("starting_window = {}, longWindowId = {}, ending_window = {}",
                 new Object[] {Codec.getStringWindowId(starting_window), Codec.getStringWindowId(longWindowId), Codec.getStringWindowId(ending_window)});
    boolean found = false;
    long bs = starting_window & 0xffffffff00000000L;
    SerializedData lastReset = null;

    DataListIterator dli = new DataListIterator(this, null);
    done:
    while (dli.hasNext()) {
      SerializedData sd = dli.next();
      switch (sd.bytes[sd.dataOffset]) {
        case MessageType.RESET_WINDOW_VALUE:
          ResetWindowTuple rwt = (ResetWindowTuple)Tuple.getTuple(sd.bytes, sd.dataOffset, sd.size - sd.dataOffset + sd.offset);
          bs = (long)rwt.getBaseSeconds() << 32;
          lastReset = sd;
          break;

        case MessageType.BEGIN_WINDOW_VALUE:
          BeginWindowTuple bwt = (BeginWindowTuple)Tuple.getTuple(sd.bytes, sd.dataOffset, sd.size - sd.dataOffset + sd.offset);
          if ((bs | bwt.getWindowId()) > longWindowId) {
            found = true;
            if (lastReset != null) {
              /*
               * Restore the last Reset tuple if there was any and adjust the writingOffset to the beginning of the reset tuple.
               */
              if (sd.offset >= lastReset.size) {
                sd.offset -= lastReset.size;
                if (!(sd.bytes == lastReset.bytes && sd.offset == lastReset.offset)) {
                  System.arraycopy(lastReset.bytes, lastReset.offset, sd.bytes, sd.offset, lastReset.size);
                }
              }

              this.starting_window = bs | bwt.getWindowId();
              this.readingOffset = sd.offset;
            }

            break done;
          }
      }
    }

    /**
     * If we ended up purging all the data from the current Block then,
     * it also makes sense to start all over.
     * It helps with better utilization of the RAM.
     */
    if (!found) {
      //logger.debug("we could not find a tuple which is in a window later than the window to be purged, so this has to be the last window published so far");
      if (lastReset != null && lastReset.offset != 0) {
        this.readingOffset = this.writingOffset - lastReset.size;
        System.arraycopy(lastReset.bytes, lastReset.offset, this.data, this.readingOffset, lastReset.size);
        this.starting_window = this.ending_window = bs;
      }
      else {
        this.readingOffset = this.writingOffset;
        this.starting_window = this.ending_window = 0;
      }


      SerializedData sd = new SerializedData(this.data, readingOffset, 0);

      // the rest of it is just a copy from beginWindow case here to wipe the data - refactor
      int i = 1;
      while (i < Codec.getSizeOfRawVarint32(sd.offset - i)) {
        i++;
      }

      if (i <= sd.offset) {
        sd.size = sd.offset;
        sd.offset = 0;
        sd.dataOffset = Codec.writeRawVarint32(sd.size - i, sd.bytes, sd.offset, i);
        sd.bytes[sd.dataOffset] = MessageType.NO_MESSAGE_VALUE;
      }
      else {
        logger.warn("Unhandled condition while purging the data purge to offset {}", sd.offset);
      }
    }
  }

  void acquire(final Storage storage, boolean wait)
  {
    refCount++;
    if (data == null && storage != null) {
      Runnable r = new Runnable()
      {
        @Override
        public void run()
        {
          data = storage.retrieve(identifier, uniqueIdentifier);
          readingOffset = 0;
          writingOffset = data.length;
        }

      };

      if (wait) {
        r.run();
      }
      else {
        new Thread(r).start();
      }
    }
  }

  void release(final Storage storage, boolean wait)
  {
    refCount--;
    if (refCount == 0 && storage != null) {
      Runnable r = new Runnable()
      {
        @Override
        public void run()
        {
          try {
            int i = storage.store(identifier, uniqueIdentifier, data, readingOffset, writingOffset);
            if (i == 0) {
              logger.warn("Storage returned unexpectedly, please check the status of the spool directory!");
            }
            else {
              uniqueIdentifier = i;
              data = null;
            }
          }
          catch (RuntimeException ex) {
            logger.warn("Storage failed!", ex);
          }
        }

      };

      if (wait) {
        r.run();
      }
      else {
        new Thread(r).start();
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(Block.class);
}
