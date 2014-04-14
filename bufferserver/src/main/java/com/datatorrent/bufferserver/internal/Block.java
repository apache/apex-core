/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.packet.BeginWindowTuple;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.packet.ResetWindowTuple;
import com.datatorrent.bufferserver.packet.Tuple;
import com.datatorrent.bufferserver.storage.Storage;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.common.util.SerializedData;
import com.datatorrent.common.util.VarInt;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * <p>Block class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
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
  /**
   * Executor which takes care of storing and retrieving blocks from the storage.
   */
  private ScheduledExecutorService executor;

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

  @Override
  @SuppressWarnings("FinalizeDeclaration")
  protected void finalize() throws Throwable
  {
    super.finalize();
    if (executor != null) {
      executor.shutdown();
    }
  }

  void getNextData(SerializedData current)
  {
    if (current.offset < writingOffset) {
      VarInt.read(current);
      if (current.offset + current.size > writingOffset) {
        current.size = 0;
      }
    }
    else {
      current.size = 0;
    }
  }

  public long rewind(long windowId, boolean fast, Storage storage)
  {
    long bs = starting_window & 0x7fffffff00000000L;
    DataListIterator dli = fast ? new FastDataListIterator(this, storage) : new DataListIterator(this, storage);
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

    if (starting_window == -1) {
      starting_window = windowId;
      ending_window = windowId;
      //logger.debug("assigned both window id {}", this);
    }
    else if (windowId < ending_window) {
      ending_window = windowId;
      //logger.debug("assigned end window id {}", this);
    }

    return bs;
  }

  public void purge(long longWindowId, boolean fast, Storage storage)
  {
//    logger.debug("starting_window = {}, longWindowId = {}, ending_window = {}",
//                 new Object[] {VarInt.getStringWindowId(starting_window), VarInt.getStringWindowId(longWindowId), VarInt.getStringWindowId(ending_window)});
    boolean found = false;
    long bs = starting_window & 0xffffffff00000000L;
    SerializedData lastReset = null;

    DataListIterator dli = fast ? new FastDataListIterator(this, storage) : new DataListIterator(this, storage);
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
              //logger.debug("assigned starting window id {}", this);
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
        //logger.debug("=20140220= reassign the windowids {}", this);
      }
      else {
        this.readingOffset = this.writingOffset;
        this.starting_window = this.ending_window = longWindowId;
        //logger.debug("=20140220= avoid the windowids {}", this);
      }


      SerializedData sd = new SerializedData(this.data, readingOffset, 0);

      // the rest of it is just a copy from beginWindow case here to wipe the data - refactor
      int i = 1;
      while (i < VarInt.getSize(sd.offset - i)) {
        i++;
      }

      if (i <= sd.offset) {
        sd.size = sd.offset;
        sd.offset = 0;
        sd.dataOffset = VarInt.write(sd.size - i, sd.bytes, sd.offset, i);
        sd.bytes[sd.dataOffset] = MessageType.NO_MESSAGE_VALUE;
      }
      else {
        logger.warn("Unhandled condition while purging the data purge to offset {}", sd.offset);
      }
    }
  }

  private void initExecutor()
  {
    executor = Executors.newSingleThreadScheduledExecutor(new NameableThreadFactory("StorageExecutor(" + identifier + ')'));
  }

  synchronized void acquire(final Storage storage, boolean wait)
  {
    refCount++;
    if (data == null && storage != null) {
      Runnable r = new Runnable()
      {
        @Override
        public void run()
        {
          synchronized (Block.this) {
            if (data != null) {
              return;
            }
          }
          data = storage.retrieve(identifier, uniqueIdentifier);
          readingOffset = 0;
          writingOffset = data.length;
        }

      };

      if (wait) {
        r.run();
      }
      else {
        if (executor == null) {
          initExecutor();
        }
        executor.schedule(r, 0, TimeUnit.MILLISECONDS);
      }
    }
  }

  synchronized void release(final Storage storage, boolean wait)
  {
    if (refCount == 1 && storage != null) {
      Runnable r = new Runnable()
      {
        @Override
        public void run()
        {
          synchronized (Block.this) {
            if (--refCount != 0) {
              return;
            }
          }

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
        if (executor == null) {
          initExecutor();
        }
        executor.schedule(r, 0, TimeUnit.MILLISECONDS);
      }
    }
  }

  @Override
  public String toString()
  {
    return "Block{" + "identifier=" + identifier + ", data=" + (data == null ? "null" : data.length)
           + ", readingOffset=" + readingOffset + ", writingOffset=" + writingOffset
           + ", starting_window=" + Codec.getStringWindowId(starting_window) + ", ending_window=" + Codec.getStringWindowId(ending_window)
           + ", uniqueIdentifier=" + uniqueIdentifier + ", next=" + (next == null ? "null" : next.identifier)
           + ", refCount=" + refCount + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(Block.class);
}
