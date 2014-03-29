/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.internal;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.packet.Tuple;
import com.datatorrent.bufferserver.storage.Storage;
import com.datatorrent.bufferserver.util.BitVector;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.common.util.SerializedData;
import com.datatorrent.common.util.VarInt;
import com.datatorrent.common.util.VarInt.MutableInt;

/**
 * Maintains list of data and manages addition and deletion of the data<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class DataList
{
  private static final Logger logger = LoggerFactory.getLogger(DataList.class);
  protected final String identifier;
  private final Integer blocksize;
  private HashMap<BitVector, HashSet<DataListener>> listeners = new HashMap<BitVector, HashSet<DataListener>>();
  protected HashSet<DataListener> all_listeners = new HashSet<DataListener>();
  protected Block first;
  protected Block last;
  protected Storage storage;
  protected ScheduledExecutorService executor;
  private final int MAX_COUNT_OF_INMEM_BLOCKS;

  public int getBlockSize()
  {
    return blocksize;
  }

  public void rewind(int baseSeconds, int windowId) throws IOException
  {
    long longWindowId = (long)baseSeconds << 32 | windowId;

    for (Block temp = first; temp != null; temp = temp.next) {

      if (temp.starting_window >= longWindowId || temp.ending_window > longWindowId) {
        if (temp != last) {
          temp.next = null;
          last = temp;
        }

        this.baseSeconds = temp.rewind(longWindowId, false, storage);
        processingOffset = temp.writingOffset;
        size = 0;
      }
    }

    for (DataListIterator dli : iterators.values()) {
      dli.rewind(processingOffset);
    }
  }

  public void reset()
  {
    listeners.clear();
    all_listeners.clear();

    if (storage != null) {
      while (first != null) {
        if (first.uniqueIdentifier > 0) {
          logger.debug("discarding {} {} in reset", identifier, first.uniqueIdentifier);
          storage.discard(identifier, first.uniqueIdentifier);
        }
        first = first.next;
      }
    }
  }

  public void purge(int baseSeconds, int windowId)
  {
    long longWindowId = (long)baseSeconds << 32 | windowId;
    logger.debug("purge request for windowId {}", Codec.getStringWindowId(longWindowId));

    Block prev = null;
    for (Block temp = first; temp != null && temp.starting_window <= longWindowId; temp = temp.next) {
      if (temp.ending_window > longWindowId || temp == last) {
        if (prev != null) {
          first = temp;
        }

        first.purge(longWindowId, false, storage);
        break;
      }

      if (storage != null && temp.uniqueIdentifier > 0) {
        logger.debug("discarding {} {} in purge", identifier, temp.uniqueIdentifier);

        storage.discard(identifier, temp.uniqueIdentifier);
      }

      prev = temp;
    }
  }

  /**
   * @return the identifier
   */
  public String getIdentifier()
  {
    return identifier;
  }

  public DataList(String identifier, int blocksize, int numberOfCacheBlocks)
  {
    this.identifier = identifier;
    this.blocksize = blocksize;
    first = new Block(identifier, blocksize);
    last = first;
    this.MAX_COUNT_OF_INMEM_BLOCKS = numberOfCacheBlocks;

  }

  public DataList(String identifier)
  {
    /*
     * We use 64MB (the default HDFS block getSize) as the getSize of the memory pool so we can flush the data 1 block at a time to the filesystem.
     * we will use default value of 8 block sizes to be cached in memory
     */
    this(identifier, 64 * 1024 * 1024, 8);
  }

  MutableInt nextOffset = new MutableInt();
  long baseSeconds;
  int size;
  int processingOffset;

  public void flush(final int writeOffset)
  {
    //logger.debug("size = {}, processingOffset = {}, nextOffset = {}, writeOffset = {}", size, processingOffset, nextOffset.integer, writeOffset);
    flush:
    do {
      while (size == 0) {
        size = VarInt.read(last.data, processingOffset, writeOffset, nextOffset);
        switch (nextOffset.integer) {
          case -5:
            throw new RuntimeException("problemo!");

          case -4:
          case -3:
          case -2:
          case -1:
          case 0:
            if (writeOffset == last.data.length) {
              nextOffset.integer = 0;
              processingOffset = 0;
              size = 0;
            }
            break flush;
        }
      }

      processingOffset = nextOffset.integer;

      if (processingOffset + size <= writeOffset) {
        switch (last.data[processingOffset]) {
          case MessageType.BEGIN_WINDOW_VALUE:
            Tuple bwt = Tuple.getTuple(last.data, processingOffset, size);
            if (last.starting_window == -1) {
              last.starting_window = baseSeconds | bwt.getWindowId();
              last.ending_window = last.starting_window;
              //logger.debug("assigned both window id {}", last);
            }
            else {
              last.ending_window = baseSeconds | bwt.getWindowId();
              //logger.debug("assigned last window id {}", last);
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
          nextOffset.integer = 0;
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
        for (DataListener dl : all_listeners) {
          dl.addedData();
        }
      }

    });
  }

  public void setAutoflush(final ScheduledExecutorService es)
  {
    executor = es;
  }

  public void setSecondaryStorage(Storage storage)
  {
    this.storage = storage;
  }

  /*
   * Iterator related functions.
   */
  protected final HashMap<String, DataListIterator> iterators = new HashMap<String, DataListIterator>();

  public Iterator<SerializedData> newIterator(String identifier, long windowId)
  {
    //logger.debug("request for a new iterator {} and {}", identifier, windowId);
    for (Block temp = first; temp != null; temp = temp.next) {
      if (temp.starting_window >= windowId || temp.ending_window > windowId) {
        DataListIterator dli = new DataListIterator(temp, storage);
        iterators.put(identifier, dli);
        //logger.debug("returning new iterator on temp = {}", temp);
        return dli;
      }
    }

    DataListIterator dli = new DataListIterator(last, storage);
    iterators.put(identifier, dli);
    //logger.debug("returning new iterator on last = {}", last);
    return dli;
  }

  /**
   * Release previous acquired iterator from this DataList
   *
   * @param iterator
   * @return true if successfully released, false otherwise.
   */
  public boolean delIterator(Iterator<SerializedData> iterator)
  {
    boolean released = false;
    if (iterator instanceof DataListIterator) {
      DataListIterator dli = (DataListIterator)iterator;
      for (Entry<String, DataListIterator> e : iterators.entrySet()) {
        if (e.getValue() == dli) {
          iterators.remove(e.getKey());
          released = true;
          dli.da.release(storage, false);
          dli.da = null;
          break;
        }
      }
    }
    return released;
  }

  /**
   *
   * @return the currentOffset of iterators
   */
  public int clearIterators()
  {
    int count = 0;

    for (DataListIterator dli : iterators.values()) {
      count++;
      dli.da.release(storage, false);
      dli.da = null;
    }

    iterators.clear();

    return count;
  }

  public void addDataListener(DataListener dl)
  {
    all_listeners.add(dl);
    //logger.debug("total {} listeners {} -> {}", all_listeners.size(), dl, this);
    ArrayList<BitVector> partitions = new ArrayList<BitVector>();
    if (dl.getPartitions(partitions) > 0) {
      for (BitVector partition : partitions) {
        HashSet<DataListener> set;
        if (listeners.containsKey(partition)) {
          set = listeners.get(partition);
        }
        else {
          set = new HashSet<DataListener>();
          listeners.put(partition, set);
        }
        set.add(dl);
      }
    }
    else {
      HashSet<DataListener> set;
      if (listeners.containsKey(DataListener.NULL_PARTITION)) {
        set = listeners.get(DataListener.NULL_PARTITION);
      }
      else {
        set = new HashSet<DataListener>();
        listeners.put(DataListener.NULL_PARTITION, set);
      }

      set.add(dl);
    }
  }

  public void removeDataListener(DataListener dl)
  {
    ArrayList<BitVector> partitions = new ArrayList<BitVector>();
    if (dl.getPartitions(partitions) > 0) {
      for (BitVector partition : partitions) {
        if (listeners.containsKey(partition)) {
          listeners.get(partition).remove(dl);
        }
      }
    }
    else {
      if (listeners.containsKey(DataListener.NULL_PARTITION)) {
        listeners.get(DataListener.NULL_PARTITION).remove(dl);
      }
    }

    all_listeners.remove(dl);
  }

  public void addBuffer(byte[] array)
  {
    last.next = new Block(identifier, array);
    last.next.starting_window = last.ending_window;
    last.next.ending_window = last.ending_window;
    last = last.next;

    //logger.debug("addbuffer last = {}", last);
    int inmemBlockCount;

    inmemBlockCount = 0;
    for (Block temp = first; temp != null; temp = temp.next) {
      if (temp.data != null) {
        inmemBlockCount++;
      }
    }

    if (inmemBlockCount >= MAX_COUNT_OF_INMEM_BLOCKS) {
      //logger.debug("InmemBlockCount before releaes {}", inmemBlockCount);
      for (Block temp = first; temp != null; temp = temp.next) {
        boolean found = false;
        for (DataListIterator iterator : iterators.values()) {
          if (iterator.da == temp) {
            found = true;
            break;
          }
        }

        if (!found && temp.data != null) {
          temp.release(storage, false);
          if (--inmemBlockCount < MAX_COUNT_OF_INMEM_BLOCKS) {
            break;
          }
        }
      }
      //logger.debug("InmemBlockCount after release {}", inmemBlockCount);
    }
  }

  public byte[] getBuffer(long windowId)
  {
    //logger.debug("getBuffer windowid = {} when starting_window = {}", windowId, last.starting_window);
    if (last.starting_window == -1) {
      last.starting_window = windowId;
      last.ending_window = windowId;
      baseSeconds = windowId & 0xffffffff00000000L;
    }
    return last.data;
  }

  public int getPosition()
  {
    return last.writingOffset;
  }

  public static class Status
  {
    public long numBytesWaiting = 0;
    public long numBytesAllocated = 0;
    public String slowestConsumer;
  }

  public Status getStatus()
  {
    Status status = new Status();

    // When the number of subscribers becomes high or the number of blocks becomes high, consider optimize it.
    Block b = first;
    Map<Block, Integer> indices = new HashMap<Block, Integer>();
    int i = 0;
    while (b != null) {
      indices.put(b, i++);
      b = b.next;
    }
    int oldestBlockIndex = Integer.MAX_VALUE;
    int oldestReadOffset = Integer.MAX_VALUE;

    for (Map.Entry<String, DataListIterator> entry : iterators.entrySet()) {
      Integer index = indices.get(entry.getValue().da);
      if (index == null) {
        // error
        throw new RuntimeException("problemo!");
      }
      if (index.intValue() < oldestBlockIndex) {
        oldestBlockIndex = index.intValue();
        oldestReadOffset = entry.getValue().getReadOffset();
        status.slowestConsumer = entry.getKey();
      }
      else if (index.intValue() == oldestBlockIndex && entry.getValue().getReadOffset() < oldestReadOffset) {
        oldestReadOffset = entry.getValue().getReadOffset();
        status.slowestConsumer = entry.getKey();
      }
    }

    b = first;
    i = 0;
    while (b != null) {
      status.numBytesAllocated += b.data.length;
      if (oldestBlockIndex == i) {
        status.numBytesWaiting += b.writingOffset - oldestReadOffset;
      }
      else if (oldestBlockIndex < i) {
        status.numBytesWaiting += b.writingOffset - b.readingOffset;
      }
      b = b.next;
      ++i;
    }
    return status;
  }

}
