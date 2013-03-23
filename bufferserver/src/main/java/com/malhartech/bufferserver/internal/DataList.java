/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.internal;

import com.malhartech.bufferserver.packet.MessageType;
import com.malhartech.bufferserver.packet.Tuple;
import com.malhartech.bufferserver.storage.Storage;
import com.malhartech.bufferserver.util.BitVector;
import com.malhartech.bufferserver.util.Codec;
import com.malhartech.bufferserver.util.Codec.MutableInt;
import com.malhartech.bufferserver.util.SerializedData;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains list of data and manages addition and deletion of the data<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class DataList
{
  private static final Logger logger = LoggerFactory.getLogger(DataList.class);
  private final String identifier;
  private final Integer blocksize;
  private HashMap<BitVector, HashSet<DataListener>> listeners = new HashMap<BitVector, HashSet<DataListener>>();
  private HashSet<DataListener> all_listeners = new HashSet<DataListener>();
  private Block first;
  private Block last;
  private Storage storage;

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

        this.baseSeconds = temp.rewind(longWindowId);
        processingOffset = temp.writingOffset;
        size = 0;
      }
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

    Block prev = null;
    for (Block temp = first; temp != null && temp.starting_window <= longWindowId; temp = temp.next) {
      if (temp.ending_window > longWindowId || temp == last) {
        if (prev != null) {
          first = temp;
        }

        first.purge(longWindowId);
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

  public DataList(String identifier, int blocksize)
  {
    this.identifier = identifier;
    this.blocksize = blocksize;
    first = new Block();
    first.data = new byte[blocksize];
    last = first;
  }

  public DataList(String identifier)
  {
    /*
     * We use 64MB (the default HDFS block getSize) as the getSize of the memory pool so we can flush the data 1 block at a time to the filesystem.
     */
    this(identifier, 64 * 1024 * 1024);
  }
//    if (writingOffset == 0) {
//      if (discard > 0) {
//        if (writeOffset < discard) {
//          return;
//        }
//        else {
//          writeOffset = discard;
//          discard = 0;
//        }
//      }
//      else if (prev != null) {
//        int size = Codec.readVarInt(prev.data, prevOffset, prev.data.length, newOffset);
//        if (newOffset.integer > prevOffset) {
//          int remainingLength = size - prev.data.length + newOffset.integer;
//          if (remainingLength > writeOffset) {
//            return; /* we still do not have enough data */
//          }
//          else {
//            byte[] buffer = new byte[size];
//            System.arraycopy(prev.data, newOffset.integer, buffer, 0, size - remainingLength);
//            System.arraycopy(data, 0, buffer, prev.data.length - newOffset.integer, remainingLength);
//            // we have our new object in the buffer!
//          }
//        }
//        else if (newOffset.integer != -5) { /* we do not have enough bytes to read even the int */
//
//        }
//      }
//    }
//
//    while (writingOffset < writeOffset) {
//    }

  MutableInt nextOffset = new MutableInt();
  long baseSeconds;
  int size;
  int processingOffset;

  public final void flush(int writeOffset)
  {
    logger.debug("processingOffset = {} and writeOffset = {}", new Object[] {processingOffset, writeOffset});
    flush:
    do {
      while (size == 0) {
        size = Codec.readVarInt(last.data, processingOffset, writeOffset, nextOffset);
        switch (nextOffset.integer) {
          case -5:
            throw new RuntimeException("problemo!");

          case -4:
          case -3:
          case -2:
          case -1:
          case 0:
            if (writeOffset == last.data.length) {
              last.writingOffset = processingOffset;
              processingOffset = 0;
            }
            break flush;
        }
      }

      if (nextOffset.integer + size < writeOffset) {
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
        processingOffset = size + nextOffset.integer;
        size = 0;
      }
      else {
        last.writingOffset = processingOffset;
        break;
      }
    }
    while (true);

    for (DataListener dl : all_listeners) {
      dl.dataAdded();
    }
  }

  public void setSecondaryStorage(Storage storage)
  {
    this.storage = storage;
  }

  /*
   * Iterator related functions.
   */
  private final HashMap<String, DataListIterator> iterators = new HashMap<String, DataListIterator>();

  public Iterator<SerializedData> newIterator(String identifier, long windowId)
  {
    for (Block temp = first; temp != null; temp = temp.next) {
      if (true || temp.starting_window >= windowId || temp.ending_window > windowId) { // for now always send the first
        DataListIterator dli = new DataListIterator(temp);
        iterators.put(identifier, dli);
        return dli;
      }
    }

    DataListIterator dli = new DataListIterator(last);
    iterators.put(identifier, dli);
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
          dli.da.release(false);
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
      dli.da.release(false);
      dli.da = null;
    }

    iterators.clear();

    return count;
  }

  public void addDataListener(DataListener dl)
  {
    all_listeners.add(dl);
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
    last.next = new Block();
    long windowId = last.ending_window;
    last = last.next;
    last.starting_window = windowId;
    last.data = array;
  }

  public byte[] getBufer()
  {
    return last.data;
  }

  public int getPosition()
  {
    return last.writingOffset;
  }

}
