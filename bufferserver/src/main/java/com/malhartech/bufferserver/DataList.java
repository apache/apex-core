/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.bufferserver.Buffer.ResetWindow;
import com.malhartech.bufferserver.util.Codec;
import com.malhartech.bufferserver.util.SerializedData;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Maintains list of data and manages addition and deletion of the data<p>
 * <br>
 *
 * @author chetan
 */
public class DataList
{
  private static final Logger logger = LoggerFactory.getLogger(DataList.class);
  /**
   * We use 64MB (the default HDFS block getSize) as the getSize of the memory pool so we can flush the data 1 block at a time to the filesystem.
   */
  private static final Integer BLOCKSIZE = 64 * 1024 * 1024;
  HashMap<ByteBuffer, HashSet<DataListener>> listeners = new HashMap<ByteBuffer, HashSet<DataListener>>();
  HashSet<DataListener> all_listeners = new HashSet<DataListener>();
  int capacity;
  String identifier;
  String type;
  static volatile DataArray free = null;
  volatile DataArray first;
  volatile DataArray last;

  Object getType()
  {
    return this.type;
  }

  synchronized void rewind(long longWindowId, DataIntrospector di)
  {
    for (DataArray temp = first; temp != last; temp = temp.next) {
      if (temp.starting_window >= longWindowId) {
        synchronized (BLOCKSIZE) {
          last.next = free;
          free = temp.next;
        }

        last = temp;
        last.next = null;
        break;
      }
    }

    if (last.starting_window >= longWindowId) {
      long baseSeconds = last.starting_window & 0xffffffff00000000L;
      last.lockWrite();
      try {
        DataListIterator dli = new DataListIterator(last, di);
        done:
        while (dli.hasNext()) {
          SerializedData sd = dli.next();
          switch (di.getType(sd)) {
            case RESET_WINDOW:
              baseSeconds = (long)di.getWindowId(sd) << 32;
              break;

            case BEGIN_WINDOW:
              if ((baseSeconds | di.getWindowId(sd)) >= longWindowId) {
                last.offset = sd.offset;
                Arrays.fill(last.data, last.offset, last.data.length - 1, Byte.MIN_VALUE);
                break done;
              }
              break;
          }
        }
      }
      finally {
        last.unlockWrite();
      }
    }
  }

  /**
   * @param capacity - it's ignored if we are reusing block
   * @return DataArray
   */
  private DataArray getDataArray(int capacity)
  {
    DataArray retval = null;

    synchronized (BLOCKSIZE) {
      if (free != null) {
        retval = free;
        free = free.next;
      }
    }

    if (retval == null) {
      retval = new DataArray(capacity);
    }
    else {
      retval.starting_window = retval.ending_window = retval.offset = 0;
      retval.next = null;
    }

    Arrays.fill(retval.data, Byte.MIN_VALUE);

    return retval;
  }

  synchronized void purge(int baseSeconds, int windowId, DataIntrospector di)
  {
    long longWindowId = (long)baseSeconds << 32 | windowId;

    DataArray prev = null;
    for (DataArray temp = first; temp != null && temp.starting_window <= longWindowId; temp = temp.next) {
      if (temp.ending_window > longWindowId) {
        if (prev != null) {
          synchronized (BLOCKSIZE) {
            prev.next = free;
            free = first;
            first = temp;
          }
        }

        purge(first, longWindowId, di);
        break;
      }

      prev = temp;
    }
  }

  private static void purge(DataArray da, long longWindowId, DataIntrospector di)
  {
    logger.debug("starting_window = {}, longWindowId = {}, baseSeconds = {}",
                 new Object[] {Long.toHexString(da.starting_window), Long.toHexString(longWindowId), Long.toHexString(da.baseSeconds)});
    da.lockWrite();
    try {
      long bs = (long)da.baseSeconds << 32;
      SerializedData lastReset = null;

      DataListIterator dli = new DataListIterator(da, di);
      done:
      while (dli.hasNext()) {
        SerializedData sd = dli.next();
        switch (di.getType(sd)) {
          case RESET_WINDOW:
            bs = (long)di.getWindowId(sd) << 32;
            lastReset = sd;
            break;

          case BEGIN_WINDOW:
            if ((bs | di.getWindowId(sd)) > longWindowId) {
              if (lastReset != null) {
                /**
                 * Restore the last Reset tuple if there was any.
                 */
                if (sd.offset >= lastReset.size) {
                  sd.offset -= lastReset.size;
                  if (!(sd.bytes == lastReset.bytes && sd.offset == lastReset.offset)) {
                    System.arraycopy(lastReset.bytes, lastReset.offset, sd.bytes, sd.offset, lastReset.size);
                  }
                }

                da.starting_window = bs | di.getWindowId(sd);
              }

              int i = 1;
              while (i < Codec.getSizeOfRawVarint32(sd.offset - i)) {
                i++;
              }
              
              if (i <= sd.size) {
                sd.size = sd.offset;
                sd.offset = 0;
                sd.dataOffset = Codec.writeRawVarint32(sd.size - i, sd.bytes, sd.offset, i);
                di.wipeData(sd);
              }
              break done;
            }
        }
      }
    }
    finally {
      da.unlockWrite();
    }
  }

  class DataArray
  {
    /**
     * Any operation on this data array would need read or write lock here.
     */
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();
    private volatile int baseSeconds = 0;
    private volatile int intervalMillis = 0;
    /**
     * currentOffset is the number of data elements in the array.
     */
    volatile int offset;
    /**
     * The starting window which is available in this data array
     */
    volatile long starting_window;
    /**
     * the ending window which is available in this data array
     */
    volatile long ending_window;
    /**
     * actual data - stored as length followed by actual data.
     */
    volatile byte data[];
    /**
     * the next in the chain.
     */
    DataArray next;

    private DataArray(int capacity)
    {
      /*
       * we want to make sure that MSB of each byte is on, so that we can exploit it ensure presence of a record which is prepended with getSize represented as
       * 32 bit integer varint.
       */
      data = new byte[capacity];
    }

    void getNextData(SerializedData current)
    {
      if (current.offset + 5 < current.bytes.length) {
        r.lock();
        try {
          Codec.readRawVarInt32(current);
        }
        finally {
          r.unlock();
        }
      }
      else {
        current.size = 0;
      }
    }

    public void add(Data d)
    {
      w.lock();

      try {
        int size = d.getSerializedSize();
        if (size + 5 + offset >= data.length) {
          if (offset < data.length) {
            offset = Codec.writeRawVarint32(data.length - offset, data, offset);
            if (offset < data.length) {
              Data.Builder db = Data.newBuilder();
              db.setType(DataType.NO_DATA);
              db.setWindowId(0);
              System.arraycopy(db.build().toByteArray(), 0, data, offset, data.length - offset);
              offset = data.length;
            }
          }

          int newblockSize = BLOCKSIZE;
          while (newblockSize < size + 5) {
            newblockSize += BLOCKSIZE;
          }

          DataList.this.last = next = getDataArray(newblockSize);

          /**
           * Add reset window at the beginning of each new block.
           */
          if (d.getType() != DataType.RESET_WINDOW) {
            Data.Builder db = Data.newBuilder();
            db.setType(DataType.RESET_WINDOW);
            db.setWindowId(baseSeconds);

            ResetWindow.Builder rwb = ResetWindow.newBuilder();
            rwb.setWidth(intervalMillis);
            db.setResetWindow(rwb);
            next.add(db.build());
          }

          next.add(d);
        }
        else {
          offset = Codec.writeRawVarint32(size, data, offset);
          System.arraycopy(d.toByteArray(), 0, data, offset, size);
          offset += size;

          switch (d.getType()) {
            case BEGIN_WINDOW:
              long long_window_id = ((long)baseSeconds << 32 | d.getWindowId());
//            logger.debug("baseSeconds = {}, windowId = {}, long_window_id = {}",
//                         new Object[] {Integer.toHexString(baseSeconds), Integer.toHexString(d.getWindowId()), Long.toHexString(long_window_id)});
              if (starting_window == 0) {
                starting_window = long_window_id;
              }
              ending_window = long_window_id;
              break;

            case RESET_WINDOW:
              baseSeconds = d.getWindowId();
              intervalMillis = d.getResetWindow().getWidth();
              if (starting_window == 0) {
                starting_window = (long)baseSeconds << 32;
              }
              ending_window = (long)baseSeconds << 32;
              break;
          }
        }
      }
      finally {
        w.unlock();
      }
    }

    public void lockWrite()
    {
      w.lock();
    }

    public void unlockWrite()
    {
      w.unlock();
    }

    public void lockRead()
    {
      r.lock();
    }

    public void unlockRead()
    {
      r.unlock();
    }
  }

  public DataList(String identifier, String type, int capacity)
  {
    this.identifier = identifier;
    this.type = type;
    this.capacity = capacity;

    first = last = getDataArray(capacity);
  }

  public DataList(String identifier, String type)
  {
    this(identifier, type, BLOCKSIZE);
  }

  public void add(Data d)
  {
    last.add(d);

    // here somehow we need to let the other thread know that we are ready
    // to write w/o writing all the data since that comes with the danger
    // of getting blocked. May be it's enough for us to write just one byte
    // of data.

    ByteBuffer bytebuffer = null;
    switch (d.getType()) {
      case PARTITIONED_DATA:
        bytebuffer = d.getPartitionedData().getPartition().asReadOnlyByteBuffer();
        if (listeners.containsKey(bytebuffer)) {
          Set<DataListener> interested = listeners.get(bytebuffer);
          for (DataListener dl: interested) {
            dl.dataAdded(bytebuffer);
          }
        }
      /*
       * fall through here since we also want to give data to all the listeners who do not have preference for the partition.
       */
      case SIMPLE_DATA:
        if (listeners.containsKey(DataListener.NULL_PARTITION)) {
          if (bytebuffer == null) {
            bytebuffer = DataListener.NULL_PARTITION;
          }
          Set<DataListener> interested = listeners.get(DataListener.NULL_PARTITION);
          for (DataListener dl: interested) {
            dl.dataAdded(bytebuffer);
          }
        }
        break;

      default:
        for (DataListener dl: all_listeners) {
          dl.dataAdded(DataListener.NULL_PARTITION);
        }
        break;
    }
  }

  /*
   * public void purge(int ending_id) { first.lockWrite(); try { while (first != last && ending_id > first.ending_window) { DataArray temp = first; first =
   * first.next; first.lockWrite(); temp.unlockWrite(); }
   *
   * if (ending_id <= first.ending_window) { int offset_2_delete = 0; while (offset_2_delete < capacity) { Data d = first.data[offset_2_delete++]; if
   * (d.getType() == DataType.END_WINDOW && d.getWindowId() == ending_id) { break; } } first.offset_2_delete = offset_2_delete; first.currentOffset -=
   * offset_2_delete; while (offset_2_delete < capacity) { Data d = first.data[offset_2_delete++]; if (d.getType() == DataType.BEGIN_WINDOW) {
   * first.starting_window = d.getWindowId(); } } } else { first.offset_2_delete = 0; first.currentOffset = 0; first.ending_window = 0; first.starting_window =
   * 0; } } finally { first.unlockWrite(); } }
   *
   */

  /*
   * Iterator related functions.
   */
  private final HashMap<String, DataListIterator> iterators = new HashMap<String, DataListIterator>();

  public Iterator<SerializedData> newIterator(String identifier, DataIntrospector di)
  {
    DataListIterator dli;
    first.lockRead();
    try {
      dli = new DataListIterator(first, di);
      synchronized (iterators) {
        iterators.put(identifier, dli);
      }
    }
    finally {
      first.unlockRead();
    }
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
      DataArray da = dli.da;
      synchronized (iterator) {
        da.lockRead();
        try {
          for (Entry<String, DataListIterator> e: iterators.entrySet()) {
            if (e.getValue() == dli) {
              iterators.remove(e.getKey());
              released = true;
              dli.da = null;
              break;
            }
          }
        }
        finally {
          da.unlockRead();
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

    synchronized (iterators) {
      for (DataListIterator dli: iterators.values()) {
        count++;

        DataArray da = dli.da;
        da.lockRead();
        try {
          dli.da = null;
        }
        finally {
          da.unlockRead();
        }
      }

      iterators.clear();
    }

    return count;
  }

  public void addDataListener(DataListener dl)
  {
    all_listeners.add(dl);
    ArrayList<ByteBuffer> partitions = new ArrayList<ByteBuffer>();
    if (dl.getPartitions(partitions) > 0) {
      for (ByteBuffer partition: partitions) {
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
    ArrayList<ByteBuffer> partitions = new ArrayList<ByteBuffer>();
    if (dl.getPartitions(partitions) > 0) {
      for (ByteBuffer partition: partitions) {
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

  public void printState()
  {
    System.out.println("capacity = " + capacity);
    System.out.println("identifier = " + identifier);
    System.out.println("type = " + type);

    DataArray tmp = first;
    while (tmp != null) {
      System.out.println("offset = " + tmp.offset);
      tmp = tmp.next;
    }

    System.out.println("=====================================================");
  }
}
