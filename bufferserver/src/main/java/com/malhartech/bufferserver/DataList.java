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
import com.malhartech.bufferserver.util.Tuple;
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
  private final String identifier;
  private final Integer capacity;
  private HashMap<Integer, HashSet<DataListener>> listeners = new HashMap<Integer, HashSet<DataListener>>();
  private HashSet<DataListener> all_listeners = new HashSet<DataListener>();
  private static volatile DataArray free = null;
  private volatile DataArray first;
  private volatile DataArray last;

  synchronized void rewind(int baseSeconds, int windowId, DataIntrospector di)
  {
    long longWindowId = (long)baseSeconds << 32 | windowId;

    for (DataArray temp = first; temp != null; temp = temp.next) {
      if (temp.starting_window >= longWindowId || temp.ending_window > longWindowId) {
        if (temp != last) {
          synchronized (this.capacity) {
            last.next = free;
            free = temp.next;
          }
          temp.next = null;
          last = temp;
        }

        long bs = temp.starting_window & 0x7fffffff00000000L;
        temp.lockWrite();
        try {
          DataListIterator dli = new DataListIterator(last, di);
          done:
          while (dli.hasNext()) {
            SerializedData sd = dli.next();
            switch (di.getType(sd)) {
              case RESET_WINDOW:
                bs = (long)di.getWindowId(sd) << 32;
                if (bs > longWindowId) {
                  temp.writingOffset = sd.offset;
                  Arrays.fill(temp.data, temp.writingOffset, temp.data.length, Byte.MIN_VALUE);
                  break done;
                }
                break;

              case BEGIN_WINDOW:
                if ((bs | di.getWindowId(sd)) >= longWindowId) {
                  temp.writingOffset = sd.offset;
                  Arrays.fill(temp.data, temp.writingOffset, temp.data.length, Byte.MIN_VALUE);
                  break done;
                }
                break;
            }
          }

        }
        finally {
          temp.unlockWrite();
        }
      }
    }

    // passing the baseSeconds == 0 is a crime, so the following is a hack.
    if (baseSeconds != 0) {
      last.add(Tuple.getResetTuple(baseSeconds, windowId)); // passing windowId is a hack here!!! I should be passing the windowWidth.
    }
  }

  /**
   * @param capacity - it's ignored if we are reusing block
   * @return DataArray
   */
  private DataArray getDataArray(int capacity)
  {
    DataArray retval = null;

    synchronized (this.capacity) {
      if (free != null) {
        retval = free;
        free = free.next;
      }
    }

    if (retval == null) {
      retval = new DataArray(capacity);
    }
    else {
      retval.starting_window = retval.ending_window = retval.writingOffset = retval.readingOffset = 0;
      retval.next = null;
    }

    Arrays.fill(retval.data, Byte.MIN_VALUE);

    return retval;
  }

  synchronized void reset()
  {
    listeners.clear();
    all_listeners.clear();

    synchronized (capacity) {
      last.next = free;
      free = first;
    }

    first = last = getDataArray(capacity);
  }

  synchronized void purge(int baseSeconds, int windowId, DataIntrospector di)
  {
    long longWindowId = (long)baseSeconds << 32 | windowId;

    DataArray prev = null;
    for (DataArray temp = first; temp != null && temp.starting_window <= longWindowId; temp = temp.next) {
      if (temp.ending_window > longWindowId || temp == last) {
        if (prev != null) {
          synchronized (capacity) {
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
                 new Object[] {Codec.getStringWindowId(da.starting_window), Codec.getStringWindowId(longWindowId), da.baseSeconds});
    da.lockWrite();
    try {
      boolean found = false;
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

                da.starting_window = bs | di.getWindowId(sd);
                da.readingOffset = sd.offset;
              }

              // the following code through done may not even be needed. why waste cycles.
              int i = 1;
              while (i < Codec.getSizeOfRawVarint32(sd.offset - i)) {
                i++;
              }

              if (i <= sd.offset) {
                sd.size = sd.offset;
                sd.offset = 0;
                sd.dataOffset = Codec.writeRawVarint32(sd.size - i, sd.bytes, sd.offset, i);
                di.wipeData(sd);
              }
              else {
                logger.warn("Unhandled condition while purging the data purge to offset {}", sd.offset);
              }
              break done;
            }
        }
      }

      /**
       * If we ended up purging all the data from the current DataArray then,
       * it also makes sense to start all over.
       * It helps with better utilization of the RAM.
       */
      if (!found) {
        logger.debug("we could not find a tuple which is in a window later than the window to be purged, so this has to be the last window published so far");
        if (lastReset != null && lastReset.offset != 0) {
          da.readingOffset = da.writingOffset - lastReset.size;
          System.arraycopy(lastReset.bytes, lastReset.offset, da.data, da.readingOffset, lastReset.size);
          da.starting_window = da.ending_window = bs;
        }
        else {
          da.readingOffset = da.writingOffset;
          da.starting_window = da.ending_window = 0;
        }


        SerializedData sd = new SerializedData();
        sd.bytes = da.data;
        sd.offset = da.readingOffset;

        // the rest of it is just a copy from beginWindow case here to wipe the data - refactor
        int i = 1;
        while (i < Codec.getSizeOfRawVarint32(sd.offset - i)) {
          i++;
        }

        if (i <= sd.offset) {
          sd.size = sd.offset;
          sd.offset = 0;
          sd.dataOffset = Codec.writeRawVarint32(sd.size - i, sd.bytes, sd.offset, i);
          di.wipeData(sd);
        }
        else {
          logger.warn("Unhandled condition while purging the data purge to offset {}", sd.offset);
        }
      }
    }
    finally {
      da.unlockWrite();
    }
  }

  /**
   * @return the identifier
   */
  public String getIdentifier()
  {
    return identifier;
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
    volatile int readingOffset;
    /**
     * currentOffset is the number of data elements in the array.
     */
    volatile int writingOffset;
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
    byte data[];
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
      if (current.offset < data.length) {
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

    public final void addUnsafe(Data d)
    {
      int size = d.getSerializedSize();
      if (writingOffset + 5 /* for max varint size */ + size > data.length
              && /* this is a fast check */ writingOffset + Codec.getSizeOfRawVarint32(size) + size > data.length) {
        int i = 1;
        while (i < Codec.getSizeOfRawVarint32(data.length - writingOffset - i)) {
          i++;
        }

        if (i + writingOffset <= data.length) {
          writingOffset = Codec.writeRawVarint32(data.length - writingOffset - i, data, writingOffset, i);
          if (writingOffset < data.length) {
            Data.Builder db = Data.newBuilder();
            db.setType(DataType.NO_DATA);
            db.setWindowId(0);

            Data noData = db.build();
            int writeSize = data.length - writingOffset;
            if (writeSize > noData.getSerializedSize()) {
              writeSize = noData.getSerializedSize();
            }
            System.arraycopy(db.build().toByteArray(), 0, data, writingOffset, writeSize);
            writingOffset = data.length;
          }
        }

        int newblockSize = capacity;
        while (newblockSize < size + 5) {
          newblockSize += capacity;
        }

        DataList.this.last = next = getDataArray(newblockSize);
        logger.debug("added a new data array {}", next);

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
        writingOffset = Codec.writeRawVarint32(size, data, writingOffset);
        System.arraycopy(d.toByteArray(), 0, data, writingOffset, size);
        writingOffset += size;

        switch (d.getType()) {
          case BEGIN_WINDOW:
            long long_window_id = ((long)baseSeconds << 32 | d.getWindowId());
//            logger.debug("baseSeconds = {}, windowId = {}, long_window_id = {}",
//                         new Object[] {Integer.toHexString(baseSeconds), Integer.toHexString(d.getWindowId()), Codec.getStringWindowId(long_window_id)});
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

    public void add(Data d)
    {
      w.lock();
      try {
        addUnsafe(d);
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

  public DataList(String identifier, int capacity)
  {
    this.identifier = identifier;
    this.capacity = capacity;
    first = last = getDataArray(capacity);
  }

  public DataList(String identifier)
  {
    /*
     * We use 64MB (the default HDFS block getSize) as the getSize of the memory pool so we can flush the data 1 block at a time to the filesystem.
     */
    this(identifier, 64 * 1024 * 1024);
  }

  public final void flush()
  {
    for (DataListener dl: all_listeners) {
      dl.dataAdded(DataListener.NULL_PARTITION);
    }
  }

  public final void add(Data d)
  {
    last.add(d);

    // here somehow we need to let the other thread know that we are ready
    // to write w/o writing all the data since that comes with the danger
    // of getting blocked. May be it's enough for us to write just one byte
    // of data.
    // netty 4alpha5 provides it.

//    ByteBuffer bytebuffer = null;
//    switch (d.getType()) {
//      case PARTITIONED_DATA:
//        bytebuffer = d.getPartitionedData().getPartition().asReadOnlyByteBuffer();
//        if (listeners.containsKey(bytebuffer)) {
//          Set<DataListener> interested = listeners.get(bytebuffer);
//          for (DataListener dl: interested) {
//            dl.dataAdded(bytebuffer);
//          }
//        }
//      /*
//       * fall through here since we also want to give data to all the listeners who do not have preference for the partition.
//       */
//      case SIMPLE_DATA:
//        if (listeners.containsKey(DataListener.NULL_PARTITION)) {
//          if (bytebuffer == null) {
//            bytebuffer = DataListener.NULL_PARTITION;
//          }
//          Set<DataListener> interested = listeners.get(DataListener.NULL_PARTITION);
//          for (DataListener dl: interested) {
//            dl.dataAdded(bytebuffer);
//          }
//        }
//        break;
//
//      default:
//        for (DataListener dl: all_listeners) {
//          dl.dataAdded(DataListener.NULL_PARTITION);
//        }
//        break;
//    }
  }

  /*
   * Iterator related functions.
   */
  private final HashMap<String, DataListIterator> iterators = new HashMap<String, DataListIterator>();

  public Iterator<SerializedData> newIterator(String identifier, DataIntrospector di, long windowId)
  {
    for (DataArray temp = first; temp != null; temp = temp.next) {
      if (true || temp.starting_window >= windowId || temp.ending_window > windowId) { // for now always send the first
        temp.lockWrite();
        try {
          DataListIterator dli = new DataListIterator(temp, di);
          synchronized (iterators) {
            iterators.put(identifier, dli);
          }
          return dli;
        }
        finally {
          temp.unlockWrite();
        }
      }
    }

    last.lockRead();
    try {
      DataListIterator dli = new DataListIterator(last, di);
      synchronized (iterators) {
        iterators.put(identifier, dli);
      }
      return dli;
    }
    finally {
      last.unlockRead();
    }
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
    ArrayList<Integer> partitions = new ArrayList<Integer>();
    if (dl.getPartitions(partitions) > 0) {
      for (Integer partition: partitions) {
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
    ArrayList<Integer> partitions = new ArrayList<Integer>();
    if (dl.getPartitions(partitions) > 0) {
      for (Integer partition: partitions) {
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
    System.out.println("identifier = " + getIdentifier());
    DataArray tmp = first;
    while (tmp != null) {
      System.out.println("writing offset = " + tmp.writingOffset);
      tmp = tmp.next;
    }

    System.out.println("=====================================================");
  }
}
