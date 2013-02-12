/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.google.protobuf.CodedOutputStream;
import com.malhartech.bufferserver.Buffer.Message;
import com.malhartech.bufferserver.Buffer.Message.MessageType;
import com.malhartech.bufferserver.Buffer.ResetWindow;
import com.malhartech.bufferserver.storage.Storage;
import com.malhartech.bufferserver.util.BitVector;
import com.malhartech.bufferserver.util.Codec;
import com.malhartech.bufferserver.util.SerializedData;
import com.malhartech.bufferserver.util.TupleFactory;
import java.io.ByteArrayOutputStream;
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
  private volatile DataArray first;
  private volatile DataArray last;
  private Storage storage;

  synchronized void rewind(int baseSeconds, int windowId, DataIntrospector di) throws IOException
  {
    long longWindowId = (long)baseSeconds << 32 | windowId;

    for (DataArray temp = first; temp != null; temp = temp.next) {
      if (temp.starting_window >= longWindowId || temp.ending_window > longWindowId) {
        if (temp != last) {
          temp.next = null;
          last = temp;
        }

        temp.rewind(di, longWindowId);
      }
    }

    // passing the baseSeconds == 0 is a crime, so the following is a hack.
    if (baseSeconds != 0) {
      last.add(TupleFactory.getResetTuple(baseSeconds, windowId)); // passing windowId is a hack here!!! I should be passing the windowWidth.
    }
  }

  synchronized void reset()
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

    DataArray temp = new DataArray(blocksize);
    temp.acquire(true);
    first = last = temp;
  }

  synchronized void purge(int baseSeconds, int windowId, DataIntrospector di)
  {
    long longWindowId = (long)baseSeconds << 32 | windowId;

    DataArray prev = null;
    for (DataArray temp = first; temp != null && temp.starting_window <= longWindowId; temp = temp.next) {
      if (temp.ending_window > longWindowId || temp == last) {
        if (prev != null) {
          first = temp;
        }

        first.purge(longWindowId, di);
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

  class DataArray
  {
    private transient ByteArrayOutputStream output;
    /**
     * Any operation on this data array would need read or write lock here.
     */
    private volatile int baseSeconds = 0;
    private volatile int intervalMillis = 0;
    /**
     * readingOffset is the first valid byte in the array.
     */
    volatile int readingOffset;
    /**
     * writingOffset is the number of data elements in the array.
     */
    volatile int writingOffset;
    /**
     * The starting window which is available in this data array.
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
     * when the data is null, uniqueIdentifier is the identifier in the backup storage to retrieve the object.
     */
    private int uniqueIdentifier;
    /**
     * the next in the chain.
     */
    DataArray next;
    private int refcount;

    private DataArray(int blocksize)
    {
      /*
       * we want to make sure that MSB of each byte is on, so that we can exploit it ensure presence of a record which is prepended with getSize represented as
       * 32 bit integer varint.
       */
      Arrays.fill(data = new byte[blocksize], Byte.MIN_VALUE);
    }

    void getNextData(SerializedData current)
    {
      if (current.offset < writingOffset) {
        Codec.readRawVarInt32(current);
      }
      else {
        current.size = 0;
      }
    }

    private synchronized void rewind(DataIntrospector di, long windowId)
    {
      long bs = starting_window & 0x7fffffff00000000L;
      DataListIterator dli = new DataListIterator(last, di);
      done:
      while (dli.hasNext()) {
        SerializedData sd = dli.next();
        switch (di.getType(sd)) {
          case RESET_WINDOW:
            bs = (long)di.getBaseSeconds(sd) << 32;
            if (bs > windowId) {
              writingOffset = sd.offset;
              Arrays.fill(data, writingOffset, data.length, Byte.MIN_VALUE);
              break done;
            }
            break;

          case BEGIN_WINDOW:
            if ((bs | di.getWindowId(sd)) >= windowId) {
              writingOffset = sd.offset;
              Arrays.fill(data, writingOffset, data.length, Byte.MIN_VALUE);
              break done;
            }
            break;
        }
      }
    }

    private synchronized void purge(long longWindowId, DataIntrospector di)
    {
      logger.debug("starting_window = {}, longWindowId = {}, baseSeconds = {}", new Object[] {Codec.getStringWindowId(this.starting_window), Codec.getStringWindowId(longWindowId), this.baseSeconds});
      boolean found = false;
      long bs = (long)this.baseSeconds << 32;
      SerializedData lastReset = null;

      DataListIterator dli = new DataListIterator(this, di);
      done:
      while (dli.hasNext()) {
        SerializedData sd = dli.next();
        switch (di.getType(sd)) {
          case RESET_WINDOW:
            bs = (long)di.getBaseSeconds(sd) << 32;
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

                this.starting_window = bs | di.getWindowId(sd);
                this.readingOffset = sd.offset;
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
          this.readingOffset = this.writingOffset - lastReset.size;
          System.arraycopy(lastReset.bytes, lastReset.offset, this.data, this.readingOffset, lastReset.size);
          this.starting_window = this.ending_window = bs;
        }
        else {
          this.readingOffset = this.writingOffset;
          this.starting_window = this.ending_window = 0;
        }


        SerializedData sd = new SerializedData();
        sd.bytes = this.data;
        sd.offset = this.readingOffset;

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

    public final void add(Message d) throws IOException
    {
      int size = d.getSerializedSize();
      if (writingOffset + 5 /* for max varint size */ + size > data.length /* this is a fast check */
              && writingOffset + Codec.getSizeOfRawVarint32(size) + size > data.length /* this is a slower check */) {
        /*
         * The remaining space in the current data array is not sufficient to save the message.
         * We mark the remaining space unusable and allocate a new byte array to store the data.
         */
        int i = 1;
        while (i < Codec.getSizeOfRawVarint32(data.length - writingOffset - i)) {
          i++;
        }

        if (i + writingOffset <= data.length) {
          writingOffset = Codec.writeRawVarint32(data.length - writingOffset - i, data, writingOffset, i);
          ProtobufDataInspector.wipeData(data, writingOffset, data.length - writingOffset);
          writingOffset = data.length;
        }

        /*
         * Add reset window at the beginning of each new block.
         */
        Message reset = null;
        if (d.getType() != MessageType.RESET_WINDOW) {
          Message.Builder db = Message.newBuilder();
          db.setType(MessageType.RESET_WINDOW);

          ResetWindow.Builder rwb = ResetWindow.newBuilder();
          rwb.setBaseSeconds(baseSeconds);
          rwb.setWidth(intervalMillis);
          db.setResetWindow(rwb);
          reset = db.build();
          size += reset.getSerializedSize() + 5; /* roughly for storing the reset tuple and its size */
        }

        /*
         * make sure that the new block that we try to allocate is enough to store the current message.
         */
        int newblockSize = blocksize;
        while (newblockSize < size + 5) {
          newblockSize += blocksize;
        }

        /*
         * Back up the current block, possibly saving some RAM
         */
        release(false);

        DataArray temp = new DataArray(newblockSize);
        temp.acquire(true);
        last = next = temp;
        //logger.debug("added a new data array {}", next);

        /*
         * recursively invoke add on the newly allocated DataArray.
         */
        if (reset != null) {
          next.add(reset);
        }
        next.add(d);
      }
      else {
        writingOffset = Codec.writeRawVarint32(size, data, writingOffset);
        d.writeTo(CodedOutputStream.newInstance(data, writingOffset, size));
        writingOffset += size;

        switch (d.getType()) {
          case BEGIN_WINDOW:
            long long_window_id = ((long)baseSeconds << 32 | d.getBeginWindow().getWindowId());
//            logger.debug("baseSeconds = {}, windowId = {}, long_window_id = {}",
//                         new Object[] {Integer.toHexString(baseSeconds), Integer.toHexString(d.getWindowId()), Codec.getStringWindowId(long_window_id)});
            if (starting_window == 0) {
              starting_window = long_window_id;
            }
            ending_window = long_window_id;
            break;

          case RESET_WINDOW:
            baseSeconds = d.getResetWindow().getBaseSeconds();
            intervalMillis = d.getResetWindow().getWidth();
            if (starting_window == 0) {
              starting_window = (long)baseSeconds << 32;
            }
            ending_window = (long)baseSeconds << 32;
            break;
        }
      }
    }

    synchronized void acquire(boolean wait)
    {
      refcount++;

      if (data == null && storage != null) {
        Runnable r = new Runnable()
        {
          @Override
          public void run()
          {
            synchronized (DataArray.this) {
                        logger.debug("retrieving {} {} in acquire", identifier, first.uniqueIdentifier);

              data = storage.retrieve(identifier, uniqueIdentifier);
              readingOffset = 0;
              writingOffset = data.length;
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

    synchronized void release(boolean wait)
    {
      if (--refcount == 0 && storage != null) {
        Runnable r = new Runnable()
        {
          @Override
          public void run()
          {
            try {
              synchronized (DataArray.this) {

                int i = storage.store(identifier, uniqueIdentifier, data, readingOffset, writingOffset);
                if (i == 0) {
                  logger.warn("Storage returned unexpectedly, please check the status of the spool directory!");
                }
                else {
                          logger.debug("stored {} {} in release", identifier, i);
                  uniqueIdentifier = i;
                  data = null;
                }
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

  }

  public DataList(String identifier, int blocksize, int maxlivebocks)
  {
    this.identifier = identifier;
    this.blocksize = blocksize;

    DataArray temp = new DataArray(blocksize);
    temp.acquire(true);
    first = last = temp;
  }

  public DataList(String identifier)
  {
    /*
     * We use 64MB (the default HDFS block getSize) as the getSize of the memory pool so we can flush the data 1 block at a time to the filesystem.
     */
    this(identifier, 64 * 1024 * 1024, 8);
  }

  public final synchronized void flush()
  {
    for (DataListener dl: all_listeners) {
      dl.dataAdded();
    }
  }

  public void setSecondaryStorage(Storage storage)
  {
    this.storage = storage;
  }

  public final void add(Message d) throws IOException
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
        DataListIterator dli = new DataListIterator(temp, di);
        synchronized (iterators) {
          iterators.put(identifier, dli);
        }
        return dli;
      }
    }

    DataListIterator dli = new DataListIterator(last, di);
    synchronized (iterators) {
      iterators.put(identifier, dli);
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
      synchronized (iterator) {
        for (Entry<String, DataListIterator> e: iterators.entrySet()) {
          if (e.getValue() == dli) {
            iterators.remove(e.getKey());
            released = true;
            dli.da.release(false);
            dli.da = null;
            break;
          }
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
        dli.da.release(false);
        dli.da = null;
      }

      iterators.clear();
    }

    return count;
  }

  public synchronized void addDataListener(DataListener dl)
  {
    all_listeners.add(dl);
    ArrayList<BitVector> partitions = new ArrayList<BitVector>();
    if (dl.getPartitions(partitions) > 0) {
      for (BitVector partition: partitions) {
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

  public synchronized void removeDataListener(DataListener dl)
  {
    ArrayList<BitVector> partitions = new ArrayList<BitVector>();
    if (dl.getPartitions(partitions) > 0) {
      for (BitVector partition: partitions) {
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

}
