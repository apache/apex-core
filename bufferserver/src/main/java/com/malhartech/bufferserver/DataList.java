/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.Data.DataType;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author chetan
 */
public class DataList
{

  private static final Logger logger = LoggerFactory.getLogger(DataList.class.getName());
  HashMap<ByteBuffer, HashSet<DataListener>> listeners = new HashMap<ByteBuffer, HashSet<DataListener>>();
  HashSet<DataListener> all_listeners = new HashSet<DataListener>();
  int capacity;
  String identifier;
  String type;
  DataArray first;
  DataArray last;

  Object getType()
  {
    return this.type;
  }

  class DataArray
  {

    /**
     * Any operation on this data array would need read or write lock here.
     */
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();
    int offset;
    int count;
    long starting_window;
    long ending_window;
    Data data[];
    DataArray next;

    public DataArray(int capacity)
    {
      this.offset = 0;
      this.count = 0;
      this.starting_window = 0;
      this.ending_window = 0;
      data = new Data[capacity];
      next = null;
    }

    public void add(Data d)
    {
      w.lock();

      try {
        data[count++] = d;

        if (d.getType() == Data.DataType.BEGIN_WINDOW) {
          if (starting_window == 0) {
            starting_window = d.getWindowId();
          }
          ending_window = d.getWindowId();
        }
      }
      finally {
        w.unlock();
      }
    }

    public void purge(int count)
    {
      w.lock();

      try {
        this.offset += count;
        this.count -= count;
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

    first = last = new DataArray(capacity);
  }

  public void add(Data d)
  {
    last.lockWrite();
    DataArray temp = last;
    try {
      if (last.offset + last.count == capacity) {
        last.next = new DataArray(capacity);
        last = last.next;
      }
    }
    finally {
      temp.unlockWrite();
    }

    last.add(d);

    // here somehow we need to let the other thread know that we are ready
    // to write w/o writing all the data since that comes with the danger
    // of getting blocked. May be it's enough for us to write just one byte
    // of data.

    // what happens when there are listeners who are not interested in
    // partitioned data?
    ByteBuffer bytebuffer = null;
    switch (d.getType()) {
      case PARTITIONED_DATA:
        bytebuffer = d.getPartitioneddata().getPartition().asReadOnlyByteBuffer();
        if (listeners.containsKey(bytebuffer)) {
          Set<DataListener> interested = listeners.get(bytebuffer);
          for (DataListener dl : interested) {
            dl.dataAdded(bytebuffer);
          }
        }
      /*
       * fall through here since we also want to give data to all the listeners
       * who do not have preference for the partition.
       */
      case SIMPLE_DATA:
        if (listeners.containsKey(DataListener.NULL_PARTITION)) {
          if (bytebuffer == null) {
            bytebuffer = DataListener.NULL_PARTITION;
          }
          Set<DataListener> interested = listeners.get(DataListener.NULL_PARTITION);
          for (DataListener dl : interested) {
            dl.dataAdded(bytebuffer);
          }
        }
        break;


      default:
        for (DataListener dl : all_listeners) {
          dl.dataAdded(DataListener.NULL_PARTITION);
        }
        break;
    }
  }

  public void addPublisherDisconnected(long time)
  {
    Buffer.Data.Builder db = Buffer.Data.newBuilder();
    db.setType(Data.DataType.PUBLISHER_DISCONNECT);
    db.setWindowId(time);

    Buffer.PublisherDisconnect.Builder pdb = Buffer.PublisherDisconnect.newBuilder();
    pdb.setIdentifier(identifier);
    pdb.setType(type);
    pdb.setTime(time);

    db.setDisconnect(pdb.build());
    this.add(db.build());
  }

  public void purge(int ending_id)
  {
    first.lockWrite();
    try {
      while (first != last && ending_id > first.ending_window) {
        DataArray temp = first;
        first = first.next;
        first.lockWrite();
        temp.unlockWrite();
      }

      if (ending_id <= first.ending_window) {
        int offset = 0;
        while (offset < capacity) {
          Data d = first.data[offset++];
          if (d.getType() == DataType.END_WINDOW && d.getWindowId() == ending_id) {
            break;
          }
        }
        first.offset = offset;
        first.count -= offset;
        while (offset < capacity) {
          Data d = first.data[offset++];
          if (d.getType() == DataType.BEGIN_WINDOW) {
            first.starting_window = d.getWindowId();
          }
        }
      }
      else {
        first.offset = 0;
        first.count = 0;
        first.ending_window = 0;
        first.starting_window = 0;
      }
    }
    finally {
      first.unlockWrite();
    }
  }

  /*
   * Iterator related functions.
   */
  private final HashMap<String, DataListIterator> iterators = new HashMap<String, DataListIterator>();

  public Iterator<Data> newIterator(String identifier)
  {
    DataListIterator di;
    first.lockRead();
    try {
      di = new DataListIterator(first);
      synchronized (iterators) {
        iterators.put(identifier, di);
      }
    }
    finally {
      first.unlockRead();
    }
    return di;
  }

  /**
   * Release previous acquired iterator from this DataList
   *
   * @param iterator
   * @return true if successfully released, false otherwise.
   */
  public boolean delIterator(Iterator<Data> iterator)
  {
    boolean released = false;
    if (iterator instanceof DataListIterator) {
      DataListIterator dli = (DataListIterator) iterator;
      DataArray da = dli.da;
      synchronized (iterator) {
        da.lockRead();
        try {
          for (Entry<String, DataListIterator> e : iterators.entrySet()) {
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
   * @return the count of iterators
   */
  public int clearIterators()
  {
    int count = 0;

    synchronized (iterators) {
      for (DataListIterator dli : iterators.values()) {
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
      for (ByteBuffer partition : partitions) {
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
      for (ByteBuffer partition : partitions) {
        if (listeners.containsKey(partition)) {
          listeners.get(partition).remove(dl);
        }
      }
    } else {
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
      System.out.println("offset = " + tmp.offset + " count = " + tmp.count);
      tmp = tmp.next;
    }

    System.out.println("=====================================================");
  }
}
