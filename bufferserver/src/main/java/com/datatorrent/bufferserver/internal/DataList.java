/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.internal;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.packet.BeginWindowTuple;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.packet.ResetWindowTuple;
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
  private final int MAX_COUNT_OF_INMEM_BLOCKS;
  protected final String identifier;
  private final Integer blocksize;
  private HashMap<BitVector, HashSet<DataListener>> listeners = new HashMap<BitVector, HashSet<DataListener>>();
  protected HashSet<DataListener> all_listeners = new HashSet<DataListener>();
  protected Block first;
  protected Block last;
  protected Storage storage;
  protected ExecutorService autoflushExecutor;
  protected ExecutorService storageExecutor;

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

  public void setAutoflushExecutor(final ExecutorService es)
  {
    autoflushExecutor = es;
  }

  public void setSecondaryStorage(Storage storage, ExecutorService es)
  {
    this.storage = storage;
    storageExecutor = es;
  }

  /*
   * Iterator related functions.
   */
  protected final HashMap<String, DataListIterator> iterators = new HashMap<String, DataListIterator>();

  public DataListIterator getIterator(Block block)
  {
    return new DataListIterator(block);
  }

  public Iterator<SerializedData> newIterator(String identifier, long windowId)
  {
    //logger.debug("request for a new iterator {} and {}", identifier, windowId);
    for (Block temp = first; temp != null; temp = temp.next) {
      if (temp.starting_window >= windowId || temp.ending_window > windowId) {
        DataListIterator dli = getIterator(temp);
        iterators.put(identifier, dli);
        //logger.debug("returning new iterator on temp = {}", temp);
        return dli;
      }
    }

    DataListIterator dli = getIterator(last);
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
          break;
        }
      }
    }
    return released;
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
          temp.release(false);
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
        VarInt.read(current);
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
      DataListIterator dli = getIterator(this);
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

      if (uniqueIdentifier != 0) {
        storage.discard(identifier, uniqueIdentifier);
        uniqueIdentifier = 0;
      }

      return bs;
    }

    public void purge(long longWindowId)
    {
//    logger.debug("starting_window = {}, longWindowId = {}, ending_window = {}",
//                 new Object[] {VarInt.getStringWindowId(starting_window), VarInt.getStringWindowId(longWindowId), VarInt.getStringWindowId(ending_window)});
      boolean found = false;
      long bs = starting_window & 0xffffffff00000000L;
      SerializedData lastReset = null;

      DataListIterator dli = getIterator(this);
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

        if (uniqueIdentifier != 0) {
          storage.discard(identifier, uniqueIdentifier);
          uniqueIdentifier = 0;
        }
      }
    }

    private Runnable getRetriever(final int uniqueIdentifier, final Storage storage)
    {
      return new Runnable()
      {
        @Override
        public void run()
        {
          byte[] lData = storage.retrieve(identifier, uniqueIdentifier);
          synchronized (Block.this) {
            data = lData;
            readingOffset = 0;
            writingOffset = data.length;
            if (refCount > 1) {
              Block.this.notifyAll();
            }
          }
        }

      };
    }

    synchronized void acquire(boolean wait)
    {
      if (refCount++ == 0 && uniqueIdentifier > 0 && storage != null) {
        assert (data == null);
        if (wait) {
          getRetriever(uniqueIdentifier, storage).run();
        }
        else {
          storageExecutor.submit(getRetriever(uniqueIdentifier, storage));
        }
      }
      else if (wait && data == null) {
        try {
          wait();
        }
        catch (InterruptedException ex) {
          throw new RuntimeException("Interrupted while waiting for data to be loaded!", ex);
        }
      }
    }

    private Runnable getStorer(final byte[] data, final int readingOffset, final int writingOffset, final Storage storage)
    {
      return new Runnable()
      {
        @Override
        public void run()
        {
          int i = storage.store(identifier, data, readingOffset, writingOffset);
          if (i == 0) {
            logger.warn("Storage returned unexpectedly, please check the status of the spool directory!");
          }
          else {
            synchronized (Block.this) {
              Block.this.uniqueIdentifier = i;
              if (--refCount == 0) {
                Block.this.data = null;
              }
            }
          }
        }

      };
    }

    synchronized void release(boolean wait)
    {
      if (refCount == 1 && storage != null && uniqueIdentifier == 0) {
        if (wait) {
          getStorer(data, readingOffset, writingOffset, storage).run();
        }
        else {
          storageExecutor.submit(getStorer(data, readingOffset, writingOffset, storage));
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
             + '}';
    }

  }

  /**
   * <p>DataListIterator class.</p>
   *
   * @author chetan
   * @since 0.3.2
   */
  public class DataListIterator implements Iterator<SerializedData>
  {
    Block da;
    SerializedData current;
    protected byte[] buffer;
    protected int readOffset;
    MutableInt nextOffset = new MutableInt();
    int size;

    /**
     *
     * @param da
     */
    DataListIterator(Block da)
    {
      da.acquire(true);
      this.da = da;
      buffer = da.data;
      readOffset = da.readingOffset;
    }

    // this is a hack! Get rid of it.
    public int getBaseSeconds()
    {
      return da == null ? 0 : (int)(da.starting_window >> 32);
    }

    public int getReadOffset()
    {
      return readOffset;
    }

    /**
     *
     * @return boolean
     */
    @Override
    public synchronized boolean hasNext()
    {
      while (size == 0) {
        size = VarInt.read(buffer, readOffset, da.writingOffset, nextOffset);
        switch (nextOffset.integer) {
          case -5:
            throw new RuntimeException("problemo!");

          case -4:
          case -3:
          case -2:
          case -1:
          case 0:
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
        if (nextOffset.integer + size <= da.writingOffset) {
          current = new SerializedData(buffer, readOffset, size + nextOffset.integer - readOffset);
          current.dataOffset = nextOffset.integer;
          //if (buffer[current.dataOffset] == MessageType.BEGIN_WINDOW_VALUE || buffer[current.dataOffset] == MessageType.END_WINDOW_VALUE) {
          //  Tuple t = Tuple.getTuple(current.bytes, current.dataOffset, current.size - current.dataOffset + current.offset);
          //  logger.debug("next t = {}", t);
          //}
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

    /**
     *
     * @return {@link com.datatorrent.bufferserver.util.SerializedData}
     */
    @Override
    public SerializedData next()
    {
      readOffset = current.offset + current.size;
      size = 0;
      return current;
    }

    /**
     * Removes from the underlying collection the last element returned by the iterator (optional operation). This method can be called only once per call to
     * next. The behavior of an iterator is unspecified if the underlying collection is modified while the iteration is in progress in any way other than by
     * calling this method.
     */
    @Override
    public void remove()
    {
      current.bytes[current.dataOffset] = MessageType.NO_MESSAGE_VALUE;
    }

    void rewind(int processingOffset)
    {
      readOffset = processingOffset;
      size = 0;
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(DataList.class);
}
