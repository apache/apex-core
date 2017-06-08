/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.bufferserver.internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.common.util.ToStringStyle;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.datatorrent.bufferserver.packet.BeginWindowTuple;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.packet.ResetWindowTuple;
import com.datatorrent.bufferserver.packet.Tuple;
import com.datatorrent.bufferserver.storage.Storage;
import com.datatorrent.bufferserver.util.BitVector;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.bufferserver.util.SerializedData;
import com.datatorrent.bufferserver.util.VarInt;
import com.datatorrent.netlet.AbstractClient;
import com.datatorrent.netlet.util.VarInt.MutableInt;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

/**
 * Maintains list of data and manages addition and deletion of the data<p>
 * <br>
 *
 * @since 0.3.2
 */
public class DataList
{
  private static final Logger logger = LoggerFactory.getLogger(DataList.class);

  private final int MAX_COUNT_OF_INMEM_BLOCKS;
  protected final String identifier;
  private final int blockSize;
  private final HashMap<BitVector, HashSet<DataListener>> listeners = newHashMap();
  protected final HashSet<DataListener> all_listeners = newHashSet();
  protected Block first;
  protected Block last;
  protected Storage storage;
  protected ExecutorService autoFlushExecutor;
  protected ExecutorService storageExecutor;
  protected int size;
  protected int processingOffset;
  protected long baseSeconds;
  private final Set<AbstractClient> suspendedClients = newHashSet();
  private final AtomicInteger numberOfInMemBlockPermits;
  private MutableInt nextOffset = new MutableInt();
  private final ListenersNotifier listenersNotifier = new ListenersNotifier();
  private final boolean backPressureEnabled;

  public DataList(final String identifier, final int blockSize, final int numberOfCacheBlocks, final boolean backPressureEnabled)
  {
    if (numberOfCacheBlocks < 1) {
      throw new IllegalArgumentException("Invalid number of Data List Memory blocks " + numberOfCacheBlocks);
    }
    this.MAX_COUNT_OF_INMEM_BLOCKS = numberOfCacheBlocks;
    numberOfInMemBlockPermits = new AtomicInteger(MAX_COUNT_OF_INMEM_BLOCKS - 1);
    this.identifier = identifier;
    this.blockSize = blockSize;
    this.backPressureEnabled = backPressureEnabled;
    first = last = new Block(identifier, blockSize);
  }

  public DataList(String identifier)
  {
    /*
     * We use 64MB (the default HDFS block getSize) as the getSize of the memory pool so we can flush the data 1 block
     * at a time to the filesystem. We will use default value of 8 block sizes to be cached in memory
     */
    this(identifier, 64 * 1024 * 1024, 8, true);
  }

  public int getBlockSize()
  {
    return blockSize;
  }

  public void rewind(final int baseSeconds, final int windowId) throws IOException
  {
    final long longWindowId = (long)baseSeconds << 32 | windowId;
    logger.debug("Rewinding {} from window ID {} to window ID {}", this, Codec.getStringWindowId(last.ending_window),
        Codec.getStringWindowId(longWindowId));

    int numberOfInMemBlockRewound = 0;
    synchronized (this) {
      for (Block temp = first; temp != null; temp = temp.next) {
        if (temp.starting_window >= longWindowId || temp.ending_window > longWindowId) {
          if (temp != last) {
            last.refCount.decrementAndGet();
            last = temp;
            do {
              temp = temp.next;
              temp.discard(false);
              synchronized (temp) {
                if (temp.refCount.get() != 0) {
                  logger.debug("Discarded block {} has positive reference count. Listeners: {}", temp, all_listeners);
                  throw new IllegalStateException("Discarded block " + temp + " has positive reference count!");
                }
                if (temp.data != null) {
                  temp.data = null;
                  numberOfInMemBlockRewound++;
                }
              }
            } while (temp.next != null);
            last.next = null;
            last.acquire(true);
          }
          this.baseSeconds = last.rewind(longWindowId);
          processingOffset = last.writingOffset;
          size = 0;
          break;
        }
      }
    }

    /*
     * TODO: properly rewind Data List iterators, especially handle case when iterators point to blocks past the last
     *  block.
     */

    final int numberOfInMemBlockPermits = this.numberOfInMemBlockPermits.addAndGet(numberOfInMemBlockRewound);
    assert numberOfInMemBlockPermits < MAX_COUNT_OF_INMEM_BLOCKS : "Number of in memory block permits " +
        numberOfInMemBlockPermits + " exceeded configured maximum " + MAX_COUNT_OF_INMEM_BLOCKS + '.';
    resumeSuspendedClients(numberOfInMemBlockPermits);
    logger.debug("Discarded {} in memory blocks during rewind. Number of in memory blocks permits {} after" +
        " rewinding {}.", numberOfInMemBlockRewound, numberOfInMemBlockPermits, this);

  }

  public void reset()
  {
    logger.debug("Resetting {}", this);
    listeners.clear();
    all_listeners.clear();

    synchronized (this) {
      if (storage != null) {
        Block temp = first;
        while (temp != last) {
          temp.discard(false);
          synchronized (temp) {
            if (temp.refCount.get() != 0) {
              throw new IllegalStateException("Discarded block " + temp + " not zero reference count!");
            }
            temp.data = null;
            temp = temp.next;
          }
        }
      }
      first = last;
      first.prev = null;
    }
    numberOfInMemBlockPermits.set(MAX_COUNT_OF_INMEM_BLOCKS - 1);
  }

  public void purge(final long windowId)
  {
    logger.debug("Purging {} from window ID {} to window ID {}", this, Codec.getStringWindowId(first.starting_window),
        Codec.getStringWindowId(windowId));

    int numberOfInMemBlockPurged = 0;
    synchronized (this) {
      for (Block prev = null, temp = first; temp != null && temp.starting_window <= windowId;
          prev = temp, temp = temp.next) {
        if (temp.ending_window > windowId || temp == last) {
          if (prev != null) {
            first = temp;
            first.prev = null;
          }
          first.purge(windowId);
          break;
        }
        temp.discard(false);
        synchronized (temp) {
          if (temp.refCount.get() != 0) {
            logger.debug("Discarded block {} has positive reference count. Listeners: {}", temp, all_listeners);
            throw new IllegalStateException("Discarded block " + temp + " has positive reference count!");
          }
          if (temp.data != null) {
            temp.data = null;
            numberOfInMemBlockPurged++;
          }
        }
      }
    }

    final int numberOfInMemBlockPermits = this.numberOfInMemBlockPermits.addAndGet(numberOfInMemBlockPurged);
    assert numberOfInMemBlockPermits < MAX_COUNT_OF_INMEM_BLOCKS : "Number of in memory block permits " +
        numberOfInMemBlockPermits + " exceeded configured maximum " + MAX_COUNT_OF_INMEM_BLOCKS + '.';
    resumeSuspendedClients(numberOfInMemBlockPermits);
    logger.debug("Discarded {} in memory blocks during purge. Number of in memory blocks permits {} after purging {}. ",
        numberOfInMemBlockPurged, numberOfInMemBlockPermits, this);

  }

  /**
   * @return the identifier
   */
  public String getIdentifier()
  {
    return identifier;
  }

  public void flush(final int writeOffset)
  {
    //logger.debug("size = {}, processingOffset = {}, nextOffset = {}, writeOffset = {}", size, processingOffset,
    //    nextOffset.integer, writeOffset);
    flush:
    do {
      while (size == 0) {
        size = VarInt.read(last.data, processingOffset, writeOffset, nextOffset);
        if (nextOffset.integer > -5 && nextOffset.integer < 1) {
          if (writeOffset == last.data.length) {
            nextOffset.integer = 0;
            processingOffset = 0;
            size = 0;
          }
          break flush;
        } else if (nextOffset.integer == -5) {
          throw new RuntimeException("problemo!");
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
            } else {
              last.ending_window = baseSeconds | bwt.getWindowId();
              //logger.debug("assigned last window id {}", last);
            }
            break;

          case MessageType.RESET_WINDOW_VALUE:
            Tuple rwt = Tuple.getTuple(last.data, processingOffset, size);
            baseSeconds = (long)rwt.getBaseSeconds() << 32;
            break;

          default:
            break;
        }
        processingOffset += size;
        size = 0;
      } else {
        if (writeOffset == last.data.length) {
          nextOffset.integer = 0;
          processingOffset = 0;
          size = 0;
        }
        break;
      }
    } while (true);

    last.writingOffset = writeOffset;

    notifyListeners();

  }

  public void notifyListeners()
  {
    try {
      listenersNotifier.moreDataAvailable();
    } catch (RuntimeException e) {
      logger.warn("{}", listenersNotifier, e);
    }
    logger.debug("{} notified", listenersNotifier);
  }

  public void setAutoFlushExecutor(final ExecutorService es)
  {
    autoFlushExecutor = es;
  }

  public void setSecondaryStorage(Storage storage, ExecutorService es)
  {
    this.storage = storage;
    storageExecutor = es;
  }

  /*
   * Iterator related functions.
   */
  protected DataListIterator getIterator(final Block block)
  {
    return new DataListIterator(block);
  }

  private synchronized Block getNextBlock(final Block block)
  {
    return block.next;
  }

  public DataListIterator newIterator(long windowId)
  {
    //logger.debug("request for a new iterator {} and {}", identifier, windowId);
    Block temp = first;
    while (temp != last) {
      if (temp.starting_window >= windowId || temp.ending_window > windowId) {
        break;
      }
      temp = temp.next;
    }
    //logger.debug("returning new iterator on temp = {}", temp);
    return getIterator(temp);
  }

  public void addDataListener(DataListener dl)
  {
    all_listeners.add(dl);
    //logger.debug("total {} listeners {} -> {}", all_listeners.size(), dl, this);
    ArrayList<BitVector> partitions = new ArrayList<>();
    if (dl.getPartitions(partitions) > 0) {
      for (BitVector partition : partitions) {
        HashSet<DataListener> set;
        if (listeners.containsKey(partition)) {
          set = listeners.get(partition);
        } else {
          set = new HashSet<>();
          listeners.put(partition, set);
        }
        set.add(dl);
      }
    } else {
      HashSet<DataListener> set;
      if (listeners.containsKey(DataListener.NULL_PARTITION)) {
        set = listeners.get(DataListener.NULL_PARTITION);
      } else {
        set = new HashSet<>();
        listeners.put(DataListener.NULL_PARTITION, set);
      }

      set.add(dl);
    }
    listenersNotifier.run();
  }

  public void removeDataListener(DataListener dl)
  {
    ArrayList<BitVector> partitions = new ArrayList<>();
    if (dl.getPartitions(partitions) > 0) {
      for (BitVector partition : partitions) {
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

  public boolean suspendRead(final AbstractClient client)
  {
    synchronized (suspendedClients) {
      return suspendedClients.add(client) && client.suspendReadIfResumed();
    }
  }

  public boolean resumeSuspendedClients(final int numberOfInMemBlockPermits)
  {
    boolean resumedSuspendedClients = false;
    if (numberOfInMemBlockPermits > 0) {
      resumedSuspendedClients = resumeSuspendedClients();
    } else {
      logger.debug("Keeping clients: {} suspended, numberOfInMemBlockPermits={}, Listeners: {}", suspendedClients,
          numberOfInMemBlockPermits, all_listeners);
    }
    return resumedSuspendedClients;
  }

  private boolean resumeSuspendedClients()
  {
    boolean resumedSuspendedClients = false;
    synchronized (suspendedClients) {
      for (AbstractClient client : suspendedClients) {
        resumedSuspendedClients |= client.resumeReadIfSuspended();
      }
      suspendedClients.clear();
    }
    return resumedSuspendedClients;
  }

  public boolean isMemoryBlockAvailable()
  {
    return (numberOfInMemBlockPermits.get() > 0);
  }

  public boolean areSubscribersBehindByMax()
  {
    boolean behind = false;
    if (backPressureEnabled) {
      // Seek to max blocks and see if any block is in use beyond that
      int count = 0;
      synchronized (this) {
        Block curr = last.prev;
        // go back the max number of blocks
        while ((curr != null) && (++count < (MAX_COUNT_OF_INMEM_BLOCKS - 2))) {
          curr = curr.prev;
        }
        // check if any block is in use
        while (!behind && (curr != null)) {
          // Since acquire happens before release, because of concurrency, in a corner case scenario we might still count a
          // subscriber as being max behind when it is transitioning over to the next block but that is ok as it will only
          // result in publisher blocking for some time and resuming
          behind = (curr.refCount.get() != 0);
          curr = curr.prev;
        }
      }
    }
    return behind;
  }

  public byte[] newBuffer(final int size)
  {
    if (size > blockSize) {
      logger.error("Tuple size {} exceeds buffer server current block size {}. Please decrease tuple size. " +
          "Proceeding with allocating larger block that may cause out of memory exception.", size, blockSize);
      return new byte[size];
    }
    return new byte[blockSize];
  }

  public synchronized void addBuffer(byte[] array)
  {
    final int numberOfInMemBlockPermits = this.numberOfInMemBlockPermits.decrementAndGet();
    if (numberOfInMemBlockPermits < 0) {
      logger.warn("Exceeded allowed memory block allocation by {}", -numberOfInMemBlockPermits);
    }
    last.next = new Block(identifier, array, last.ending_window, last.ending_window);
    last.next.prev = last;
    last.release(false, true);
    last = last.next;
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
    Map<Block, Integer> indices = new HashMap<>();
    int i = 0;
    while (b != null) {
      indices.put(b, i++);
      b = b.next;
    }
    int oldestBlockIndex = Integer.MAX_VALUE;
    int oldestReadOffset = Integer.MAX_VALUE;

    for (DataListener dl : all_listeners) {
      LogicalNode logicalNode = (LogicalNode)dl;
      DataListIterator dli = logicalNode.getIterator();
      Integer index = indices.get(dli.da);
      if (index == null) {
        // error
        throw new RuntimeException("problemo!");
      }
      if (index < oldestBlockIndex) {
        oldestBlockIndex = index;
        oldestReadOffset = dli.getReadOffset();
        status.slowestConsumer = logicalNode.getIdentifier();
      } else if (index == oldestBlockIndex && dli.getReadOffset() < oldestReadOffset) {
        oldestReadOffset = dli.getReadOffset();
        status.slowestConsumer = logicalNode.getIdentifier();
      }
    }

    b = first;
    i = 0;
    while (b != null) {
      status.numBytesAllocated += b.data.length;
      if (oldestBlockIndex == i) {
        status.numBytesWaiting += b.writingOffset - oldestReadOffset;
      } else if (oldestBlockIndex < i) {
        status.numBytesWaiting += b.writingOffset - b.readingOffset;
      }
      b = b.next;
      ++i;
    }
    return status;
  }

  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.DEFAULT).append("identifier", identifier).toString();
  }

  /**
   * <p>Block class.</p>
   *
   * @since 0.3.2
   */
  public class Block
  {
    final String identifier;
    /**
     * actual data - stored as length followed by actual data.
     */
    byte[] data;
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
    long starting_window;
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
     * the previous in the chain
     */
    Block prev;
    /**
     * how count of references to this block.
     */
    private final AtomicInteger refCount;
    private Future<?> future;

    public Block(String id, int size)
    {
      this(id, new byte[size]);
    }

    public Block(String id, byte[] array)
    {
      this(id, array, -1, 0);
    }

    public Block(final String id, final byte[] array, final long starting_window, final long ending_window)
    {
      identifier = id;
      data = array;
      refCount = new AtomicInteger(1);
      this.starting_window = starting_window;
      this.ending_window = ending_window;
      //logger.debug("Allocated new {}", this);
    }

    void getNextData(SerializedData current)
    {
      if (current.offset < writingOffset) {
        VarInt.read(current);
        if (current.offset + current.length > writingOffset) {
          current.length = 0;
        }
      } else {
        current.length = 0;
      }
    }

    public long rewind(long windowId)
    {
      long bs = starting_window & 0x7fffffff00000000L;
      try (DataListIterator dli = getIterator(this)) {
        done:
        while (dli.hasNext()) {
          final SerializedData sd = dli.next();
          final int length = sd.length - sd.dataOffset + sd.offset;
          switch (sd.buffer[sd.dataOffset]) {
            case MessageType.RESET_WINDOW_VALUE:
              final ResetWindowTuple rwt = (ResetWindowTuple)Tuple.getTuple(sd.buffer, sd.dataOffset, length);
              bs = (long)rwt.getBaseSeconds() << 32;
              if (bs > windowId) {
                writingOffset = sd.offset;
                break done;
              }
              break;

            case MessageType.BEGIN_WINDOW_VALUE:
              BeginWindowTuple bwt = (BeginWindowTuple)Tuple.getTuple(sd.buffer, sd.dataOffset, length);
              if ((bs | bwt.getWindowId()) >= windowId) {
                writingOffset = sd.offset;
                break done;
              }
              break;

            default:
              break;
          }
        }
      }

      if (starting_window == -1) {
        starting_window = windowId;
        ending_window = windowId;
        //logger.debug("assigned both window id {}", this);
      } else if (windowId < ending_window) {
        ending_window = windowId;
        //logger.debug("assigned end window id {}", this);
      }

      discard(false);

      return bs;
    }

    public void purge(long longWindowId)
    {
      //logger.debug("starting_window = {}, longWindowId = {}, ending_window = {}",
      //    VarInt.getStringWindowId(starting_window), VarInt.getStringWindowId(longWindowId),
      //    VarInt.getStringWindowId(ending_window));
      boolean found = false;
      long bs = starting_window & 0xffffffff00000000L;
      SerializedData lastReset = null;

      try (DataListIterator dli = getIterator(this)) {
        done:
        while (dli.hasNext()) {
          SerializedData sd = dli.next();
          final int length = sd.length - sd.dataOffset + sd.offset;
          switch (sd.buffer[sd.dataOffset]) {
            case MessageType.RESET_WINDOW_VALUE:
              ResetWindowTuple rwt = (ResetWindowTuple)Tuple.getTuple(sd.buffer, sd.dataOffset, length);
              bs = (long)rwt.getBaseSeconds() << 32;
              lastReset = sd;
              break;

            case MessageType.BEGIN_WINDOW_VALUE:
              BeginWindowTuple bwt = (BeginWindowTuple)Tuple.getTuple(sd.buffer, sd.dataOffset, length);
              if ((bs | bwt.getWindowId()) > longWindowId) {
                found = true;
                if (lastReset != null) {
                  /*
                   * Restore the last Reset tuple if there was any and adjust the writingOffset to the beginning of
                   * the reset tuple.
                   */
                  if (sd.offset >= lastReset.length) {
                    sd.offset -= lastReset.length;
                    if (!(sd.buffer == lastReset.buffer && sd.offset == lastReset.offset)) {
                      System.arraycopy(lastReset.buffer, lastReset.offset, sd.buffer, sd.offset, lastReset.length);
                    }
                  }

                  this.starting_window = bs | bwt.getWindowId();
                  this.readingOffset = sd.offset;
                  //logger.debug("assigned starting window id {}", this);
                }

                break done;
              }
              break;

            default:
              break;
          }
        }
      }

      /**
       * If we ended up purging all the data from the current Block then,
       * it also makes sense to start all over.
       * It helps with better utilization of the RAM.
       */
      if (!found) {
        //logger.debug("we could not find a tuple which is in a window later than the window to be purged, " +
        //    "so this has to be the last window published so far");
        if (lastReset != null && lastReset.offset != 0) {
          this.readingOffset = this.writingOffset - lastReset.length;
          System.arraycopy(lastReset.buffer, lastReset.offset, this.data, this.readingOffset, lastReset.length);
          this.starting_window = this.ending_window = bs;
          //logger.debug("=20140220= reassign the windowids {}", this);
        } else {
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
          sd.length = sd.offset;
          sd.offset = 0;
          sd.dataOffset = VarInt.write(sd.length - i, sd.buffer, sd.offset, i);
          sd.buffer[sd.dataOffset] = MessageType.NO_MESSAGE_VALUE;
        } else {
          logger.warn("Unhandled condition while purging the data purge to offset {}", sd.offset);
        }

        discard(false);
      }
    }

    private Runnable getRetriever()
    {
      return new Runnable()
      {
        @Override
        public void run()
        {
          byte[] data = storage.retrieve(identifier, uniqueIdentifier);
          synchronized (Block.this) {
            if (Block.this.data == null) {
              Block.this.data = data;
              readingOffset = 0;
              writingOffset = data.length;
              Block.this.notifyAll();
              int numberOfInMemBlockPermits = DataList.this.numberOfInMemBlockPermits.decrementAndGet();
              if (numberOfInMemBlockPermits < 0) {
                logger.warn("Exceeded allowed memory block allocation by {}", -numberOfInMemBlockPermits);
              }
            } else {
              logger.debug("Block {} was already loaded into memory", Block.this);
            }
          }
        }
      };
    }

    protected void acquire(boolean wait)
    {
      int refCount = this.refCount.getAndIncrement();
      synchronized (Block.this) {
        if (data != null) {
          return;
        }
      }
      if (refCount == 0 && storage != null) {
        final Runnable retriever = getRetriever();
        if (future != null && future.cancel(false)) {
          logger.debug("Block {} future is cancelled", this);
        }
        if (wait) {
          future = null;
          retriever.run();
        } else {
          future = storageExecutor.submit(retriever);
        }
      } else if (wait) {
        try {
          synchronized (Block.this) {
            if (future == null) {
              throw new IllegalStateException("No task is scheduled to retrieve block " + Block.this);
            }
            while (data == null) {
              wait();
            }
          }
        } catch (InterruptedException ex) {
          throw new RuntimeException("Interrupted while waiting for data to be loaded!", ex);
        }
      }
    }

    private Runnable getStorer(final byte[] data, final int readingOffset, final int writingOffset,
        final Storage storage)
    {
      return new Runnable()
      {
        @Override
        public void run()
        {
          if (uniqueIdentifier == 0) {
            uniqueIdentifier = storage.store(identifier, data, readingOffset, writingOffset);
          }
          if (uniqueIdentifier == 0) {
            logger.warn("Storage returned unexpectedly, please check the status of the spool directory!");
          } else {
            int numberOfInMemBlockPermits = DataList.this.numberOfInMemBlockPermits.get();
            synchronized (Block.this) {
              if (refCount.get() == 0 && Block.this.data != null) {
                Block.this.data = null;
                numberOfInMemBlockPermits = DataList.this.numberOfInMemBlockPermits.incrementAndGet();
              } else {
                logger.debug("Keeping Block {} unchanged", Block.this);
              }
            }
            assert numberOfInMemBlockPermits < MAX_COUNT_OF_INMEM_BLOCKS : "Number of in memory block permits " +
                numberOfInMemBlockPermits + " exceeded configured maximum " + MAX_COUNT_OF_INMEM_BLOCKS + '.';
            resumeSuspendedClients(numberOfInMemBlockPermits);
          }
        }
      };
    }

    protected void release(boolean wait, boolean writer)
    {
      final int refCount = this.refCount.decrementAndGet();
      if (canEvict(refCount, writer)) {
        assert (next != null);
        if (storage != null) {
          evictBlock(wait);
        } else {
          if (!isPublisherAheadByMax()) {
            resumeSuspendedClients();
          }
        }
      }
    }

    private void evictBlock(boolean wait)
    {
      final Runnable storer = getStorer(data, readingOffset, writingOffset, storage);
      if (future != null && future.cancel(false)) {
        logger.debug("Block {} future is cancelled", this);
      }
      final int numberOfInMemBlockPermits = DataList.this.numberOfInMemBlockPermits.get();
      if (wait && numberOfInMemBlockPermits == 0) {
        future = null;
        storer.run();
      } else if (numberOfInMemBlockPermits < MAX_COUNT_OF_INMEM_BLOCKS / 2) {
        future = storageExecutor.submit(storer);
      } else {
        future = null;
      }
    }

    private boolean canEvict(final int refCount, boolean writer)
    {
      if (refCount == 0) {
        if (backPressureEnabled) {
          if (!writer) {
            boolean evict = true;
            // Search backwards from current block as opposed to searching forward from first block till current block as
            // it is more likely to find the match quicker.
            synchronized (this) {
              for (Block temp = this.prev; temp != null; temp = temp.prev) {
                if (temp.refCount.get() != 0) {
                  evict = false;
                  break;
                }
              }
            }
            logger.debug("Block {} evict {}", this, evict);
            return evict;
          } else {
            return false;
          }
        } else {
          return true;
        }
      }
      logger.debug("Not evicting {} due to {} references.", this, refCount);
      return false;
    }

    private boolean isPublisherAheadByMax()
    {
      boolean ahead = false;
      if (backPressureEnabled) {
        int blocks = MAX_COUNT_OF_INMEM_BLOCKS;
        synchronized (DataList.this) {
          Block curr = this.next;
          // seek till the next block that is in use to determine possible active subscriber
          while ((curr != null) && (curr.refCount.get() == 0)) {
            curr = curr.next;
          }
          // find if publisher is ahead by max
          while (!ahead && (curr != null)) {
            ahead = (--blocks == 0);
            curr = curr.next;
          }
        }
      }
      return ahead;
    }

    private Runnable getDiscarder()
    {
      return new Runnable()
      {
        @Override
        public void run()
        {
          if (uniqueIdentifier > 0) {
            logger.debug("Discarding {}", Block.this);
            storage.discard(identifier, uniqueIdentifier);
            uniqueIdentifier = 0;
          }
        }
      };
    }

    protected void discard(final boolean wait)
    {
      if (storage != null) {
        final Runnable discarder = getDiscarder();
        if (future != null && future.cancel(false)) {
          logger.debug("Block {} future is cancelled", this);
        }
        if (wait) {
          future = null;
          discarder.run();
        } else {
          future = storageExecutor.submit(discarder);
        }
      }
    }

    @Override
    public String toString()
    {
      final String future = this.future == null ? "null" : this.future.isDone() ? "Done" :
          this.future.isCancelled() ? "Cancelled" : this.future.toString();
      return getClass().getName() + '@' + Integer.toHexString(hashCode()) + "{identifier=" + identifier +
          ", data=" + (data == null ? "null" : data.length) + ", readingOffset=" + readingOffset +
          ", writingOffset=" + writingOffset + ", starting_window=" + Codec.getStringWindowId(starting_window) +
          ", ending_window=" + Codec.getStringWindowId(ending_window) + ", refCount=" + refCount.get() +
          ", uniqueIdentifier=" + uniqueIdentifier + ", next=" + (next == null ? "null" : next.identifier) +
          ", future=" + future + '}';
    }

  }

  /**
   * <p>DataListIterator class.</p>
   *
   * @since 0.3.2
   */
  public class DataListIterator implements Iterator<SerializedData>, AutoCloseable
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

    protected boolean switchToNextBlock()
    {
      Block next = getNextBlock(da);
      if (next == null) {
        return false;
      }
      //logger.debug("{}: switching to the next block {}->{}", this, da, da.next);
      next.acquire(true);
      da.release(false, false);
      da = next;
      size = 0;
      buffer = da.data;
      readOffset = da.readingOffset;
      return true;
    }

    /**
     *
     * @return boolean
     */
    @Override
    public boolean hasNext()
    {
      while (size == 0) {
        size = VarInt.read(buffer, readOffset, da.writingOffset, nextOffset);
        if (nextOffset.integer > -5 && nextOffset.integer < 1) {
          if (da.writingOffset == buffer.length && switchToNextBlock()) {
            continue;
          }
          return false;
        } else if (size == -5) {
          throw new RuntimeException("problemo!");
        }
      }

      if (nextOffset.integer + size <= da.writingOffset) {
        current = new SerializedData(buffer, readOffset, size + nextOffset.integer - readOffset);
        current.dataOffset = nextOffset.integer;
        //final byte messageType = buffer[current.dataOffset];
        //if (messageType == MessageType.BEGIN_WINDOW_VALUE || messageType == MessageType.END_WINDOW_VALUE) {
        //  final int length = current.length - current.dataOffset + current.offset;
        //  Tuple t = Tuple.getTuple(current.buffer, current.dataOffset, length);
        //  logger.debug("next t = {}", t);
        //}
        return true;
      } else if (da.writingOffset == buffer.length && switchToNextBlock()) {
        nextOffset.integer = da.readingOffset;
        return hasNext();
      }
      return false;
    }

    /**
     *
     * @return {@link com.datatorrent.bufferserver.util.SerializedData}
     */
    @Override
    public SerializedData next()
    {
      readOffset = current.offset + current.length;
      size = 0;
      return current;
    }

    /**
     * Removes from the underlying collection the last element returned by the iterator (optional operation). This
     * method can be called only once per call to next. The behavior of an iterator is unspecified if the underlying
     * collection is modified while the iteration is in progress in any way other than by calling this method.
     */
    @Override
    public void remove()
    {
      current.buffer[current.dataOffset] = MessageType.NO_MESSAGE_VALUE;
    }

    @Override
    public void close()
    {
      if (da != null) {
        da.release(false, false);
        da = null;
        buffer = null;
      }
    }

    void rewind(int processingOffset)
    {
      readOffset = processingOffset;
      size = 0;
    }

    @Override
    public String toString()
    {
      return getClass().getName() + '@' + Integer.toHexString(hashCode()) + "{da=" + da + '}';
    }

  }

  private class ListenersNotifier implements Runnable
  {
    private volatile Future<?> future;
    private boolean isMoreDataAvailable = false;

    private void moreDataAvailable()
    {
      final Future<?> future = this.future;
      if (future == null || future.isDone() || future.isCancelled()) {
        // Do not schedule a new task if there is an existing one that is still running or is waiting in the queue
        this.future = autoFlushExecutor.submit(this);
      } else {
        synchronized (this) {
          if (this.future == null) {
            // future is set to null before run() exists, no need to check whether future isDone() or isCancelled()
            this.future = autoFlushExecutor.submit(this);
          } else {
            isMoreDataAvailable = true;
          }
        }
      }
    }

    private boolean addedData()
    {
      boolean doesAtLeastOneListenerHaveDataToSend = false;
      for (DataListener dl : all_listeners) {
        try {
          doesAtLeastOneListenerHaveDataToSend |= dl.addedData(false);
        } catch (RuntimeException e) {
          logger.warn("{} removing {} due to exception", this, dl, e);
          removeDataListener(dl);
          break;
        }
      }
      return doesAtLeastOneListenerHaveDataToSend;
    }

    private boolean checkIfListenersHaveDataToSendOnly()
    {
      for (DataListener dl : all_listeners) {
        try {
          if (dl.addedData(true)) {
            return true;
          }
        } catch (RuntimeException e) {
          logger.warn("{} removing {} due to exception", this, dl, e);
          removeDataListener(dl);
          return checkIfListenersHaveDataToSendOnly();
        }
      }
      return false;
    }

    @Override
    public void run()
    {
      logger.debug("{} entered run", this);
      try {
        if (addedData() || checkIfListenersHaveDataToSendOnly()) {
          future = autoFlushExecutor.submit(this);
        } else {
          synchronized (this) {
            if (isMoreDataAvailable) {
              isMoreDataAvailable = false;
              future = autoFlushExecutor.submit(this);
            } else {
              future = null;
            }
          }
        }
      } catch (RuntimeException e) {
        logger.warn("{}", this, e);
      } finally {
        logger.debug("{} exiting run", this);
      }
    }

    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.DEFAULT).append(DataList.this)
          .append("future", future == null ? null : future.getClass().getSimpleName() + '@' + Integer.toHexString(System.identityHashCode(future)))
          .append("isMoreDataAvailable", isMoreDataAvailable).toString();
    }
  }
}
