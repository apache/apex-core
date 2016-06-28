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
package com.datatorrent.bufferserver.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.internal.DataList;
import com.datatorrent.bufferserver.internal.FastDataList;
import com.datatorrent.bufferserver.internal.LogicalNode;
import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.bufferserver.packet.PublishRequestTuple;
import com.datatorrent.bufferserver.packet.PurgeRequestTuple;
import com.datatorrent.bufferserver.packet.ResetRequestTuple;
import com.datatorrent.bufferserver.packet.SubscribeRequestTuple;
import com.datatorrent.bufferserver.packet.Tuple;
import com.datatorrent.bufferserver.storage.Storage;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.netlet.AbstractLengthPrependerClient;
import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.EventLoop;
import com.datatorrent.netlet.Listener.ServerListener;
import com.datatorrent.netlet.util.VarInt;

/**
 * The buffer server application<p>
 * <br>
 *
 * @since 0.3.2
 */
public class Server implements ServerListener
{
  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024 * 1024;
  public static final int DEFAULT_NUMBER_OF_CACHED_BLOCKS = 8;
  private final int port;
  private String identity;
  private Storage storage;
  private EventLoop eventloop;
  private InetSocketAddress address;
  private final ExecutorService serverHelperExecutor;
  private final ExecutorService storageHelperExecutor;

  private byte[] authToken;

  /**
   * @param port - port number to bind to or 0 to auto select a free port
   */
  public Server(int port)
  {
    this(port, DEFAULT_BUFFER_SIZE, DEFAULT_NUMBER_OF_CACHED_BLOCKS);
  }

  public Server(int port, int blocksize, int numberOfCacheBlocks)
  {
    this.port = port;
    this.blockSize = blocksize;
    this.numberOfCacheBlocks = numberOfCacheBlocks;
    serverHelperExecutor = Executors.newSingleThreadExecutor(new NameableThreadFactory("ServerHelper"));
    final ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(numberOfCacheBlocks);
    final NameableThreadFactory threadFactory = new NameableThreadFactory("StorageHelper");
    storageHelperExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, workQueue, threadFactory,
        new ThreadPoolExecutor.CallerRunsPolicy());
  }

  public void setSpoolStorage(Storage storage)
  {
    this.storage = storage;
  }

  @Override
  public synchronized void registered(SelectionKey key)
  {
    ServerSocketChannel channel = (ServerSocketChannel)key.channel();
    address = (InetSocketAddress)channel.socket().getLocalSocketAddress();
    logger.info("Server started listening at {}", address);
    notifyAll();
  }

  @Override
  public void unregistered(SelectionKey key)
  {
    serverHelperExecutor.shutdown();
    storageHelperExecutor.shutdown();
    try {
      serverHelperExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      logger.debug("Executor Termination", ex);
    }
    logger.info("Server stopped listening at {}", address);
  }

  public synchronized InetSocketAddress run(EventLoop eventloop)
  {
    eventloop.start(null, port, this);
    while (address == null) {
      try {
        wait(20);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }

    this.eventloop = eventloop;
    return address;
  }

  public void setAuthToken(byte[] authToken)
  {
    this.authToken = authToken;
  }

  /**
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception
  {
    int port;
    if (args.length > 0) {
      port = Integer.parseInt(args[0]);
    } else {
      port = 0;
    }

    DefaultEventLoop eventloop = DefaultEventLoop.createEventLoop("alone");
    eventloop.start(null, port, new Server(port));
    new Thread(eventloop).start();
  }

  @Override
  public String toString()
  {
    return identity;
  }

  private final ConcurrentHashMap<String, DataList> publisherBuffers = new ConcurrentHashMap<>(1, 0.75f, 1);
  private final ConcurrentHashMap<String, LogicalNode> subscriberGroups = new ConcurrentHashMap<String, LogicalNode>();
  private final ConcurrentHashMap<String, AbstractLengthPrependerClient> publisherChannels = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AbstractLengthPrependerClient> subscriberChannels = new ConcurrentHashMap<>();
  private final int blockSize;
  private final int numberOfCacheBlocks;

  private void handlePurgeRequest(PurgeRequestTuple request, final AbstractLengthPrependerClient ctx) throws IOException
  {
    DataList dl;
    dl = publisherBuffers.get(request.getIdentifier());

    byte[] message;
    if (dl == null) {
      message = ("Invalid identifier '" + request.getIdentifier() + "'").getBytes();
    } else {
      dl.purge((long)request.getBaseSeconds() << 32 | request.getWindowId());
      message = ("Request sent for processing: " + request).getBytes();
    }

    final byte[] tuple = PayloadTuple.getSerializedTuple(0, message.length);
    System.arraycopy(message, 0, tuple, tuple.length - message.length, message.length);
    if (ctx.write(tuple)) {
      ctx.write();
    } else {
      logger.error("Failed to deliver purge ack message. {} send buffers are full.", ctx);
      throw new RuntimeException("Failed to deliver purge ack message. " + ctx + "send buffers are full.");
    }
  }

  public void purge(long windowId)
  {
    for (DataList dataList: publisherBuffers.values()) {
      dataList.purge(windowId);
    }
  }

  private void handleResetRequest(ResetRequestTuple request, final AbstractLengthPrependerClient ctx) throws IOException
  {
    DataList dl;
    dl = publisherBuffers.remove(request.getIdentifier());

    byte[] message;
    if (dl == null) {
      message = ("Invalid identifier '" + request.getIdentifier() + "'").getBytes();
    } else {
      AbstractLengthPrependerClient channel = publisherChannels.remove(request.getIdentifier());
      if (channel != null) {
        eventloop.disconnect(channel);
      }
      dl.reset();
      message = ("Request sent for processing: " + request).getBytes();
    }

    final byte[] tuple = PayloadTuple.getSerializedTuple(0, message.length);
    System.arraycopy(message, 0, tuple, tuple.length - message.length, message.length);
    if (ctx.write(tuple)) {
      ctx.write();
    } else {
      logger.error("Failed to deliver reset ack message. {} send buffers are full.", ctx);
      throw new RuntimeException("Failed to deliver reset ack message. " + ctx + "send buffers are full.");
    }
  }

  /**
   *
   * @param request
   * @param connection
   * @return
   */
  public LogicalNode handleSubscriberRequest(SubscribeRequestTuple request,
      final AbstractLengthPrependerClient connection)
  {
    String identifier = request.getIdentifier();
    String type = request.getStreamType();
    String upstream_identifier = request.getUpstreamIdentifier();

    // Check if there is a logical node of this type, if not create it.
    final LogicalNode ln;
    if (subscriberGroups.containsKey(type)) {
      //logger.debug("adding to exiting group = {}", subscriberGroups.get(type));
      /*
       * close previous connection with the same identifier which is guaranteed to be unique.
       */
      AbstractLengthPrependerClient previous = subscriberChannels.put(identifier, connection);
      if (previous != null) {
        eventloop.disconnect(previous);
      }

      ln = subscriberGroups.get(type);
      serverHelperExecutor.submit(new Runnable()
      {
        @Override
        public void run()
        {
          ln.boot(eventloop);
          ln.addConnection(connection);
          ln.catchUp();
        }
      });
    } else {
      /*
       * if there is already a datalist registered for the type in which this client is interested,
       * then get a iterator on the data items of that data list. If the datalist is not registered,
       * then create one and register it. Hopefully this one would be used by future upstream nodes.
       */
      final DataList dl;
      if (publisherBuffers.containsKey(upstream_identifier)) {
        dl = publisherBuffers.get(upstream_identifier);
        //logger.debug("old list = {}", dl);
      } else {
        dl = Tuple.FAST_VERSION.equals(request.getVersion()) ?
            new FastDataList(upstream_identifier, blockSize, numberOfCacheBlocks) :
            new DataList(upstream_identifier, blockSize, numberOfCacheBlocks);
        publisherBuffers.put(upstream_identifier, dl);
        //logger.debug("new list = {}", dl);
      }

      long skipWindowId = (long)request.getBaseSeconds() << 32 | request.getWindowId();
      ln = new LogicalNode(identifier, upstream_identifier, type, dl.newIterator(skipWindowId), skipWindowId);

      int mask = request.getMask();
      if (mask != 0) {
        for (Integer bs : request.getPartitions()) {
          ln.addPartition(bs, mask);
        }
      }

      subscriberGroups.put(type, ln);
      serverHelperExecutor.submit(new Runnable()
      {
        @Override
        public void run()
        {
          ln.addConnection(connection);
          ln.catchUp();
          dl.addDataListener(ln);
        }
      });
    }

    return ln;
  }

  /**
   *
   * @param request
   * @param connection
   * @return
   */
  public DataList handlePublisherRequest(PublishRequestTuple request, AbstractLengthPrependerClient connection)
  {
    String identifier = request.getIdentifier();

    DataList dl;

    if (publisherBuffers.containsKey(identifier)) {
      /*
       * close previous connection with the same identifier which is guaranteed to be unique.
       */
      AbstractLengthPrependerClient previous = publisherChannels.put(identifier, connection);
      if (previous != null) {
        eventloop.disconnect(previous);
      }

      dl = publisherBuffers.get(identifier);
      try {
        dl.rewind(request.getBaseSeconds(), request.getWindowId());
      } catch (IOException ie) {
        throw new RuntimeException(ie);
      }
    } else {
      dl = Tuple.FAST_VERSION.equals(request.getVersion()) ?
          new FastDataList(identifier, blockSize, numberOfCacheBlocks) :
          new DataList(identifier, blockSize, numberOfCacheBlocks);
      publisherBuffers.put(identifier, dl);
    }
    dl.setSecondaryStorage(storage, storageHelperExecutor);

    return dl;
  }

  @Override
  public ClientListener getClientConnection(SocketChannel sc, ServerSocketChannel ssc)
  {
    ClientListener client;
    if (authToken == null) {
      client = new UnidentifiedClient();
    } else {
      AuthClient authClient = new AuthClient();
      authClient.setToken(authToken);
      client = authClient;
    }
    return client;
  }

  @Override
  public void handleException(Exception cce, EventLoop el)
  {
    if (cce instanceof RuntimeException) {
      throw (RuntimeException)cce;
    }

    throw new RuntimeException(cce);
  }

  class AuthClient extends com.datatorrent.bufferserver.client.AuthClient
  {
    boolean ignore;

    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      if (ignore) {
        return;
      }

      authenticateMessage(buffer, offset, size);

      unregistered(key);
      UnidentifiedClient client = new UnidentifiedClient();
      key.attach(client);
      key.interestOps(SelectionKey.OP_READ);
      client.registered(key);
      client.connected();

      int len = writeOffset - readOffset - size;
      if (len > 0) {
        client.transferBuffer(buffer, readOffset + size, len);
      }

      ignore = true;
    }
  }

  class UnidentifiedClient extends SeedDataClient
  {
    boolean ignore;

    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      if (ignore) {
        return;
      }

      Tuple request = Tuple.getTuple(buffer, offset, size);
      switch (request.getType()) {
        case PUBLISHER_REQUEST:

          /*
           * unregister the unidentified client since its job is done!
           */
          unregistered(key);
          logger.info("Received publisher request: {}", request);
          PublishRequestTuple publisherRequest = (PublishRequestTuple)request;

          DataList dl = handlePublisherRequest(publisherRequest, this);
          dl.setAutoFlushExecutor(serverHelperExecutor);

          Publisher publisher;
          if (publisherRequest.getVersion().equals(Tuple.FAST_VERSION)) {
            publisher = new Publisher(dl, (long)request.getBaseSeconds() << 32 | request.getWindowId())
            {
              @Override
              public int readSize()
              {
                if (writeOffset - readOffset < 2) {
                  return -1;
                }

                short s = buffer[readOffset++];
                return s | (buffer[readOffset++] << 8);
              }

            };
          } else {
            publisher = new Publisher(dl, (long)request.getBaseSeconds() << 32 | request.getWindowId());
          }

          key.attach(publisher);
          key.interestOps(SelectionKey.OP_READ);
          publisher.registered(key);

          int len = writeOffset - readOffset - size;
          if (len > 0) {
            publisher.transferBuffer(this.buffer, readOffset + size, len);
          }
          ignore = true;

          break;

        case SUBSCRIBER_REQUEST:
          /*
           * unregister the unidentified client since its job is done!
           */
          unregistered(key);
          ignore = true;
          logger.info("Received subscriber request: {}", request);

          SubscribeRequestTuple subscriberRequest = (SubscribeRequestTuple)request;
          AbstractLengthPrependerClient subscriber;

//          /* for backward compatibility - set the buffer size to 16k - EXPERIMENTAL */
          int bufferSize = subscriberRequest.getBufferSize();
//          if (bufferSize == 0) {
//            bufferSize = 16 * 1024;
//          }
          if (subscriberRequest.getVersion().equals(Tuple.FAST_VERSION)) {
            subscriber = new Subscriber(subscriberRequest.getStreamType(), subscriberRequest.getMask(),
                subscriberRequest.getPartitions(), bufferSize);
          } else {
            subscriber = new Subscriber(subscriberRequest.getStreamType(), subscriberRequest.getMask(),
                subscriberRequest.getPartitions(), bufferSize)
            {
              @Override
              public int readSize()
              {
                if (writeOffset - readOffset < 2) {
                  return -1;
                }

                short s = buffer[readOffset++];
                return s | (buffer[readOffset++] << 8);
              }

            };
          }
          key.attach(subscriber);
          key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
          subscriber.registered(key);

          handleSubscriberRequest(subscriberRequest, subscriber);
          break;

        case PURGE_REQUEST:
          logger.info("Received purge request: {}", request);
          try {
            handlePurgeRequest((PurgeRequestTuple)request, this);
          } catch (IOException io) {
            throw new RuntimeException(io);
          }
          break;

        case RESET_REQUEST:
          logger.info("Received reset all request: {}", request);
          try {
            handleResetRequest((ResetRequestTuple)request, this);
          } catch (IOException io) {
            throw new RuntimeException(io);
          }
          break;

        default:
          throw new RuntimeException("unexpected message: " + request.toString());
      }
    }

  }

  class Subscriber extends AbstractLengthPrependerClient
  {
    private final String type;
    private final int mask;
    private final int[] partitions;

    Subscriber(String type, int mask, int[] partitions, int bufferSize)
    {
      super(1024, bufferSize);
      this.type = type;
      this.mask = mask;
      this.partitions = partitions;
      super.write = false;
    }

    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      logger.warn("Received data when no data is expected: {}",
          Arrays.toString(Arrays.copyOfRange(buffer, offset, offset + size)));
    }

    @Override
    public void unregistered(final SelectionKey key)
    {
      super.unregistered(key);
      teardown();
    }

    @Override
    public void handleException(Exception cce, EventLoop el)
    {
      teardown();
      super.handleException(cce, el);
    }

    @Override
    public String toString()
    {
      return "Server.Subscriber{" + "type=" + type + ", mask=" + mask +
          ", partitions=" + (partitions == null ? "null" : Arrays.toString(partitions)) + '}';
    }

    private volatile boolean torndown;

    private void teardown()
    {
      //logger.debug("Teardown is being called {}", torndown, new Exception());
      if (torndown) {
        return;
      }
      torndown = true;

      LogicalNode ln = subscriberGroups.get(type);
      if (ln != null) {
        if (subscriberChannels.containsValue(this)) {
          final Iterator<Entry<String, AbstractLengthPrependerClient>> i = subscriberChannels.entrySet().iterator();
          while (i.hasNext()) {
            if (i.next().getValue() == this) {
              i.remove();
              break;
            }
          }
        }

        ln.removeChannel(this);
        if (ln.getPhysicalNodeCount() == 0) {
          DataList dl = publisherBuffers.get(ln.getUpstream());
          if (dl != null) {
            dl.removeDataListener(ln);
          }
          subscriberGroups.remove(ln.getGroup());
        }
        ln.getIterator().close();
      }
    }

  }

  /**
   * When the publisher connects to the server and starts publishing the data,
   * this is the end on the server side which handles all the communication.
   *
   */
  class Publisher extends SeedDataClient
  {
    private final DataList datalist;
    boolean dirty;

    Publisher(DataList dl, long windowId)
    {
      super(dl.getBuffer(windowId), dl.getPosition(), 1024);
      this.datalist = dl;
    }

    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      //if (buffer[offset] == MessageType.BEGIN_WINDOW_VALUE || buffer[offset] == MessageType.END_WINDOW_VALUE) {
      //  logger.debug("server received {}", Tuple.getTuple(buffer, offset, size));
      //}
      dirty = true;
    }

    /**
     * Schedules a task to conditionally resume I/O channel read operations.
     * No-op if {@linkplain java.nio.channels.SelectionKey#OP_READ OP_READ}
     * is already set in the key {@linkplain java.nio.channels.SelectionKey#interestOps() interestOps}.
     * Otherwise, calls {@linkplain #read(int) read(0)} to process data
     * left in the Publisher read buffer and registers {@linkplain java.nio.channels.SelectionKey#OP_READ OP_READ}
     * in the key {@linkplain java.nio.channels.SelectionKey#interestOps() interestOps}.
     * @return true
     */
    @Override
    public boolean resumeReadIfSuspended()
    {
      eventloop.submit(new Runnable()
      {
        @Override
        public void run()
        {
          final int interestOps = key.interestOps();
          if ((interestOps & SelectionKey.OP_READ) == 0) {
            if (readExt(0)) {
              logger.debug("Resuming read on key {} with attachment {}", key, key.attachment());
              key.interestOps(interestOps | SelectionKey.OP_READ);
            } else {
              logger.debug("Keeping read on key {} with attachment {} suspended. ", key, key.attachment(), datalist);
              datalist.notifyListeners();
            }
          }
        }
      });
      return true;
    }

    @Override
    public void read(int len)
    {
      readExt(len);
    }

    private boolean readExt(int len)
    {
      //logger.debug("read {} bytes", len);
      writeOffset += len;
      do {
        if (size <= 0) {
          switch (size = readSize()) {
            case -1:
              if (writeOffset == buffer.length) {
                if (readOffset > writeOffset - 5) {
                  dirty = false;
                  datalist.flush(writeOffset);
                  /*
                   * if the data is not corrupt, we are limited by space to receive full varint.
                   * so we allocate a new byteBuffer and copy over the partially written data to the
                   * new byteBuffer and start as if we always had full room but not enough data.
                   */
                  if (!switchToNewBufferOrSuspendRead(buffer, readOffset, size  + VarInt.getSize(size))) {
                    return false;
                  }
                }
              } else if (dirty) {
                dirty = false;
                datalist.flush(writeOffset);
              }
              return true;

            case 0:
              continue;

            default:
              break;
          }
        }

        if (writeOffset - readOffset >= size) {
          onMessage(buffer, readOffset, size);
          readOffset += size;
          size = 0;
        } else {
          if (writeOffset == buffer.length) {
            dirty = false;
            datalist.flush(writeOffset);
            /*
             * hit wall while writing serialized data, so have to allocate a new byteBuffer.
             */
            if (!switchToNewBufferOrSuspendRead(buffer, readOffset - VarInt.getSize(size), size + VarInt.getSize(size))) {
              readOffset -= VarInt.getSize(size);
              size = 0;
              return false;
            }
            size = 0;
          } else if (dirty) {
            dirty = false;
            datalist.flush(writeOffset);
          }
          return true;
        }
      } while (true);
    }

    private boolean switchToNewBufferOrSuspendRead(final byte[] array, final int offset, final int size)
    {
      if (switchToNewBuffer(array, offset, size)) {
        return true;
      }
      datalist.suspendRead(this);
      return false;
    }

    private boolean switchToNewBuffer(final byte[] array, final int offset, final int size)
    {
      if (datalist.isMemoryBlockAvailable()) {
        final byte[] newBuffer = datalist.newBuffer(size);
        byteBuffer = ByteBuffer.wrap(newBuffer);
        if (array == null || array.length - offset == 0) {
          writeOffset = 0;
        } else {
          writeOffset = array.length - offset;
          System.arraycopy(buffer, offset, newBuffer, 0, writeOffset);
          byteBuffer.position(writeOffset);
        }
        buffer = newBuffer;
        readOffset = 0;
        datalist.addBuffer(buffer);
        return true;
      }
      return false;
    }

    @Override
    public void unregistered(final SelectionKey key)
    {
      super.unregistered(key);
      teardown();
    }

    @Override
    public void handleException(Exception cce, EventLoop el)
    {
      teardown();

      if (cce instanceof RejectedExecutionException && serverHelperExecutor.isTerminated()) {
        logger.warn("Terminated Executor Exception for {}.", this, cce);
        el.disconnect(this);
      } else {
        super.handleException(cce, el);
      }
    }

    @Override
    public String toString()
    {
      return getClass().getName() + '@' + Integer.toHexString(hashCode()) + " {datalist=" + datalist + '}';
    }

    private volatile boolean torndown;

    private void teardown()
    {
      //logger.debug("Teardown is being called {}", torndown, new Exception());
      if (torndown) {
        return;
      }
      torndown = true;

      /*
       * if the publisher unregistered, all the downstream guys are going to be unregistered anyways
       * in our world. So it makes sense to kick them out proactively. Otherwise these clients since
       * are not being written to, just stick around till the next publisher shows up and eat into
       * the data it's publishing for the new subscribers.
       */

      /**
       * since the publisher server died, the queue which it was using would stop pumping the data unless
       * a new publisher comes up with the same name. We leave it to the stream to decide when to bring up a new node
       * with the same identifier as the one which just died.
       */
      if (publisherChannels.containsValue(this)) {
        final Iterator<Entry<String, AbstractLengthPrependerClient>> i = publisherChannels.entrySet().iterator();
        while (i.hasNext()) {
          if (i.next().getValue() == this) {
            i.remove();
            break;
          }
        }
      }

      ArrayList<LogicalNode> list = new ArrayList<LogicalNode>();
      String publisherIdentifier = datalist.getIdentifier();
      Iterator<LogicalNode> iterator = subscriberGroups.values().iterator();
      while (iterator.hasNext()) {
        LogicalNode ln = iterator.next();
        if (publisherIdentifier.equals(ln.getUpstream())) {
          list.add(ln);
        }
      }

      for (LogicalNode ln : list) {
        ln.boot(eventloop);
      }
    }

  }

  abstract class SeedDataClient extends AbstractLengthPrependerClient
  {

    public SeedDataClient()
    {
    }

    public SeedDataClient(int readBufferSize, int sendBufferSize)
    {
      super(readBufferSize, sendBufferSize);
    }

    public SeedDataClient(byte[] readbuffer, int position, int sendBufferSize)
    {
      super(readbuffer, position, sendBufferSize);
    }

    public void transferBuffer(byte[] array, int offset, int len)
    {
      int remainingCapacity;
      do {
        remainingCapacity = buffer.length - writeOffset;
        if (len < remainingCapacity) {
          remainingCapacity = len;
          byteBuffer.position(writeOffset + remainingCapacity);
        } else {
          byteBuffer.position(buffer.length);
        }
        System.arraycopy(array, offset, buffer, writeOffset, remainingCapacity);
        read(remainingCapacity);

        offset += remainingCapacity;
      } while ((len -= remainingCapacity) > 0);
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(Server.class);
}
