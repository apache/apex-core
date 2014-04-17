/*
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All Rights Reserved.
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.internal.DataList;
import com.datatorrent.bufferserver.internal.FastDataList;
import com.datatorrent.bufferserver.internal.LogicalNode;
import com.datatorrent.bufferserver.packet.*;
import com.datatorrent.bufferserver.storage.Storage;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.common.util.VarInt;
import com.datatorrent.netlet.AbstractLengthPrependerClient;
import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.EventLoop;
import com.datatorrent.netlet.Listener.ServerListener;

/**
 * The buffer server application<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
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
    storageHelperExecutor = Executors.newSingleThreadExecutor(new NameableThreadFactory("StorageHelper"));
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
    }
    catch (InterruptedException ex) {
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
      }
      catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }

    this.eventloop = eventloop;
    return address;
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
    }
    else {
      port = 0;
    }

    DefaultEventLoop eventloop = new DefaultEventLoop("alone");
    eventloop.start(null, port, new Server(port));
    new Thread(eventloop).start();
  }

  @Override
  public String toString()
  {
    return identity;
  }

  private final HashMap<String, DataList> publisherBuffers = new HashMap<String, DataList>();
  private final ConcurrentHashMap<String, LogicalNode> subscriberGroups = new ConcurrentHashMap<String, LogicalNode>();
  private final ConcurrentHashMap<String, AbstractLengthPrependerClient> publisherChannels = new ConcurrentHashMap<String, AbstractLengthPrependerClient>();
  private final ConcurrentHashMap<String, AbstractLengthPrependerClient> subscriberChannels = new ConcurrentHashMap<String, AbstractLengthPrependerClient>();
  private final int blockSize;
  private final int numberOfCacheBlocks;

  public void handlePurgeRequest(PurgeRequestTuple request, final AbstractLengthPrependerClient ctx) throws IOException
  {
    DataList dl;
    dl = publisherBuffers.get(request.getIdentifier());

    byte[] message;
    if (dl == null) {
      message = ("Invalid identifier '" + request.getIdentifier() + "'").getBytes();
    }
    else {
      dl.purge(request.getBaseSeconds(), request.getWindowId());
      message = "Purge request sent for processing".getBytes();
    }

    final byte[] tuple = PayloadTuple.getSerializedTuple(0, message.length);
    System.arraycopy(message, 0, tuple, tuple.length - message.length, message.length);
    serverHelperExecutor.submit(new Runnable()
    {
      @Override
      public void run()
      {
        ctx.write(tuple);
        eventloop.disconnect(ctx);
      }

    });
  }

  private void handleResetRequest(ResetRequestTuple request, final AbstractLengthPrependerClient ctx) throws IOException
  {
    DataList dl;
    dl = publisherBuffers.remove(request.getIdentifier());

    byte[] message;
    if (dl == null) {
      message = ("Invalid identifier '" + request.getIdentifier() + "'").getBytes();
    }
    else {
      AbstractLengthPrependerClient channel = publisherChannels.remove(request.getIdentifier());
      if (channel != null) {
        eventloop.disconnect(channel);
      }
      dl.reset();
      message = "Reset request sent for processing".getBytes();
    }

    final byte[] tuple = PayloadTuple.getSerializedTuple(0, message.length);
    System.arraycopy(message, 0, tuple, tuple.length - message.length, message.length);
    serverHelperExecutor.submit(new Runnable()
    {
      @Override
      public void run()
      {
        ctx.write(tuple);
        eventloop.disconnect(ctx);
      }

    });
  }

  /**
   *
   * @param request
   * @param connection
   * @return
   */
  public LogicalNode handleSubscriberRequest(SubscribeRequestTuple request, AbstractLengthPrependerClient connection)
  {
    String identifier = request.getIdentifier();
    String type = request.getStreamType();
    String upstream_identifier = request.getUpstreamIdentifier();

    // Check if there is a logical node of this type, if not create it.
    LogicalNode ln;
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
      ln.boot(eventloop);
      ln.addConnection(connection);
    }
    else {
      /*
       * if there is already a datalist registered for the type in which this client is interested,
       * then get a iterator on the data items of that data list. If the datalist is not registered,
       * then create one and register it. Hopefully this one would be used by future upstream nodes.
       */
      DataList dl;
      if (publisherBuffers.containsKey(upstream_identifier)) {
        dl = publisherBuffers.get(upstream_identifier);
        //logger.debug("old list = {}", dl);
      }
      else {
        dl = Tuple.FAST_VERSION.equals(request.getVersion()) ? new FastDataList(upstream_identifier, blockSize, numberOfCacheBlocks) : new DataList(upstream_identifier, blockSize, numberOfCacheBlocks);
        publisherBuffers.put(upstream_identifier, dl);
        //logger.debug("new list = {}", dl);
      }

      long skipWindowId = (long)request.getBaseSeconds() << 32 | request.getWindowId();
      ln = new LogicalNode(upstream_identifier,
                           type,
                           dl.newIterator(identifier, skipWindowId),
                           skipWindowId);

      int mask = request.getMask();
      if (mask != 0) {
        for (Integer bs : request.getPartitions()) {
          ln.addPartition(bs, mask);
        }
      }

      subscriberGroups.put(type, ln);
      ln.addConnection(connection);
      dl.addDataListener(ln);
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
      }
      catch (IOException ie) {
        throw new RuntimeException(ie);
      }
    }
    else {
      dl = Tuple.FAST_VERSION.equals(request.getVersion()) ? new FastDataList(identifier, blockSize, numberOfCacheBlocks) : new DataList(identifier, blockSize, numberOfCacheBlocks);
      publisherBuffers.put(identifier, dl);
    }
    dl.setSecondaryStorage(storage, storageHelperExecutor);

    return dl;
  }

  @Override
  public ClientListener getClientConnection(SocketChannel sc, ServerSocketChannel ssc)
  {
    return new UnidentifiedClient(sc);
  }

  @Override
  public void handleException(Exception cce, EventLoop el)
  {
    if (cce instanceof RuntimeException) {
      throw (RuntimeException)cce;
    }

    throw new RuntimeException(cce);
  }

  class UnidentifiedClient extends AbstractLengthPrependerClient
  {
    SocketChannel channel;
    boolean ignore;

    UnidentifiedClient(SocketChannel channel)
    {
      this.channel = channel;
    }

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
          dl.setAutoflushExecutor(serverHelperExecutor);

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
          }
          else {
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
          if (subscriberRequest.getVersion().equals(Tuple.FAST_VERSION)) {
            subscriber = new Subscriber(subscriberRequest.getStreamType(), subscriberRequest.getMask(), subscriberRequest.getPartitions());
          }
          else {
            subscriber = new Subscriber(subscriberRequest.getStreamType(), subscriberRequest.getMask(), subscriberRequest.getPartitions())
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

          final LogicalNode logicalNode = handleSubscriberRequest(subscriberRequest, subscriber);
          serverHelperExecutor.submit(new Runnable()
          {
            @Override
            public void run()
            {
              logicalNode.catchUp();
            }

          });
          break;

        case PURGE_REQUEST:
          logger.info("Received purge request: {}", request);
          try {
            handlePurgeRequest((PurgeRequestTuple)request, this);
          }
          catch (IOException io) {
            throw new RuntimeException(io);
          }
          break;

        case RESET_REQUEST:
          logger.info("Received reset all request: {}", request);
          try {
            handleResetRequest((ResetRequestTuple)request, this);
          }
          catch (IOException io) {
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

    Subscriber(String type, int mask, int[] partitions)
    {
      super(1024, 16 * 1024);
      this.type = type;
      this.mask = mask;
      this.partitions = partitions;
      super.write = false;
    }

    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      logger.warn("Received data when no data is expected: {}", Arrays.toString(Arrays.copyOfRange(buffer, offset, offset + size)));
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
      return "Server.Subscriber{" + "type=" + type + ", mask=" + mask + ", partitions=" + (partitions == null ? "null" : Arrays.toString(partitions)) + '}';
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
            dl.delIterator(ln.getIterator());
          }
          subscriberGroups.remove(ln.getGroup());
        }
      }
    }

  }

  /**
   * When the publisher connects to the server and starts publishing the data,
   * this is the end on the server side which handles all the communication.
   *
   * @author Chetan Narsude <chetan@datatorrent.com>
   */
  class Publisher extends AbstractLengthPrependerClient
  {
    private final DataList datalist;
    boolean dirty;

    Publisher(DataList dl, long windowId)
    {
      super(dl.getBuffer(windowId), dl.getPosition(), 1024);
      this.datalist = dl;
    }

    public void transferBuffer(byte[] array, int offset, int len)
    {
      int remainingCapacity;
      do {
        remainingCapacity = buffer.length - writeOffset;
        if (len < remainingCapacity) {
          remainingCapacity = len;
          byteBuffer.position(writeOffset + remainingCapacity);
        }
        else {
          byteBuffer.position(buffer.length);
        }
        System.arraycopy(array, offset, buffer, writeOffset, remainingCapacity);
        read(remainingCapacity);

        offset += remainingCapacity;
      }
      while ((len -= remainingCapacity) > 0);
    }

    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      //if (buffer[offset] == MessageType.BEGIN_WINDOW_VALUE || buffer[offset] == MessageType.END_WINDOW_VALUE) {
      //  logger.debug("server received {}", Tuple.getTuple(buffer, offset, size));
      //}
      dirty = true;
    }

    @Override
    public void read(int len)
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
                  switchToNewBuffer(buffer, readOffset);
                }
              }
              else if (dirty) {
                dirty = false;
                datalist.flush(writeOffset);
              }
              return;

            case 0:
              continue;
          }
        }

        if (writeOffset - readOffset >= size) {
          onMessage(buffer, readOffset, size);
          readOffset += size;
          size = 0;
        }
        else {
          if (writeOffset == buffer.length) {
            dirty = false;
            datalist.flush(writeOffset);
            /*
             * hit wall while writing serialized data, so have to allocate a new byteBuffer.
             */
            switchToNewBuffer(buffer, readOffset - VarInt.getSize(size));
            size = 0;
          }
          else if (dirty) {
            dirty = false;
            datalist.flush(writeOffset);
          }
          return;
        }
      }
      while (true);
    }

    public void switchToNewBuffer(byte[] array, int offset)
    {
      byte[] newBuffer = new byte[datalist.getBlockSize()];
      byteBuffer = ByteBuffer.wrap(newBuffer);
      if (array == null || array.length - offset == 0) {
        writeOffset = 0;
      }
      else {
        writeOffset = array.length - offset;
        System.arraycopy(buffer, offset, newBuffer, 0, writeOffset);
        byteBuffer.position(writeOffset);
      }
      buffer = newBuffer;
      readOffset = 0;
      datalist.addBuffer(buffer);
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
      }
      else {
        super.handleException(cce, el);
      }
    }

    @Override
    public String toString()
    {
      return "Server.Publisher{" + "datalist=" + datalist + '}';
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
       * since the publisher server died, the queue which it was using would stop pumping the data unless a new publisher comes up with the same name. We leave
       * it to the stream to decide when to bring up a new node with the same identifier as the one which just died.
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

  private static final Logger logger = LoggerFactory.getLogger(Server.class);
}
