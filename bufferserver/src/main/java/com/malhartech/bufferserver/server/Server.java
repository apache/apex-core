/*
 * Copyright (c) 2012 Malhar, Inc.
 * All Rights Reserved.
 */
package com.malhartech.bufferserver.server;

import com.malhartech.bufferserver.internal.LogicalNode;
import com.malhartech.bufferserver.internal.DataList;
import com.malhartech.bufferserver.client.VarIntLengthPrependerClient;
import com.malhartech.bufferserver.packet.*;
import com.malhartech.bufferserver.storage.Storage;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import malhar.netlet.DefaultEventLoop;
import malhar.netlet.Listener.ServerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The readBuffer server application<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Server implements ServerListener
{
  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024 * 1024;
  private final int port;
  private String identity;
  private Storage storage;
  DefaultEventLoop eventloop;
  private InetSocketAddress address;

  /**
   * @param port - port number to bind to or 0 to auto select a free port
   */
  public Server(int port)
  {
    this(port, DEFAULT_BUFFER_SIZE);
  }

  public Server(int port, int blocksize)
  {
    this.port = port;
    this.blockSize = blocksize;
  }

  public void setSpoolStorage(Storage storage)
  {
    this.storage = storage;
  }

  @Override
  public synchronized void registered(SelectionKey key)
  {
    ServerSocketChannel channel = (ServerSocketChannel)key.channel();
    logger.info("server started listening at {}", channel);
    address = (InetSocketAddress)channel.socket().getLocalSocketAddress();
    notifyAll();
  }

  @Override
  public void unregistered(SelectionKey key)
  {
    logger.info("Server stopped listening at {}", key.channel());
  }

  public synchronized InetSocketAddress run(DefaultEventLoop eventloop)
  {
    eventloop.start(null, port, this);
    try {
      wait();
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
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

  private static final Logger logger = LoggerFactory.getLogger(Server.class);
  private final HashMap<String, DataList> publisherBufffers = new HashMap<String, DataList>();
  private final HashMap<String, LogicalNode> subscriberGroups = new HashMap<String, LogicalNode>();
  private final ConcurrentHashMap<String, VarIntLengthPrependerClient> publisherChannels = new ConcurrentHashMap<String, VarIntLengthPrependerClient>();
  private final ConcurrentHashMap<String, VarIntLengthPrependerClient> subscriberChannels = new ConcurrentHashMap<String, VarIntLengthPrependerClient>();
  private final int blockSize;

//  /**
//   *
//   * @param policytype
//   * @param type
//   * @return Policy
//   */
//  public Policy getPolicy(Buffer.SubscriberRequest.PolicyType policytype, String type)
//  {
//    Policy p = null;
//
//    switch (policytype) {
//      case CUSTOM:
//        try {
//          Class<?> customclass = Class.forName(type);
//          p = (Policy)customclass.newInstance();
//        }
//        catch (InstantiationException ex) {
//          throw new RuntimeException(ex);
//        }
//        catch (IllegalAccessException ex) {
//          throw new RuntimeException(ex);
//        }
//        catch (ClassNotFoundException ex) {
//          throw new RuntimeException(ex);
//        }
//        break;
//
//      case GIVE_ALL:
//        p = GiveAll.getInstance();
//        break;
//
//      case LEAST_BUSY:
//        p = LeastBusy.getInstance();
//        break;
//
//      case RANDOM_ONE:
//        p = RandomOne.getInstance();
//        break;
//
//      case ROUND_ROBIN:
//        p = new RoundRobin();
//        break;
//    }
//
//    return p;
//  }
  private void handleResetRequest(ResetRequestTuple request, VarIntLengthPrependerClient ctx) throws IOException
  {
    DataList dl;
    dl = publisherBufffers.remove(request.getIdentifier());

    byte[] message;
    if (dl == null) {
      message = ("Invalid identifier '" + request.getIdentifier() + "'").getBytes();
    }
    else {
      VarIntLengthPrependerClient channel = publisherChannels.remove(request.getIdentifier());
      if (channel != null) {
        eventloop.disconnect(channel);
        // how do we wait here?
      }
      dl.reset();
      message = "Reset request sent for processing".getBytes();
    }

    byte[] tuple = PayloadTuple.getSerializedTuple(0, message.length);
    System.arraycopy(message, 0, tuple, tuple.length - message.length, message.length);
    ctx.write(tuple);
    eventloop.disconnect(ctx);
  }

  /**
   *
   * @param request
   * @param connection
   * @return
   */
  public LogicalNode handleSubscriberRequest(SubscribeRequestTuple request, VarIntLengthPrependerClient connection)
  {
    String identifier = request.getIdentifier();
    String type = request.getUpstreamType();
    String upstream_identifier = request.getUpstreamIdentifier();

    // Check if there is a logical node of this type, if not create it.
    LogicalNode ln;
    if (subscriberGroups.containsKey(type)) {
      /*
       * close previous connection with the same identifier which is guaranteed to be unique.
       */
      VarIntLengthPrependerClient previous = subscriberChannels.put(identifier, connection);
      if (previous != null) {
        eventloop.disconnect(previous);
      }

      ln = subscriberGroups.get(type);
      ln.addConnection(connection);
    }
    else {
      /*
       * if there is already a datalist registered for the type in which this client is interested,
       * then get a iterator on the data items of that data list. If the datalist is not registered,
       * then create one and register it. Hopefully this one would be used by future upstream nodes.
       */
      DataList dl;
      if (publisherBufffers.containsKey(upstream_identifier)) {
        dl = publisherBufffers.get(upstream_identifier);
      }
      else {
        dl = new DataList(upstream_identifier, blockSize);
        publisherBufffers.put(upstream_identifier, dl);
      }

      ln = new LogicalNode(upstream_identifier,
                           type,
                           dl.newIterator(identifier, request.getWindowId()),
                           (long)request.getBaseSeconds() << 32 | request.getWindowId());

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

  public void handlePurgeRequest(PurgeRequestTuple request, VarIntLengthPrependerClient ctx) throws IOException
  {
    DataList dl;
    dl = publisherBufffers.get(request.getIdentifier());

    byte[] message;
    if (dl == null) {
      message = ("Invalid identifier '" + request.getIdentifier() + "'").getBytes();
    }
    else {
      dl.purge(request.getBaseSeconds(), request.getWindowId());
      message = "Purge request sent for processing".getBytes();
    }

    byte[] tuple = PayloadTuple.getSerializedTuple(0, message.length);
    System.arraycopy(message, 0, tuple, tuple.length - message.length, message.length);
    ctx.write(tuple);
    eventloop.disconnect(ctx);
  }

  /**
   *
   * @param request
   * @param connection
   * @return
   */
  public DataList handlePublisherRequest(PublishRequestTuple request, VarIntLengthPrependerClient connection)
  {
    String identifier = request.getIdentifier();

    DataList dl;

    if (publisherBufffers.containsKey(identifier)) {
      /*
       * close previous connection with the same identifier which is guaranteed to be unique.
       */
      VarIntLengthPrependerClient previous = publisherChannels.put(identifier, connection);
      if (previous != null) {
        eventloop.disconnect(previous);
      }

      dl = publisherBufffers.get(identifier);
      try {
        dl.rewind(request.getBaseSeconds(), request.getWindowId());
      }
      catch (IOException ie) {
        throw new RuntimeException(ie);
      }
    }
    else {
      dl = new DataList(identifier, blockSize);
      publisherBufffers.put(identifier, dl);
    }
    dl.setSecondaryStorage(storage);

    return dl;
  }

  @Override
  public ClientListener getClientConnection(SocketChannel sc, ServerSocketChannel ssc)
  {
    return new UnidentifiedClient(sc);
  }

  @Override
  public void handleException(Exception cce, DefaultEventLoop el)
  {
    if (cce instanceof RuntimeException) {
      throw (RuntimeException)cce;
    }

    throw new RuntimeException(cce);
  }

  class UnidentifiedClient extends VarIntLengthPrependerClient
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

          DataList dl = handlePublisherRequest((PublishRequestTuple)request, this);

          Publisher publisher = new Publisher(dl);
          key.attach(publisher);
          key.interestOps(SelectionKey.OP_READ);
          logger.debug("registering the channel for read operation {}", publisher);
          publisher.registered(key);

          int len = writeOffset - readOffset - size;
          if (len > 0) {
            publisher.transferBuffer(readBuffer, readOffset + size, len);
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
          VarIntLengthPrependerClient subscriber = new Subscriber(subscriberRequest.getUpstreamType());
          key.attach(subscriber);
          key.interestOps(SelectionKey.OP_WRITE);
          logger.debug("registering the channel for write operation {}", subscriber);
          subscriber.registered(key);

          boolean need2cathcup = !subscriberGroups.containsKey(subscriberRequest.getUpstreamType());
          LogicalNode ln = handleSubscriberRequest(subscriberRequest, subscriber);
          if (need2cathcup) {
            ln.catchUp();
          }
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
          logger.info("Received purge all request: {}", request);
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

    @Override
    public void handleException(Exception cce, DefaultEventLoop el)
    {
      el.disconnect(this);
    }

  }

  class Subscriber extends VarIntLengthPrependerClient
  {
    private final String type;

    Subscriber(String type)
    {
      this.type = type;
      write = false;
    }

    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      logger.warn("Received data when no data is expected: {}", Arrays.toString(Arrays.copyOfRange(buffer, offset, offset + size)));
    }

    @Override
    public void handleException(Exception cce, DefaultEventLoop el)
    {
      LogicalNode ln = subscriberGroups.get(type);
      if (ln != null) {
        if (subscriberChannels.containsValue(this)) {
          final Iterator<Entry<String, VarIntLengthPrependerClient>> i = subscriberChannels.entrySet().iterator();
          while (i.hasNext()) {
            if (i.next().getValue() == this) {
              i.remove();
              break;
            }
          }
        }

        ln.removeChannel(this);
        if (ln.getPhysicalNodeCount() == 0) {
          DataList dl = publisherBufffers.get(ln.getUpstream());
          if (dl != null) {
            dl.removeDataListener(ln);
            dl.delIterator(ln.getIterator());
          }
          subscriberGroups.remove(ln.getGroup());
        }
      }

      el.disconnect(this);
    }

    @Override
    public String toString()
    {
      return "subscriber";
    }

  }

  /**
   * When the publisher connects to the server and starts publishing the data,
   * this is the end on the server side which handles all the communication.
   *
   * @author Chetan Narsude <chetan@malhar-inc.com>
   */
  class Publisher extends VarIntLengthPrependerClient
  {
    private final DataList datalist;
    boolean dirty;

    Publisher(DataList dl)
    {
      super(dl.getBufer(), dl.getPosition());
      this.datalist = dl;
    }

    public void transferBuffer(byte[] array, int offset, int len)
    {
      System.arraycopy(array, offset, readBuffer, writeOffset, len);
      buffer.position(writeOffset + len);
      read(len);
    }

    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      dirty = true;
    }

    @Override
    public void read(int len)
    {
      //logger.debug("read {} bytes", len);
      writeOffset += len;
      do {
        if (size <= 0) {
          switch (size = readVarInt()) {
            case -1:
              if (writeOffset == readBuffer.length) {
                if (readOffset > writeOffset - 5) {
                  /*
                   * if the data is not corrupt, we are limited by space to receive full varint.
                   * so we allocate a new buffer and copy over the partially written data to the
                   * new buffer and start as if we always had full room but not enough data.
                   */
                  logger.info("hit the boundary while reading varint!");
                  switchToNewBuffer(readBuffer, readOffset);
                }
              }

              if (dirty) {
                dirty = false;
                datalist.flush(writeOffset);
              }
              return;

            case 0:
              continue;
          }
        }

        if (writeOffset - readOffset >= size) {
          onMessage(readBuffer, readOffset, size);
          readOffset += size;
          size = 0;
        }
        else if (writeOffset == readBuffer.length) {
          /*
           * hit wall while writing serialized data, so have to allocate a new buffer.
           */
          switchToNewBuffer(null, 0);
        }
        else {
          if (dirty) {
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
      buffer = ByteBuffer.wrap(newBuffer);
      if (array == null || array.length - offset == 0) {
        writeOffset = 0;
      }
      else {
        writeOffset = array.length - offset;
        System.arraycopy(readBuffer, offset, newBuffer, 0, writeOffset);
        buffer.position(writeOffset);
      }
      readBuffer = newBuffer;
      readOffset = 0;
      datalist.addBuffer(readBuffer);
    }

    @Override
    public void unregistered(SelectionKey key)
    {
      /*
       * if the publisher unregistered, all the downstream guys are going to be unregistered anyways
       * in our world. So it makes sense to kick them out proactively. Otherwise these clients since
       * are not being written to, just stick around till the next publisher shows up and eat into
       * the data it's publishing for the new subscribers.
       */
      super.unregistered(key);
      String publisherIdentifier = datalist.getIdentifier();

      Iterator<LogicalNode> iterator = subscriberGroups.values().iterator();
      while (iterator.hasNext()) {
        LogicalNode ln = iterator.next();
        if (publisherIdentifier.equals(ln.getUpstream())) {
          ln.boot(eventloop);
          iterator.remove();
        }
      }
    }

    @Override
    public void handleException(Exception cce, DefaultEventLoop el)
    {
      /**
       * since the publisher server died, the queue which it was using would stop pumping the data unless a new publisher comes up with the same name. We leave
       * it to the stream to decide when to bring up a new node with the same identifier as the one which just died.
       */
      if (publisherChannels.containsValue(this)) {
        final Iterator<Entry<String, VarIntLengthPrependerClient>> i = publisherChannels.entrySet().iterator();
        while (i.hasNext()) {
          if (i.next().getValue() == this) {
            i.remove();
            break;
          }
        }
      }

      el.disconnect(this);
    }

  }

}
