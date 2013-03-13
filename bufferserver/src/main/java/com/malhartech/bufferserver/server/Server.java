/*
 * Copyright (c) 2012 Malhar, Inc.
 * All Rights Reserved.
 */
package com.malhartech.bufferserver.server;

import com.malhartech.bufferserver.internal.ProtobufDataInspector;
import com.malhartech.bufferserver.internal.LogicalNode;
import com.malhartech.bufferserver.internal.DataList;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;
import com.malhartech.bufferserver.Buffer;
import com.malhartech.bufferserver.Buffer.Message;
import com.malhartech.bufferserver.Buffer.Message.MessageType;
import static com.malhartech.bufferserver.Buffer.Message.MessageType.*;
import com.malhartech.bufferserver.Buffer.Payload;
import com.malhartech.bufferserver.Buffer.Request;
import com.malhartech.bufferserver.Buffer.SubscriberRequest;
import static com.malhartech.bufferserver.Buffer.SubscriberRequest.PolicyType.*;
import com.malhartech.bufferserver.client.VarIntLengthPrependerClient;
import com.malhartech.bufferserver.client.ProtoBufClient;
import com.malhartech.bufferserver.policy.*;
import com.malhartech.bufferserver.storage.Storage;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashMap;
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
  public static final int DEFAULT_PORT = 9080;
  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024 * 1024;
  public static final int DEFAULT_BLOCK_COUNT = 8;
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
    this(port, DEFAULT_BUFFER_SIZE, DEFAULT_BLOCK_COUNT);
  }

  public Server(int port, int blocksize, int blockcount)
  {
    this.port = port;
    this.blockSize = blocksize;
    this.blockCount = blockcount;
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
      port = DEFAULT_PORT;
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
  static final ExtensionRegistry registry = ExtensionRegistry.newInstance();

  static {
    Buffer.registerAllExtensions(registry);
  }

  private final HashMap<String, DataList> publisherBufffers = new HashMap<String, DataList>();
  private final HashMap<String, LogicalNode> subscriberGroups = new HashMap<String, LogicalNode>();
  private final ConcurrentHashMap<String, VarIntLengthPrependerClient> publisherChannels = new ConcurrentHashMap<String, VarIntLengthPrependerClient>();
  private final ConcurrentHashMap<String, VarIntLengthPrependerClient> subscriberChannels = new ConcurrentHashMap<String, VarIntLengthPrependerClient>();
  private final int blockSize;
  private final int blockCount;

  /**
   *
   * @param policytype
   * @param type
   * @return Policy
   */
  public Policy getPolicy(Buffer.SubscriberRequest.PolicyType policytype, String type)
  {
    Policy p = null;

    switch (policytype) {
      case CUSTOM:
        try {
          Class<?> customclass = Class.forName(type);
          p = (Policy)customclass.newInstance();
        }
        catch (InstantiationException ex) {
          throw new RuntimeException(ex);
        }
        catch (IllegalAccessException ex) {
          throw new RuntimeException(ex);
        }
        catch (ClassNotFoundException ex) {
          throw new RuntimeException(ex);
        }
        break;

      case GIVE_ALL:
        p = GiveAll.getInstance();
        break;

      case LEAST_BUSY:
        p = LeastBusy.getInstance();
        break;

      case RANDOM_ONE:
        p = RandomOne.getInstance();
        break;

      case ROUND_ROBIN:
        p = new RoundRobin();
        break;
    }

    return p;
  }

  private synchronized void handleResetRequest(Buffer.Request request, VarIntLengthPrependerClient ctx) throws IOException
  {
    DataList dl;
    dl = publisherBufffers.remove(request.getIdentifier());

    Payload.Builder sdb = Payload.newBuilder();
    sdb.setPartition(0);
    if (dl == null) {
      sdb.setData(ByteString.copyFromUtf8("Invalid identifier '" + request.getIdentifier() + "'"));
    }
    else {
      VarIntLengthPrependerClient channel = publisherChannels.remove(request.getIdentifier());
      if (channel != null) {
        eventloop.disconnect(channel);
        // how do we wait here?
      }
      dl.reset();
      sdb.setData(ByteString.copyFromUtf8("Reset request sent for processing"));
    }

    Message.Builder db = Message.newBuilder();
    db.setType(MessageType.PAYLOAD);
    db.setPayload(sdb);

    ctx.write(db.build().toByteArray());
    eventloop.disconnect(ctx);
  }

  /**
   *
   * @param request
   * @param connection
   * @return
   */
  public LogicalNode handleSubscriberRequest(Buffer.Request request, VarIntLengthPrependerClient connection)
  {
    SubscriberRequest subscriberRequest = request.getExtension(SubscriberRequest.request);
    String identifier = request.getIdentifier();
    String type = subscriberRequest.getType();
    String upstream_identifier = subscriberRequest.getUpstreamIdentifier();
    //String upstream_type = request.getUpstreamType();

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
                           dl.newIterator(identifier, new ProtobufDataInspector(), request.getWindowId()),
                           getPolicy(subscriberRequest.getPolicy(), null),
                           (long)request.getBaseSeconds() << 32 | request.getWindowId());

      int mask = subscriberRequest.getPartitions().getMask();
      if (subscriberRequest.getPartitions().getPartitionCount() > 0) {
        for (Integer bs : subscriberRequest.getPartitions().getPartitionList()) {
          ln.addPartition(bs, mask);
        }
      }

      subscriberGroups.put(type, ln);
      ln.addConnection(connection);
      dl.addDataListener(ln);
    }

    return ln;
  }

  public void handlePurgeRequest(Buffer.Request request, VarIntLengthPrependerClient ctx) throws IOException
  {
    DataList dl;
    dl = publisherBufffers.get(request.getIdentifier());

    Payload.Builder sdb = Payload.newBuilder();
    sdb.setPartition(0);
    if (dl == null) {
      sdb.setData(ByteString.copyFromUtf8("Invalid identifier '" + request.getIdentifier() + "'"));
    }
    else {
      dl.purge(request.getBaseSeconds(), request.getWindowId(), new ProtobufDataInspector());
      sdb.setData(ByteString.copyFromUtf8("Purge request sent for processing"));
    }

    Message.Builder db = Message.newBuilder();
    db.setType(MessageType.PAYLOAD);
    db.setPayload(sdb);

    ctx.write(db.build().toByteArray());
    eventloop.disconnect(ctx);
  }

  /**
   *
   * @param request
   * @param connection
   * @return
   */
  public DataList handlePublisherRequest(Buffer.Request request, VarIntLengthPrependerClient connection)
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

  class UnidentifiedClient extends ProtoBufClient
  {
    SocketChannel channel;
    boolean ignore;

    UnidentifiedClient(SocketChannel channel)
    {
      this.channel = channel;
    }

    @Override
    public void onMessage(Message message)
    {
      if (ignore) {
        return;
      }

      Request request = message.getRequest();
      switch (message.getType()) {
        case PUBLISHER_REQUEST:
          logger.info("Received publisher request: {}", request);

          DataList dl = handlePublisherRequest(request, this);
          try {
            dl.rewind(request.getBaseSeconds(), request.getWindowId(), new ProtobufDataInspector());
          }
          catch (IOException ie) {
            logger.debug("exception while rewiding", ie);
          }

          unregistered(key);
          Publisher publisher = new Publisher(dl);
          publisher.registered(key);
          publisher.transferBuffer(readBuffer, readOffset + size, writeOffset - readOffset - size);

          logger.debug("registering the channel for read operation {}", publisher);
          key.attach(publisher);
          key.interestOps(SelectionKey.OP_READ);
          ignore = true;
          break;

        case SUBSCRIBER_REQUEST:
          logger.info("Received subscriber request: {}", request);
          boolean contains = subscriberGroups.containsKey(request.getExtension(Buffer.SubscriberRequest.request).getType());

          unregistered(key);
          VarIntLengthPrependerClient newSubscriber = new Subscriber();
          newSubscriber.registered(key);

          key.attach(newSubscriber);
          key.interestOps(SelectionKey.OP_READ);
          ignore = true;

          LogicalNode ln = handleSubscriberRequest(request, newSubscriber);
          if (!contains) {
            ln.catchUp();
          }
          break;

        case PURGE_REQUEST:
          logger.info("Received purge request: {}", request);
          try {
            handlePurgeRequest(request, this);
          }
          catch (IOException io) {
            throw new RuntimeException(io);
          }
          break;

        case RESET_REQUEST:
          logger.info("Received purge all request: {}", request);
          try {
            handleResetRequest(request, this);
          }
          catch (IOException io) {
            throw new RuntimeException(io);
          }
          break;

        default:
          throw new RuntimeException("unexpected message");
      }

    }

  }

  class Subscriber extends VarIntLengthPrependerClient
  {
    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      logger.warn("Received data when no data is expected: {}", Arrays.toString(Arrays.copyOfRange(buffer, offset, offset + size)));
    }

  }

}
