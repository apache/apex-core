/*
 * Copyright (c) 2012 Malhar, Inc.
 * All Rights Reserved.
 */
package com.malhartech.bufferserver.netlet;

import com.malhartech.bufferserver.server.ServerHandler;
import com.malhartech.bufferserver.server.ProtobufDataInspector;
import com.malhartech.bufferserver.server.LogicalNode;
import com.malhartech.bufferserver.server.DataList;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.malhartech.bufferserver.Buffer;
import com.malhartech.bufferserver.Buffer.Message;
import com.malhartech.bufferserver.Buffer.Message.MessageType;
import static com.malhartech.bufferserver.Buffer.Message.MessageType.*;
import com.malhartech.bufferserver.Buffer.Payload;
import com.malhartech.bufferserver.Buffer.Request;
import com.malhartech.bufferserver.Buffer.SubscriberRequest;
import static com.malhartech.bufferserver.Buffer.SubscriberRequest.PolicyType.*;
import com.malhartech.bufferserver.client.Client;
import com.malhartech.bufferserver.policy.*;
import com.malhartech.bufferserver.storage.Storage;
import com.malhartech.bufferserver.util.SerializedData;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import malhar.netlet.EventLoop;
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
  EventLoop eventloop;

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
    try {
      eventloop = new EventLoop("server");
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public void setSpoolStorage(Storage storage)
  {
    this.storage = storage;
  }

  @Override
  public void started(SelectionKey key)
  {
    logger.info("server started listening at {}", key.channel());
  }

  @Override
  public void stopped(SelectionKey key)
  {
    logger.info("Server stopped listening at {}", key.channel());
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

    EventLoop eventloop = new EventLoop("alone");
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
  private final ConcurrentHashMap<String, Client> publisherChannels = new ConcurrentHashMap<String, Client>();
  private final ConcurrentHashMap<String, Client> subscriberChannels = new ConcurrentHashMap<String, Client>();
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
          java.util.logging.Logger.getLogger(ServerHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        catch (IllegalAccessException ex) {
          java.util.logging.Logger.getLogger(ServerHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        catch (ClassNotFoundException ex) {
          java.util.logging.Logger.getLogger(ServerHandler.class.getName()).log(Level.SEVERE, null, ex);
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

  private synchronized void handleResetRequest(Buffer.Request request, Client ctx) throws IOException
  {
    DataList dl;
    dl = publisherBufffers.remove(request.getIdentifier());

    Payload.Builder sdb = Payload.newBuilder();
    sdb.setPartition(0);
    if (dl == null) {
      sdb.setData(ByteString.copyFromUtf8("Invalid identifier '" + request.getIdentifier() + "'"));
    }
    else {
      Client channel = publisherChannels.remove(request.getIdentifier());
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
  public LogicalNode handleSubscriberRequest(Buffer.Request request, Client connection)
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
      Client previous = subscriberChannels.put(identifier, connection);
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
        dl = new DataList(upstream_identifier, blockSize, 8);
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

  public void handlePurgeRequest(Buffer.Request request, Client ctx) throws IOException
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
  public DataList handlePublisherRequest(Buffer.Request request, Client connection)
  {
    String identifier = request.getIdentifier();

    DataList dl;

    if (publisherBufffers.containsKey(identifier)) {
      /*
       * close previous connection with the same identifier which is guaranteed to be unique.
       */
      Client previous = publisherChannels.put(identifier, connection);
      if (previous != null) {
        eventloop.disconnect(previous);
      }

      dl = publisherBufffers.get(identifier);
    }
    else {
      dl = new DataList(identifier, blockSize, 8);
      publisherBufffers.put(identifier, dl);
    }
    dl.setSecondaryStorage(storage);

    return dl;
  }

  class UnidentifiedClient extends Client
  {
    public void onMessage(Message message) throws IOException
    {
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

          Bootstrap.connectors.get("Server").replace(this, SelectionKey.OP_READ, new PublisherConnection(dl));
          break;

        case SUBSCRIBER_REQUEST:
          logger.info("Received subscriber request: {}", request);
          boolean contains = subscriberGroups.containsKey(request.getExtension(Buffer.SubscriberRequest.request).getType());
          LogicalNode ln = handleSubscriberRequest(request, this);
          if (!contains) {
            ln.catchUp();
          }
          Bootstrap.connectors.get("Server").replace(this, SelectionKey.OP_WRITE, new SubscriberConnection());
          break;

        case PURGE_REQUEST:
          logger.info("Received purge request: {}", request);
          handlePurgeRequest(request, this);
          break;

        case RESET_REQUEST:
          logger.info("Received purge all request: {}", request);
          handleResetRequest(request, this);
          break;

        default:
          throw new RuntimeException("unexpected message");
      }

    }
    // this is crippled implementation since we really do not need access
    // to byte array and the offset within the byte array as we track it
    // separately in this class.

    protected void onRecv(byte[] b, int off, int len)
    {
      writeOffset += len;
      while (size == 0) {
        size = readVarInt();
        if (size == -1) {
          if (writeOffset == readBuffer.length) {
            if (readOffset > writeOffset - 5) {
              logger.info("hit the boundary while reading varint!");
              /*
               * we may be reading partial varint, adjust the buffers so that we have enough space to read the full data.
               */
              System.arraycopy(readBuffer, readOffset, readBuffer, 0, writeOffset - readOffset);
              writeOffset -= readOffset;
              readOffset = 0;
              buffer.clear();
              buffer.position(writeOffset);
            }
          }
          else {
            return;
          }
        }
      }

      if (size > 0) {
        if (writeOffset - readOffset >= size) {
          try {
            onMessage(Message.newBuilder().mergeFrom(readBuffer, readOffset, size, registry).build());
          }
          catch (InvalidProtocolBufferException ex) {
            logger.debug("parse error", ex);
            throw new RuntimeException(ex);
          }
          catch (IOException ex) {
            logger.debug("IO exception", ex);
            throw new RuntimeException(ex);
          }
          readOffset += size;
        }
        else if (writeOffset == readBuffer.length) {
          if (size > readBuffer.length) {
            int newsize = readBuffer.length;
            while (newsize < size) {
              newsize <<= 1;
            }
            logger.info("resizing buffer to size {} from size {}", newsize, readBuffer.length);
            byte[] newArray = new byte[newsize];
            System.arraycopy(readBuffer, readOffset, newArray, 0, writeOffset - readOffset);
            writeOffset -= readOffset;
            readOffset = 0;
            buffer = ByteBuffer.wrap(newArray);
            buffer.position(writeOffset);
          }
          else {
            System.arraycopy(readBuffer, readOffset, readBuffer, 0, writeOffset - readOffset);
            writeOffset -= readOffset;
            readOffset = 0;
            buffer.clear();
            buffer.position(writeOffset);
          }
        }
        /* else need to read more */
      }
    }

  }

  class PublisherConnection extends Client
  {
    int size = 0;
    byte[] readBuffer = new byte[64 * 1024 * 1024];

    PublisherConnection(DataList dl)
    {
      super(dl.getDataArray().array());
    }

    @Override
    protected void onEvent(Event event)
    {
      super.onEvent(event); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void onDisconnect()
    {
      super.onDisconnect(); //To change body of generated methods, choose Tools | Templates.
    }

  }

  class SubscriberConnection extends Client
  {
    /*
     * readBuffer to receive the incoming data, if this is publisher, connection then the data gets replaced with the huge readBuffer.
     */
    int size = 0;
    byte[] readBuffer = new byte[4096];
    int readOffset;
    int writeOffset;

    @Override
    public ByteBuffer getBuffer()
    {
      return super.buffer;
    }


  }

}
