package com.googlecode.connectlet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Collection;

/**
 * The encapsulation of a {@link SocketChannel} and its {@link SelectionKey},
 * which corresponds to a TCP Socket.
 */
public class Connection
{
  private static final InetSocketAddress NULL_ADDRESS = new InetSocketAddress(0);
  protected ByteBuffer buffer;

  public Connection()
  {
    buffer = ByteBuffer.allocate(Connector.MAX_BUFFER_SIZE);
  }

  public Connection(byte[] byteArray)
  {
    buffer = ByteBuffer.wrap(byteArray);
  }

  public Connection(ByteBuffer buffer)
  {
    this.buffer = buffer;
  }

  public ByteBuffer getBuffer()
  {
    buffer.clear();
    return buffer;
  }

  public void onRecv(int bytesRead)
  {
    netFilter.onRecv(buffer.array(), buffer.position() - bytesRead, bytesRead);
  }

  public  SocketChannel getSocketChannel()
  {
    return socketChannel;
  }

  /**
   * This interface provides the way to consume sent data
   * in the network end of a connection.
   *
   * @see Connection#setWriter(Writer)
   * @see Connection#setWriter(Writer,
   * InetSocketAddress, InetSocketAddress, Collection, Collection)
   * @see Connection#getNetWriter()
   */
  public static interface Writer
  {
    /**
     * Consumes sent data in the network end of a connection.
     *
     * @return The number of bytes actually sent,
     * must equals to <b>len</b> in virtual connections.
     * @throws IOException If fail to send and the connection must close.
     * Note that IOException should not be thrown in virtual connections.
     */
    public int write(byte[] b, int off, int len) throws IOException;

  }

  /**
   * This interface provides the way to consume received data
   * and events (including connecting and disconnecting events)
   * in the network end of a connection.
   *
   * @see Connection#getNetReader()
   */
  public static interface Reader
  {
    /**
     * Consumes received data in the NETWORK end of the connection
     */
    public void onRecv(byte[] b, int off, int len);

    /**
     * Consumes events in the NETWORK end of the connection
     */
    public void onEvent(Event event);

    /**
     * Consumes connecting events in the NETWORK end of the connection
     */
    public void onConnect();

    /**
     * Consumes disconnected events in the NETWORK end of the connection
     */
    public void onDisconnect();

  }

  /**
   * A event which can be raised by {@link Connection#dispatchEvent(Event)}
   * and consumed by {@link Connection#onEvent(Event)}.
   */
  public class Event
  {
    /**
     * Reserved event type which indicates an active disconnecting
     */
    public static final int DISCONNECT = 0;
    /**
     * Reserved event type which indicates a queued sending
     */
    public static final int QUEUED = 1;
    /**
     * Reserved event type which indicates that sending is no longer queued
     */
    public static final int EMPTY = 2;
    private int type;

    /**
     * Creates an Event with a given type.<p>
     * A zero type means an active disconnecting, which will successively
     * be consumed by {@link Connection#onEvent(Event)} and
     * {@link Connection#onDisconnect()}.<p>
     * A negative type means a passive disconnecting or a socket error,
     * which will be consumed by {@link Connection#onDisconnect()} but
     * will NOT be consumed by {@link Connection#onEvent(Event)}.
     */
    protected Event(int type)
    {
      this.type = type;
    }

    /**
     * @return The type of the event.
     */
    public int getType()
    {
      return type;
    }

    /**
     * @return The connection where the event initially occurred.
     */
    public Connection getConnection()
    {
      return Connection.this;
    }

  }

  /**
   * Consumes received data in the application end of the connection.
   *
   * @param b
   * @param off
   * @param len
   */
  protected void onRecv(byte[] b, int off, int len)
  {/**/

  }

  /**
   * Consumes events in the APPLICATION end of the connection.
   *
   * @param event
   */
  protected void onEvent(Event event)
  {/**/

  }

  /**
   * Consumes connecting events in the APPLICATION end of the connection.
   */
  protected void onConnect()
  {/**/

  }

  /**
   * Consumes disconnecting events in the APPLICATION end of the connection.
   */
  protected void onDisconnect()
  {/**/

  }

  protected Filter netFilter = new Filter()
  {
    @Override
    protected void send(byte[] b, int off, int len)
    {
      write(b, off, len);
    }

  };
  private Filter appFilter = new Filter()
  {
    @Override
    protected void onRecv(byte[] b, int off, int len)
    {
      Connection.this.onRecv(b, off, len);
    }

    @Override
    protected void onEvent(Event event)
    {
      Connection.this.onEvent(event);
    }

    @Override
    protected void onConnect()
    {
      Connection.this.onConnect();
    }

    @Override
    protected void onDisconnect()
    {
      Connection.this.onDisconnect();
    }

  };
  private Filter lastFilter = appFilter;

  {
    netFilter.appFilter = appFilter;
    appFilter.netFilter = netFilter;
  }

  /**
   * Adds a {@link Filter} into the network end of the filter chain.
   *
   * @see Connector#getFilterFactories()
   * @see ServerConnection#getFilterFactories()
   */
  public void appendFilter(Filter filter)
  {
    filter.connection = this;
    // lastFilter <-> filter
    lastFilter.netFilter = filter;
    filter.appFilter = lastFilter;
    // filter <-> netFilter
    filter.netFilter = netFilter;
    netFilter.appFilter = filter;
    lastFilter = filter;
  }

  /**
   * Adds {@link Filter}s into the network end of the filter chain.
   * These {@link Filter}s are created by a list (or other collections) of {@link FilterFactory}s,
   * in a definite order (according to the iteration), from the application side to the network side.
   *
   * @param filterFactories - A collection of <b>FilterFactory</b>s.
   * @see Connector#getFilterFactories()
   * @see ServerConnection#getFilterFactories()
   */
  public void appendFilters(Iterable<FilterFactory> filterFactories)
  {
    for (FilterFactory filterFactory : filterFactories) {
      appendFilter(filterFactory.createFilter());
    }
  }

  // available to both virtual and non-virtual connections
  private Writer writer = new Writer()
  {
    @Override
    public int write(byte[] b, int off, int len) throws IOException
    {
      return socketChannel.write(ByteBuffer.wrap(b, off, len));
    }

  };
  private InetSocketAddress local, remote;
  Collection<Event> events, concurrentEvents;

  /**
   * Makes the connection virtual and write-only.
   *
   * @param writer - The writer which consumes sent data.
   */
  public void setWriter(Writer writer)
  {
    setWriter(writer, NULL_ADDRESS, NULL_ADDRESS, null, null);
  }

  /**
   * Makes the connection virtual.
   *
   * @param writer - The writer which consumes sent data.
   * @param local - Local IP address and port.
   * @param remote - Remote IP address and port.
   * @param events - A collection to consume events via {@link Collection#add(Object)}.
   * @param concurrentEvents - A collection to consume concurrent events
   * (raised by another thread) via {@link Collection#add(Object)}.
   * @see #getNetReader()
   */
  public void setWriter(Writer writer,
          InetSocketAddress local, InetSocketAddress remote,
          Collection<Event> events, Collection<Event> concurrentEvents)
  {
    this.writer = writer;
    this.local = local;
    this.remote = remote;
    this.events = events;
    this.concurrentEvents = concurrentEvents;
  }

  private Writer netWriter = null;
  private Reader netReader = null;

  /**
   * @return The network end of the writer where data can be sent directly.
   */
  public Writer getNetWriter()
  {
    if (netWriter == null) {
      netWriter = new Writer()
      {
        @Override
        public int write(byte[] b, int off, int len)
        {
          Connection.this.write(b, off, len);
          return len;
        }

      };
    }
    return netWriter;
  }

  /**
   * @return The network end of the reader from which data and events can be raised.
   */
  public Reader getNetReader()
  {
    if (netReader == null) {
      netReader = new Reader()
      {
        @Override
        public void onRecv(byte[] b, int off, int len)
        {
          netFilter.onRecv(b, off, len);
        }

        @Override
        public void onEvent(Event event)
        {
          netFilter.onEvent(event);
        }

        @Override
        public void onConnect()
        {
          netFilter.onConnect();
        }

        @Override
        public void onDisconnect()
        {
          netFilter.onDisconnect();
        }

      };
    }
    return netReader;
  }

  // the following fields and methods are available to non-virtual connections only
  protected SocketChannel socketChannel;
  protected SelectionKey selectionKey;
  private static final int STATUS_IDLE = 0;
  private static final int STATUS_BUSY = 1;
  private static final int STATUS_DISCONNECTING = 2;
  private static final int STATUS_CLOSED = 3;
  // always STATUS_IDLE for virtual connections
  private int status = STATUS_IDLE;
  // always empty for virtual connections
  private ByteArrayQueue queue = new ByteArrayQueue();

  void write() throws IOException
  {
    while (queue.length() > 0) {
      int bytesWritten = writer.write(queue.array(),
                                      queue.offset(), queue.length());
      if (bytesWritten == 0) {
        return;
      }
      queue.remove(bytesWritten);
    }
    if (status == STATUS_DISCONNECTING) {
      dispatchEvent(Event.DISCONNECT);
    }
    else {
      selectionKey.interestOps(SelectionKey.OP_READ);
      status = STATUS_IDLE;
      dispatchEvent(Event.EMPTY);
    }
  }
  // the above fields and methods are available to non-virtual connections only

  void write(byte[] b, int off, int len)
  {
    if (status != STATUS_IDLE) {
      queue.add(b, off, len);
      return;
    }
    int bytesWritten;
    try {
      bytesWritten = writer.write(b, off, len);
    }
    catch (IOException e) {
      disconnect();
      return;
    }
    // Should never reached in virtual connections
    if (bytesWritten < len) {
      queue.add(b, off + bytesWritten, len - bytesWritten);
      selectionKey.interestOps(SelectionKey.OP_WRITE);
      status = STATUS_BUSY;
      dispatchEvent(Event.QUEUED);
    }
  }

  void startConnect()
  {
    status = STATUS_BUSY;
  }

  void finishConnect()
  {
    local = ((InetSocketAddress)socketChannel.
            socket().getLocalSocketAddress());
    remote = ((InetSocketAddress)socketChannel.
            socket().getRemoteSocketAddress());
    netFilter.onConnect();
    // "onConnect()" might call "disconnect()"
    if (status == STATUS_IDLE || status == STATUS_CLOSED) {
      return;
    }
    if (queue.length() == 0) {
      if (status == STATUS_DISCONNECTING) {
        dispatchEvent(Event.DISCONNECT);
      }
      else {
        selectionKey.interestOps(SelectionKey.OP_READ);
        status = STATUS_IDLE;
      }
    }
    else {
      selectionKey.interestOps(SelectionKey.OP_WRITE);
      dispatchEvent(Event.QUEUED);
    }
  }

  /**
   * @return Local IP address of the Connection.
   */
  public String getLocalAddr()
  {
    return local.getAddress().getHostAddress();
  }

  /**
   * @return Local port of the Connection.
   */
  public int getLocalPort()
  {
    return local.getPort();
  }

  /**
   * @return Remote IP address of the Connection.
   */
  public String getRemoteAddr()
  {
    return remote.getAddress().getHostAddress();
  }

  /**
   * @return Remote port of the Connection.
   */
  public int getRemotePort()
  {
    return remote.getPort();
  }

  /**
   * Raises an event with a given type immediately.
   */
  public void dispatchEvent(int type)
  {
    dispatchEvent(type, false);
  }

  /**
   * Raises an event with a given type.
   *
   * @see #dispatchEvent(Event, boolean)
   */
  public void dispatchEvent(int type, boolean concurrent)
  {
    dispatchEvent(new Event(type), concurrent);
  }

  /**
   * Raises an event immediately.
   */
  public void dispatchEvent(Event event)
  {
    dispatchEvent(event, false);
  }

  /**
   * Raises an event.
   *
   * @param event - The event to be raised.
   * @param concurrent - Set <code>true</code> to add the event
   * in the event queue (only necessary when called in another thread),
   * and set <code>false</code> to consume the event immediately.
   */
  public void dispatchEvent(Event event, boolean concurrent)
  {
    (concurrent ? concurrentEvents : events).add(event);
  }

  /**
   * Closes the connection actively.<p>
   *
   * If
   * <code>send()</code> is called before
   * <code>disconnect()</code>,
   * the connection will not be closed until all queued data sent out.
   */
  public void disconnect()
  {
    if (status == STATUS_IDLE) {
      dispatchEvent(Event.DISCONNECT);
    }
    else if (status == STATUS_BUSY) {
      status = STATUS_DISCONNECTING;
    }
  }

  /**
   * @return <code>true</code> if the connection is connecting or sending data.
   */
  public boolean isBusy()
  {
    return status != STATUS_IDLE;
  }

  /**
   * @return <code>true</code> if the connection is not closed.
   */
  public boolean isOpen()
  {
    return status != STATUS_CLOSED;
  }

  /**
   * Sends a sequence of bytes in the application end,
   * equivalent to
   * <code>send(b, 0, b.length).</code>
   */
  public void send(byte[] b)
  {
    send(b, 0, b.length);
  }

  /**
   * Sends a sequence of bytes in the application end.
   */
  public void send(byte[] b, int off, int len)
  {
    appFilter.send(b, off, len);
  }

  void close()
  {
    status = STATUS_CLOSED;
    selectionKey.cancel();
    try {
      socketChannel.close();
    }
    catch (IOException e) {/**/

    }
  }

}