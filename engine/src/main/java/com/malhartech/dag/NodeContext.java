/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class NodeContext implements Context
{
  private static Logger LOG = LoggerFactory.getLogger(NodeContext.class);
  
  private BackupAgent backupAgent;

  public static enum RequestType
  {
    UNDEFINED,
    REPORT,
    BACKUP,
    RESTORE,
    TERMINATE
  }

  public static class HeartbeatCounters
  {
    public volatile long tuplesProcessed;
    public volatile long bytesProcessed;
  }
  private String id;
  private long windowId;
  private volatile HeartbeatCounters heartbeatCounters = new HeartbeatCounters();
  private volatile RequestType request = RequestType.UNDEFINED;
  /**
   * The AbstractNode to which this context is passed, will timeout after the following milliseconds if no new tuple has been received by it.
   */
  // we should make it configurable somehow.
  private long idleTimeout = 1000L;

  /**
   * @return the requestType
   */
  public final RequestType getRequestType()
  {
    return request;
  }

  /**
   * @return the idleTimeout
   */
  public long getIdleTimeout()
  {
    return idleTimeout;
  }

  /**
   * @param idleTimeout the idleTimeout to set
   */
  public void setIdleTimeout(long idleTimeout)
  {
    this.idleTimeout = idleTimeout;
  }

  public NodeContext(String id)
  {
    this.id = id;
  }

  public String getId()
  {
    return id;
  }

  public long getCurrentWindowId()
  {
    return windowId;
  }

  public void setCurrentWindowId(long windowId)
  {
    this.windowId = windowId;
  }

  /**
   * Reset counts for next heartbeat interval and return current counts. This is called as part of the heartbeat processing.
   *
   * @return
   */
  public HeartbeatCounters resetHeartbeatCounters()
  {
    HeartbeatCounters counters = this.heartbeatCounters;
    this.heartbeatCounters = new HeartbeatCounters();
    if (request == RequestType.UNDEFINED) {
      request = RequestType.REPORT;
    }
    return counters;
  }

  synchronized void report(int consumedTupleCount)
  {
    this.heartbeatCounters.tuplesProcessed = consumedTupleCount;
    request = RequestType.UNDEFINED;
  }

  void backup(AbstractNode aThis) throws IOException
  {
    LOG.debug("Backup node={}, window={}", id, getCurrentWindowId());
    OutputStream os = backupAgent.borrowOutputStream(id, getCurrentWindowId());
    try {
      Kryo kryo = new Kryo();
      Output output = new Output(os);
      kryo.writeClassAndObject(output, aThis);
      output.flush();

      /*
       * we purposely do not close the stream here since it may close the underlying stream which we did not open. We do not want the foreign logic to have to
       * reopen the stream which may be inconvenient where as closing it is possible and easy when we return the stream back to the agent.
       */
      request = RequestType.UNDEFINED;
    }
    finally {
      backupAgent.returnOutputStream(os);
    }
  }

  public void requestBackup(BackupAgent agent)
  {
    this.backupAgent = agent;
    request = RequestType.BACKUP;
    LOG.debug("Received backup request (node={})", id);
  }

  public Object restore(BackupAgent agent, long windowId) throws IOException
  {
    return new Kryo().readClassAndObject(new Input(agent.getInputStream(id, windowId)));
  }
}
