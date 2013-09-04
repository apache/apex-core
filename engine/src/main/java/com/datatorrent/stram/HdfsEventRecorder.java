/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.codec.JsonStreamCodec;
import com.datatorrent.api.util.PubSubWebSocketClient;
import com.datatorrent.common.util.Slice;
import com.datatorrent.stram.plan.logical.LogicalPlanRequest;
import com.datatorrent.stram.util.HdfsPartFileCollection;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import org.apache.commons.beanutils.BeanUtils;
import org.eclipse.jetty.websocket.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>HdfsEventRecorder class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.4
 */
public class HdfsEventRecorder implements EventRecorder
{
  public static final String VERSION = "1.0";
  private final BlockingQueue<Event> queue = new LinkedBlockingQueue<Event>();
  private static final Logger LOG = LoggerFactory.getLogger(HdfsEventRecorder.class);
  private HdfsPartFileCollection storage = new HdfsPartFileCollection();
  private String basePath = ".";
  private transient StreamCodec<Object> streamCodec;
  private URI pubSubUrl = null;
  private int numSubscribers = 0;
  private PubSubWebSocketClient wsClient;
  private String pubSubTopic;
  private String appid;

  private class EventRecorderThread extends Thread
  {
    @Override
    public void run()
    {
      while (true) {
        try {
          Event event = queue.take();
          writeEvent(event);
          if (queue.isEmpty()) {
            storage.flushData();
          }
        }
        catch (InterruptedException ex) {
          return;
        }
        catch (IOException ex) {
          LOG.error("Caught IOException", ex);
        }
      }
    }

  }

  public HdfsEventRecorder(String appid)
  {
    LOG.debug("Event recorder created for {}", appid);
    this.appid = appid;
  }

  public void setPubSubUrl(String pubSubUrl) throws URISyntaxException
  {
    this.pubSubUrl = new URI(pubSubUrl);
  }

  public void setBasePath(String basePath)
  {
    this.basePath = basePath;
  }

  public void setup()
  {
    try {
      streamCodec = new JsonStreamCodec<Object>();
      storage = new HdfsPartFileCollection();
      storage.setBasePath(basePath);
      storage.setup();
      storage.writeMetaData((VERSION + "\n").getBytes());

      if (pubSubUrl != null) {
        pubSubTopic = "eventRecorder." + appid;
        try {
          setupWsClient();
        }
        catch (Exception ex) {
          LOG.error("Cannot connect to daemon at {}", pubSubUrl);
        }
      }

      new EventRecorderThread().start();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void recordEventAsync(Event event)
  {
    LOG.debug("Adding event to the queue");
    queue.add(event);
  }

  public void writeEvent(Event event) throws IOException
  {
    LOG.debug("Writing event {} to the queue", event.getType());
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    bos.write((event.getTimestamp() + ":").getBytes());
    bos.write((event.getType() + ":").getBytes());
    Slice f = streamCodec.toByteArray(event.getData());
    bos.write(f.buffer, f.offset, f.length);
    bos.write("\n".getBytes());
    storage.writeDataItem(bos.toByteArray(), true);
    if (numSubscribers > 0) {
      wsClient.publish(pubSubTopic, event);
    }
  }

  private void setupWsClient() throws ExecutionException, IOException, InterruptedException, TimeoutException
  {
    wsClient = new PubSubWebSocketClient()
    {
      @Override
      public void onOpen(WebSocket.Connection connection)
      {
      }

      @Override
      public void onMessage(String type, String topic, Object data)
      {
        if (topic.equals(pubSubTopic + ".numSubscribers")) {
          numSubscribers = Integer.valueOf((String)data);
          LOG.info("Number of subscribers is now {}", numSubscribers);
        }
      }

      @Override
      public void onClose(int code, String message)
      {
        numSubscribers = 0;
      }

    };
    wsClient.setUri(pubSubUrl);
    wsClient.openConnection(500);
    wsClient.subscribeNumSubscribers(pubSubTopic);
  }

  public void requestSync()
  {
    this.storage.requestSync();
  }
}
