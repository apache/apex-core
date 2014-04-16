/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.codec.JsonStreamCodec;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.client.EventsAgent;
import com.datatorrent.stram.util.FSPartFileCollection;
import com.datatorrent.stram.util.SharedPubSubWebSocketClient;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.*;
import net.engio.mbassy.listener.Handler;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>FSEventRecorder class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.4
 */
public class FSEventRecorder implements EventRecorder
{
  public static final String VERSION = "1.0";
  private final BlockingQueue<StramEvent> queue = new LinkedBlockingQueue<StramEvent>();
  private static final Logger LOG = LoggerFactory.getLogger(FSEventRecorder.class);
  private FSPartFileCollection storage;
  private String basePath = ".";
  private transient StreamCodec<Object> streamCodec;
  private final URI pubSubUrl = null;
  private int numSubscribers = 0;
  private SharedPubSubWebSocketClient wsClient;
  private final String pubSubTopic;

  private class EventRecorderThread extends Thread
  {
    @Override
    public void run()
    {
      while (true) {
        try {
          writeEvent(queue.take());
          if (queue.isEmpty()) {
            if (!storage.flushData() && wsClient != null) {
              String topic = SharedPubSubWebSocketClient.LAST_INDEX_TOPIC_PREFIX + ".event." + storage.getBasePath();
              wsClient.publish(topic, storage.getLatestIndexLine());
            }
          }
        }
        catch (InterruptedException ex) {
          return;
        }
        catch (Exception ex) {
          LOG.error("Caught Exception", ex);
        }
      }
    }

  }

  public FSEventRecorder(String appid)
  {
    LOG.debug("Event recorder created for {}", appid);
    pubSubTopic = "applications." + appid + ".events";
  }

  public void setWebSocketClient(SharedPubSubWebSocketClient wsClient)
  {
    this.wsClient = wsClient;
  }

  public void setBasePath(String basePath)
  {
    this.basePath = basePath;
  }

  public void setup()
  {
    try {
      streamCodec = new JsonStreamCodec<Object>();
      storage = new FSPartFileCollection();
      storage.setBasePath(basePath);
      storage.setup();
      storage.writeMetaData((VERSION + "\n").getBytes());

      if (wsClient != null) {
        try {
          setupWsClient();
        }
        catch (Exception ex) {
          LOG.error("Cannot connect to gateway at {}", pubSubUrl);
        }
      }

      new EventRecorderThread().start();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Handler
  @Override
  public void recordEventAsync(StramEvent event)
  {
    LOG.debug("Adding event to the queue");
    queue.add(event);
  }

  public void writeEvent(StramEvent event) throws Exception
  {
    LOG.debug("Writing event {} to the queue", event.getType());
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    bos.write((event.getTimestamp() + ":").getBytes());
    bos.write((event.getType() + ":").getBytes());
    @SuppressWarnings("unchecked")
    Map<String, Object> data = BeanUtils.describe(event);
    data.remove("timestamp");
    data.remove("class");
    data.remove("type");
    Slice f = streamCodec.toByteArray(data);
    bos.write(f.buffer, f.offset, f.length);
    bos.write("\n".getBytes());
    storage.writeDataItem(bos.toByteArray(), true);
    if (numSubscribers > 0) {
      EventsAgent.EventInfo eventInfo = new EventsAgent.EventInfo();
      eventInfo.timestamp = event.getTimestamp();
      eventInfo.type = event.getType();
      eventInfo.data = data;
      wsClient.publish(pubSubTopic, eventInfo);
    }
  }

  private void setupWsClient() throws ExecutionException, IOException, InterruptedException, TimeoutException
  {
    wsClient.addHandler(pubSubTopic + ".numSubscribers", new SharedPubSubWebSocketClient.Handler()
    {
      @Override
      public void onMessage(String type, String topic, Object data)
      {
        numSubscribers = Integer.valueOf((String)data);
        LOG.info("Number of subscribers is now {}", numSubscribers);
      }

      @Override
      public void onClose()
      {
        numSubscribers = 0;
      }

    });

  }

  public void requestSync()
  {
    this.storage.requestSync();
  }

}
