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
package com.datatorrent.stram;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.beanutils.BeanUtils;

import com.google.common.base.Throwables;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.codec.JsonStreamCodec;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.client.EventsAgent;
import com.datatorrent.stram.util.FSPartFileCollection;
import com.datatorrent.stram.util.SharedPubSubWebSocketClient;

import net.engio.mbassy.listener.Handler;

/**
 * <p>FSEventRecorder class.</p>
 *
 * @since 0.3.4
 */
public class FSEventRecorder implements EventRecorder
{
  public static final String VERSION = "1.0";
  private final BlockingQueue<StramEvent> queue = new LinkedBlockingQueue<>();
  private static final Logger LOG = LoggerFactory.getLogger(FSEventRecorder.class);
  private FSPartFileCollection storage;
  private String basePath = ".";
  private transient StreamCodec<Object> streamCodec;
  private final URI pubSubUrl = null;
  private int numSubscribers = 0;
  private SharedPubSubWebSocketClient wsClient;
  private final String pubSubTopic;
  private final EventRecorderThread eventRecorderThread = new EventRecorderThread();

  private class EventRecorderThread extends Thread
  {
    @Override
    @SuppressWarnings("CallToThreadYield")
    public void run()
    {
      while (true) {
        try {
          writeEvent(queue.take());
          yield();
          if (queue.isEmpty()) {
            if (!storage.flushData() && wsClient != null) {
              String topic = SharedPubSubWebSocketClient.LAST_INDEX_TOPIC_PREFIX + ".event." + storage.getBasePath();
              wsClient.publish(topic, storage.getLatestIndexLine());
            }
          }
        } catch (InterruptedException ex) {
          return;
        } catch (Exception ex) {
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
      streamCodec = new JsonStreamCodec<>();
      storage = new FSPartFileCollection();
      storage.setBasePath(basePath);
      storage.setup();
      storage.writeMetaData((VERSION + "\n").getBytes());

      if (wsClient != null) {
        try {
          setupWsClient();
        } catch (ExecutionException | IOException | InterruptedException | TimeoutException ex) {
          LOG.error("Cannot connect to gateway at {}", pubSubUrl);
        }
      }

      eventRecorderThread.start();
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  public void teardown()
  {
    eventRecorderThread.interrupt();
    try {
      eventRecorderThread.join();
    } catch (InterruptedException ex) {
      LOG.warn("Event recorder thread join interrupted");
    }
    if (storage != null) {
      storage.teardown();
    }
  }

  @Handler
  @Override
  public void recordEventAsync(StramEvent event)
  {
    LOG.debug("Adding event {} to the queue", event.getType());
    queue.add(event);
  }

  public void writeEvent(StramEvent event) throws Exception
  {
    LOG.debug("Writing event {} to the storage", event.getType());
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    bos.write((event.getTimestamp() + ":").getBytes());
    bos.write((event.getType() + ":").getBytes());
    @SuppressWarnings("unchecked")
    Map<String, String> data = BeanUtils.describe(event);
    data.remove("timestamp");
    data.remove("class");
    data.remove("type");
    Slice f = streamCodec.toByteArray(data);
    bos.write(f.buffer, f.offset, f.length);
    bos.write("\n".getBytes());
    storage.writeDataItem(bos.toByteArray(), true);
    if (numSubscribers > 0) {
      LOG.debug("Publishing event {} through websocket to gateway", event.getType());
      EventsAgent.EventInfo eventInfo = new EventsAgent.EventInfo();
      eventInfo.id = event.getId();
      eventInfo.timestamp = event.getTimestamp();
      eventInfo.type = event.getType();
      eventInfo.data = data;
      eventInfo.data.remove("id");
      wsClient.publish(pubSubTopic, eventInfo);
    }
  }

  private void setupWsClient() throws ExecutionException, IOException, InterruptedException, TimeoutException
  {
    wsClient.addHandler(pubSubTopic, true, new SharedPubSubWebSocketClient.Handler()
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
