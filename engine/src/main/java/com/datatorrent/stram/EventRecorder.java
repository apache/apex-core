/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.codec.JsonStreamCodec;
import com.datatorrent.common.util.Slice;
import com.datatorrent.stram.util.HdfsPartFileCollection;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class EventRecorder
{
  public static final String VERSION = "1.0";
  private final BlockingQueue<Event> queue = new LinkedBlockingQueue<Event>();
  private static final Logger LOG = LoggerFactory.getLogger(EventRecorder.class);
  private HdfsPartFileCollection storage = new HdfsPartFileCollection();
  private String basePath = ".";
  private transient StreamCodec<Object> streamCodec;

  public static class Event
  {
    private String type;
    private long timestamp = System.currentTimeMillis();
    private Map<String, Object> data = new HashMap<String, Object>();

    public Event(String type)
    {
      this.type = type;
    }

    public void setTimestamp(long timestamp)
    {
      this.timestamp = timestamp;
    }

    public String getType()
    {
      return type;
    }

    public void addData(String key, Object value)
    {
      data.put(key, value);
    }

    public Map<String, Object> getData()
    {
      return Collections.unmodifiableMap(data);
    }

    public long getTimestamp()
    {
      return timestamp;
    }

  }

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

  public void setBasePath(String basePath)
  {
    this.basePath = basePath;
  }

  public void setup()
  {
    try {
      streamCodec = new JsonStreamCodec<Object>();
      storage = new HdfsPartFileCollection();
      storage.setBasePath(basePath + "/containers");
      storage.setup();
      storage.writeMetaData((VERSION + "\n").getBytes());
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void start()
  {
    new EventRecorderThread().start();
  }

  public void recordEventAsync(Event event)
  {
    queue.add(event);
  }

  public void writeEvent(Event event) throws IOException
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Slice f = streamCodec.toByteArray(event);
    bos.write(f.buffer, f.offset, f.length);
    bos.write("\n".getBytes());
    storage.writeDataItem(bos.toByteArray(), true);
  }

}
