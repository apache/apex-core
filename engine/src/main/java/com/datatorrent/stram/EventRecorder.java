/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.stram.util.HdfsPartFileCollection;
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

  public static class Event
  {
    private String type;
    private long timestamp = System.currentTimeMillis();
    private String reason;
    private String error;
    private Map<String, Object> data = new HashMap<String, Object>();

    public Event(String type, String reason, String error)
    {
      this.type = type;
      this.reason = reason;
      this.error = error;
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

    public String getReason()
    {
      return reason;
    }

    public String getError()
    {
      return error;
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
        }
        catch (InterruptedException ex) {
          return;
        }
      }
    }

  }

  public void setup()
  {
  }

  public void start()
  {
    new EventRecorderThread().start();
  }

  public void recordEventAsync(Event event)
  {
    queue.add(event);
  }

  public void writeEvent(Event event)
  {
    //storage.writeDataItem(bytes, true);
  }

}
