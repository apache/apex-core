/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.beanutils.BeanUtils;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public interface EventRecorder
{
  public void recordEventAsync(Event event);

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

    public void populateData(Object obj) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException
    {
      @SuppressWarnings("unchecked")
      Map<String, Object> properties = BeanUtils.describe(obj);

      for (Map.Entry<String, Object> property : properties.entrySet()) {
        data.put(property.getKey(), property.getValue());
      }
    }

  }

}
