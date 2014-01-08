/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import com.datatorrent.stram.util.FSPartFileCollection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>EventsAgent class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.4
 */
public final class EventsAgent extends FSPartFileAgent
{
  private static final Logger LOG = LoggerFactory.getLogger(EventsAgent.class);

  private static class EventsIndexLine extends IndexLine
  {
    public long startTime;
    public long endTime;
  }

  public static class Event
  {
    public long timestamp;
    public String type;
    public Map<String, Object> data;
  }

  private String getEventsDirectory(String appId)
  {
    String appPath = getAppPath(appId);
    if (appPath == null) {
      return null;
    }
    return appPath + Path.SEPARATOR + "events";
  }

  @Override
  protected EventsIndexLine parseIndexLine(String line) throws JSONException
  {
    EventsIndexLine info = new EventsIndexLine();
    if (line.startsWith("E")) {
      info.isEndLine = true;
      return info;
    }
    line = line.trim();
    int cursor = 2;
    int cursor2 = line.indexOf(':', cursor);
    info.partFile = line.substring(cursor, cursor2);
    cursor = cursor2 + 1;
    cursor2 = line.indexOf(':', cursor);
    String timeRange = line.substring(cursor, cursor2);
    String[] tmp = timeRange.split("-");
    info.startTime = Long.valueOf(tmp[0]);
    info.endTime = Long.valueOf(tmp[1]);
    cursor = cursor2 + 1;
    return info;
  }

  @SuppressWarnings("unchecked")
  public List<Event> getEvents(String appId, Long fromTime, Long toTime)
  {
    List<Event> result = new ArrayList<Event>();
    String dir = getEventsDirectory(appId);
    if (dir == null) {
      return null;
    }
    IndexFileBufferedReader ifbr = null;
    BufferedReader partBr = null;
    try {
      ifbr = new IndexFileBufferedReader(new InputStreamReader(fs.open(new Path(dir, FSPartFileCollection.INDEX_FILE))), dir);
      EventsIndexLine indexLine;

      while ((indexLine = (EventsIndexLine)ifbr.readIndexLine()) != null) {
        if (indexLine.isEndLine) {
          continue;
        }
        if (fromTime != null) {
          if (fromTime.longValue() > indexLine.endTime) {
            continue;
          }
        }

        if (toTime != null) {
          if (toTime.longValue() < indexLine.startTime) {
            return result;
          }
        }

        partBr = new BufferedReader(new InputStreamReader(fs.open(new Path(dir, indexLine.partFile))));
        String partLine;
        while ((partLine = partBr.readLine()) != null) {
          Event ev = new Event();
          int cursor = 0;
          int cursor2;
          cursor2 = partLine.indexOf(':', cursor);
          ev.timestamp = Long.valueOf(partLine.substring(cursor, cursor2));
          cursor = cursor2 + 1;
          cursor2 = partLine.indexOf(':', cursor);
          ev.type = partLine.substring(cursor, cursor2);
          cursor = cursor2 + 1;
          if ((fromTime == null || ev.timestamp >= fromTime) && (toTime == null || ev.timestamp <= toTime)) {
            ev.data = new ObjectMapper().readValue(partLine.substring(cursor), HashMap.class);
            result.add(ev);
          }
        }
      }
    }
    catch (Exception ex) {
      LOG.warn("Got exception when reading operators stats", ex);
    }
    finally {
      try {
        if (ifbr != null) {
          ifbr.close();
        }
        if (partBr != null) {
          partBr.close();
        }
      }
      catch (IOException ex) {
      }
    }
    return result;
  }

}
