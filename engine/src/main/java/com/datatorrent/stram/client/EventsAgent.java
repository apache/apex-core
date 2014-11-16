/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import com.datatorrent.common.util.Pair;
import com.datatorrent.stram.util.FSPartFileCollection;
import java.io.*;
import java.util.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.ser.std.ToStringSerializer;
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
    @JsonSerialize(using = ToStringSerializer.class)
    public long startTime;
    @JsonSerialize(using = ToStringSerializer.class)
    public long endTime;
    @JsonSerialize(using = ToStringSerializer.class)
    public long numEvents;
  }

  public static class EventInfo
  {
    @JsonSerialize(using = ToStringSerializer.class)
    public long id;
    @JsonSerialize(using = ToStringSerializer.class)
    public long timestamp;
    public String type;
    public Map<String, Object> data;
  }

  public EventsAgent(StramAgent stramAgent)
  {
    super(stramAgent);
  }

  private String getEventsDirectory(String appId)
  {
    String appPath = stramAgent.getAppPath(appId);
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
    info.numEvents = Long.valueOf(line.substring(cursor));
    return info;
  }

  public List<EventInfo> getLatestEvents(String appId, int limit)
  {
    LinkedList<EventInfo> result = new LinkedList<EventInfo>();
    String dir = getEventsDirectory(appId);
    if (dir == null) {
      return null;
    }
    long totalNumEvents = 0;
    IndexFileBufferedReader ifbr = null;
    LinkedList<Pair<String, Long>> partFiles = new LinkedList<Pair<String, Long>>();
    try {
      ifbr = new IndexFileBufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, FSPartFileCollection.INDEX_FILE))), dir);
      EventsIndexLine indexLine;
      while ((indexLine = (EventsIndexLine)ifbr.readIndexLine()) != null) {
        if (indexLine.isEndLine) {
          continue;
        }
        partFiles.add(new Pair<String, Long>(indexLine.partFile, indexLine.numEvents));
        totalNumEvents += indexLine.numEvents;
      }
    }
    catch (Exception ex) {
      LOG.warn("Got exception when reading events", ex);
      return result;
    }
    finally {
      IOUtils.closeQuietly(ifbr);
    }

    long offset = 0;
    while (totalNumEvents > limit && !partFiles.isEmpty()) {
      Pair<String, Long> head = partFiles.getFirst();
      if (totalNumEvents - head.second < limit) {
        offset = Math.max(0, totalNumEvents - limit);
        break;
      }
      totalNumEvents -= head.second;
      partFiles.removeFirst();
    }
    String lastProcessPartFile = null;
    for (Pair<String, Long> partFile : partFiles) {
      BufferedReader partBr = null;
      try {
        partBr = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, partFile.first))));
        processPartFile(partBr, null, null, offset, limit, result);
       offset = 0;
        lastProcessPartFile = partFile.first;
      }
      catch (Exception ex) {
        LOG.warn("Got exception when reading events", ex);
      }
      finally {
        IOUtils.closeQuietly(partBr);
      }
    }

    BufferedReader partBr = null;
    try {
      String extraPartFile = getNextPartFile(lastProcessPartFile);
      if (extraPartFile != null && limit > 0) {
        partBr = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, extraPartFile))));
        processPartFile(partBr, null, null, 0, Integer.MAX_VALUE, result);
      }
    }
    catch (Exception ex) {
      // ignore
    }
    finally {
      IOUtils.closeQuietly(partBr);
    }
    while (result.size() > limit) {
      result.removeFirst();
    }
    return result;
  }

  public List<EventInfo> getEvents(String appId, Long fromTime, Long toTime, long offset, int limit)
  {
    List<EventInfo> result = new ArrayList<EventInfo>();
    String dir = getEventsDirectory(appId);
    if (dir == null) {
      return null;
    }
    IndexFileBufferedReader ifbr = null;
    try {
      ifbr = new IndexFileBufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, FSPartFileCollection.INDEX_FILE))), dir);
      EventsIndexLine indexLine;
      String lastProcessPartFile = null;
      while ((indexLine = (EventsIndexLine)ifbr.readIndexLine()) != null) {
        if (indexLine.isEndLine) {
          continue;
        }
        lastProcessPartFile = indexLine.partFile;
        if (fromTime != null) {
          if (fromTime > indexLine.endTime) {
            continue;
          }
        }

        if (toTime != null) {
          if (toTime < indexLine.startTime) {
            return result;
          }
        }

        BufferedReader partBr = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, indexLine.partFile))));
        try {
          offset = processPartFile(partBr, fromTime, toTime, offset, limit, result);
          limit -= result.size();
        }
        finally {
          partBr.close();
        }
      }
      BufferedReader partBr = null;
      try {
        String extraPartFile = getNextPartFile(lastProcessPartFile);
        if (extraPartFile != null && limit > 0) {
          partBr = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, extraPartFile))));
          processPartFile(partBr, fromTime, toTime, offset, limit, result);
        }
      }
      catch (Exception ex) {
        // ignore
      }
      finally {
        IOUtils.closeQuietly(partBr);
      }
    }
    catch (Exception ex) {
      LOG.warn("Got exception when reading events", ex);
    }
    finally {
      IOUtils.closeQuietly(ifbr);
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private long processPartFile(BufferedReader partBr, Long fromTime, Long toTime, long offset, int limit, List<EventInfo> result) throws IOException
  {
    String partLine;
    while ((partLine = partBr.readLine()) != null) {
      EventInfo ev = new EventInfo();
      int cursor = 0;
      int cursor2;
      cursor2 = partLine.indexOf(':', cursor);
      ev.timestamp = Long.valueOf(partLine.substring(cursor, cursor2));
      cursor = cursor2 + 1;
      cursor2 = partLine.indexOf(':', cursor);
      ev.type = partLine.substring(cursor, cursor2);
      cursor = cursor2 + 1;
      if ((fromTime == null || ev.timestamp >= fromTime) && (toTime == null || ev.timestamp <= toTime)) {
        if (offset > 0) {
          offset--;
        }
        else if (limit-- > 0) {
          ev.data = new ObjectMapper().readValue(partLine.substring(cursor), HashMap.class);
          ev.id = Long.valueOf((String)ev.data.get("id"));
          ev.data.remove("id");
          result.add(ev);
        }
      }
    }
    return offset;
  }
}
