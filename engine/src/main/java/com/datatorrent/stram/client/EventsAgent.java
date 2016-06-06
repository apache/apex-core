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
package com.datatorrent.stram.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.ser.std.ToStringSerializer;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;

import com.datatorrent.common.util.Pair;
import com.datatorrent.stram.util.FSPartFileCollection;

/**
 * <p>EventsAgent class.</p>
 *
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
    public Map<String, String> data;
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
    LinkedList<EventInfo> result = new LinkedList<>();
    String dir = getEventsDirectory(appId);
    if (dir == null) {
      return null;
    }
    long totalNumEvents = 0;
    LinkedList<Pair<String, Long>> partFiles = new LinkedList<>();
    try (IndexFileBufferedReader ifbr = new IndexFileBufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, FSPartFileCollection.INDEX_FILE))), dir)) {
      EventsIndexLine indexLine;
      while ((indexLine = (EventsIndexLine)ifbr.readIndexLine()) != null) {
        if (indexLine.isEndLine) {
          continue;
        }
        partFiles.add(new Pair<>(indexLine.partFile, indexLine.numEvents));
        totalNumEvents += indexLine.numEvents;
      }
    } catch (Exception ex) {
      LOG.warn("Cannot read events for {}: {}", appId, ex);
      return result;
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
      try (BufferedReader partBr = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, partFile.first))))) {
        processPartFile(partBr, null, null, offset, limit, result);
        offset = 0;
        lastProcessPartFile = partFile.first;
      } catch (Exception ex) {
        LOG.warn("Cannot read events for {}: {}", appId, ex);
      }

    }

    BufferedReader partBr = null;
    try {
      String extraPartFile = getNextPartFile(lastProcessPartFile);
      if (extraPartFile != null && limit > 0) {
        partBr = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, extraPartFile))));
        processPartFile(partBr, null, null, 0, Integer.MAX_VALUE, result);
      }
    } catch (Exception ex) {
      // ignore
    } finally {
      IOUtils.closeQuietly(partBr);
    }
    while (result.size() > limit) {
      result.removeFirst();
    }
    return result;
  }

  public List<EventInfo> getEvents(String appId, Long fromTime, Long toTime, long offset, int limit)
  {
    List<EventInfo> result = new ArrayList<>();
    String dir = getEventsDirectory(appId);
    if (dir == null) {
      return null;
    }
    try (IndexFileBufferedReader ifbr = new IndexFileBufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, FSPartFileCollection.INDEX_FILE))), dir)) {
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

        try (BufferedReader partBr = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, indexLine.partFile))))) {
          offset = processPartFile(partBr, fromTime, toTime, offset, limit, result);
          limit -= result.size();
        }
      }
      BufferedReader partBr = null;
      try {
        String extraPartFile = getNextPartFile(lastProcessPartFile);
        if (extraPartFile != null && limit > 0) {
          partBr = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem()
              .open(new Path(dir, extraPartFile))));
          processPartFile(partBr, fromTime, toTime, offset, limit, result);
        }
      } catch (Exception ex) {
        // ignore
      } finally {
        IOUtils.closeQuietly(partBr);
      }
    } catch (Exception ex) {
      LOG.warn("Cannot read events for {}: {}", appId, ex);
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
        } else if (limit-- > 0) {
          ev.data = new ObjectMapper().readValue(partLine.substring(cursor), HashMap.class);
          ev.id = Long.valueOf(ev.data.get("id"));
          ev.data.remove("id");
          result.add(ev);
        }
      }
    }
    return offset;
  }
}
