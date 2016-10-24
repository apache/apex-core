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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.annotation.XmlType;

import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.ser.std.ToStringSerializer;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.sun.jersey.api.client.WebResource;

import com.datatorrent.common.util.ObjectMapperString;
import com.datatorrent.stram.client.WebServicesVersionConversion.IncompatibleVersionException;
import com.datatorrent.stram.debug.TupleRecorder;
import com.datatorrent.stram.util.FSPartFileCollection;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.StramWebServices;

/**
 * <p>RecordingsAgent class.</p>
 *
 * @since 0.3.2
 */
public final class RecordingsAgent extends FSPartFileAgent
{
  private static final Logger LOG = LoggerFactory.getLogger(RecordingsAgent.class);
  private static final long MAX_LIMIT_TUPLES = 1000;

  public static class RecordingInfo
  {
    public String id;
    @JsonSerialize(using = ToStringSerializer.class)
    public long startTime;
    public String containerId;
    public String appId;
    public String operatorId;
    @JsonSerialize(using = ToStringSerializer.class)
    public long totalTuples = 0;
    public List<PortInfo> ports;
    public boolean ended = false;
    public List<TupleRecorder.Range> windowIdRanges;
    public Map<String, Object> properties;
  }

  private static class RecordingsIndexLine extends IndexLine
  {
    public List<TupleRecorder.Range> windowIdRanges;
    @JsonSerialize(using = ToStringSerializer.class)
    public long fromTime;
    @JsonSerialize(using = ToStringSerializer.class)
    public long toTime;
    @JsonSerialize(using = ToStringSerializer.class)
    public long tupleCount;
    public Map<String, MutableLong> portTupleCount;
  }

  @XmlType(name = "port_info") // not really used, but this is to shut jackson up for conflicting xml names with TupleRecorder.PortInfo
  public static class PortInfo extends TupleRecorder.PortInfo
  {
    @JsonSerialize(using = ToStringSerializer.class)
    public long tupleCount = 0;
  }

  public static class WindowTuplesInfo
  {
    @JsonSerialize(using = ToStringSerializer.class)
    public long windowId;
    public List<TupleInfo> tuples = new ArrayList<>();
  }

  public static class TuplesInfo
  {
    @JsonSerialize(using = ToStringSerializer.class)
    public long startOffset;
    public List<WindowTuplesInfo> tuples = new ArrayList<>();
  }

  public static class TupleInfo
  {
    public String portId;
    public ObjectMapperString data;

    TupleInfo(String portId, String data)
    {
      this.portId = portId;
      this.data = new ObjectMapperString(data);
    }

  }

  public RecordingsAgent(StramAgent stramAgent)
  {
    super(stramAgent);
  }

  public String getRecordingsDirectory(String appId, String opId)
  {
    return getRecordingsDirectory(appId) + Path.SEPARATOR + opId;
  }

  public String getRecordingsDirectory(String appId)
  {
    String appPath = stramAgent.getAppPath(appId);
    if (appPath == null) {
      return null;
    }
    return appPath + Path.SEPARATOR + "recordings";
  }

  public String getRecordingDirectory(String appId, String opId, String id)
  {
    String dir = getRecordingsDirectory(appId, opId);
    return (dir == null) ? null : dir + Path.SEPARATOR + id;
  }

  @Override
  protected RecordingsIndexLine parseIndexLine(String line) throws JSONException
  {
    RecordingsIndexLine info = new RecordingsIndexLine();

    if (line.startsWith("E")) {
      info.isEndLine = true;
      return info;
    }
    line = line.trim();

    info.windowIdRanges = new ArrayList<>();
    info.portTupleCount = new HashMap<>();

    int cursor = 2;
    int cursor2 = line.indexOf(':', cursor);
    info.partFile = line.substring(cursor, cursor2);
    cursor = cursor2 + 1;
    cursor2 = line.indexOf(':', cursor);
    String timeRange = line.substring(cursor, cursor2);
    String[] tmp = timeRange.split("-");
    info.fromTime = Long.valueOf(tmp[0]);
    info.toTime = Long.valueOf(tmp[1]);
    cursor = cursor2 + 1;
    cursor2 = line.indexOf(':', cursor);
    if (cursor2 < 0) {
      info.tupleCount = Long.valueOf(line.substring(cursor));
      return info;
    }
    info.tupleCount = Long.valueOf(line.substring(cursor, cursor2));
    cursor = cursor2 + 1;
    if (!line.substring(cursor, cursor + 2).equals("T:")) {
      return info;
    }
    cursor += 2;
    cursor2 = line.indexOf(':', cursor);

    String windowRangesString = line.substring(cursor, cursor2);
    String[] windowRanges = windowRangesString.split(",");
    for (String windowRange : windowRanges) {
      String[] hilow = windowRange.split("-");
      long low = Long.valueOf(hilow[0]);
      long hi = Long.valueOf(hilow[1]);
      info.windowIdRanges.add(new TupleRecorder.Range(low, hi));
    }
    cursor = cursor2 + 1;
    cursor2 = line.indexOf(':', cursor);
    int size = Integer.valueOf(line.substring(cursor, cursor2));
    cursor = cursor2 + 1;
    cursor2 = cursor + size;
    JSONObject json = new JSONObject(line.substring(cursor, cursor2));
    Iterator<?> keys = json.keys();
    while (keys.hasNext()) {
      String portIndex = (String)keys.next();
      long tupleCount = json.getLong(portIndex);
      if (!info.portTupleCount.containsKey(portIndex)) {
        info.portTupleCount.put(portIndex, new MutableLong(tupleCount));
      } else {
        info.portTupleCount.get(portIndex).add(tupleCount);
      }
    }
    return info;
  }

  private Set<String> getRunningContainerIds(String appId)
  {
    Set<String> result = new HashSet<>();
    try {
      WebServicesClient webServicesClient = new WebServicesClient();
      JSONObject response = stramAgent.issueStramWebGetRequest(webServicesClient, appId, StramWebServices.PATH_PHYSICAL_PLAN_CONTAINERS);
      Object containersObj = response.get("containers");
      JSONArray containers;
      if (containersObj instanceof JSONArray) {
        containers = (JSONArray)containersObj;
      } else {
        containers = new JSONArray();
        containers.put(containersObj);
      }
      int len = containers.length();
      for (int i = 0; i < len; i++) {
        JSONObject container = containers.getJSONObject(i);
        if (container.getString("state").equals("ACTIVE")) {
          result.add(container.getString("id"));
        }
      }
    } catch (Exception ex) {
      LOG.warn("Error {} getting running containers for {}. Assuming no containers are running.", ex.getMessage(), appId);
    }
    return result;
  }

  public List<RecordingInfo> getRecordingInfo(String appId)
  {
    List<RecordingInfo> result = new ArrayList<>();
    String dir = getRecordingsDirectory(appId);
    if (dir == null) {
      return result;
    }
    Path path = new Path(dir);
    try {
      FileStatus fileStatus = stramAgent.getFileSystem().getFileStatus(path);

      if (!fileStatus.isDirectory()) {
        return result;
      }
      RemoteIterator<LocatedFileStatus> ri = stramAgent.getFileSystem().listLocatedStatus(path);
      while (ri.hasNext()) {
        LocatedFileStatus lfs = ri.next();
        if (lfs.isDirectory()) {
          try {
            String opId = lfs.getPath().getName();
            result.addAll(getRecordingInfo(appId, opId));
          } catch (NumberFormatException ex) {
            // ignore
          }
        }
      }
    } catch (IOException ex) {
      LOG.warn("Cannot get recording info for app id {}: {}", appId, ex);
      return result;
    }

    return result;
  }

  public List<RecordingInfo> getRecordingInfo(String appId, String opId)
  {
    Set<String> containers = getRunningContainerIds(appId);
    return getRecordingInfoHelper(appId, opId, containers);
  }

  private List<RecordingInfo> getRecordingInfoHelper(String appId, String opId, Set<String> containers)
  {
    List<RecordingInfo> result = new ArrayList<>();
    String dir = getRecordingsDirectory(appId, opId);
    if (dir == null) {
      return result;
    }
    Path path = new Path(dir);
    try {
      FileStatus fileStatus = stramAgent.getFileSystem().getFileStatus(path);

      if (!fileStatus.isDirectory()) {
        return result;
      }
      RemoteIterator<LocatedFileStatus> ri = stramAgent.getFileSystem().listLocatedStatus(path);
      while (ri.hasNext()) {
        LocatedFileStatus lfs = ri.next();
        if (lfs.isDirectory()) {
          try {
            String id = lfs.getPath().getName();
            RecordingInfo recordingInfo = getRecordingInfoHelper(appId, opId, id, containers);
            if (recordingInfo != null) {
              result.add(recordingInfo);
            }
          } catch (NumberFormatException ex) {
            // ignore
          }
        }
      }
    } catch (IOException ex) {
      LOG.warn("Cannot get recording info for app id {}: {}", appId, ex);
      return result;
    }

    return result;
  }

  public RecordingInfo getRecordingInfo(String appId, String opId, String id)
  {
    Set<String> containers = getRunningContainerIds(appId);
    return getRecordingInfoHelper(appId, opId, id, containers);
  }

  private RecordingInfo getRecordingInfoHelper(String appId, String opId, String id, Set<String> containers)
  {
    RecordingInfo info = new RecordingInfo();
    info.id = id;
    info.appId = appId;
    info.operatorId = opId;

    BufferedReader br = null;
    IndexFileBufferedReader ifbr = null;
    try {
      String dir = getRecordingDirectory(appId, opId, id);
      if (dir == null) {
        throw new Exception("recording directory is null");
      }

      Path path = new Path(dir);
      JSONObject json;

      FileStatus fileStatus = stramAgent.getFileSystem().getFileStatus(path);
      HashMap<String, PortInfo> portMap = new HashMap<>();
      if (!fileStatus.isDirectory()) {
        throw new Exception(path + " is not a directory");
      }

      // META file processing
      br = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, FSPartFileCollection.META_FILE))));
      String line;
      line = br.readLine();
      if (!line.equals("1.2")) {
        throw new Exception("Unexpected line: " + line);
      }
      line = br.readLine();
      json = new JSONObject(line);
      info.startTime = json.getLong("startTime");
      info.containerId = json.optString("containerId");
      info.properties = new HashMap<>();

      if (!StringUtils.isBlank(info.containerId) && !containers.contains(info.containerId)) {
        info.ended = true;
      }

      json = json.optJSONObject("properties");
      if (json != null) {
        @SuppressWarnings("unchecked")
        Iterator<String> keys = json.keys();
        while (keys.hasNext()) {
          String key = keys.next();
          // ugly 2 lines of code below since JSONObject.get(key).toString() doesn't give you json representation for plain strings
          String strValue = json.isNull(key) ? null : json.optString(key);
          info.properties.put(key, strValue != null ? strValue : new ObjectMapperString(json.get(key).toString()));
        }
      }
      info.ports = new ArrayList<>();
      while ((line = br.readLine()) != null) {
        PortInfo portInfo = new PortInfo();
        json = new JSONObject(line);
        portInfo.id = json.getInt("id");
        portInfo.name = json.getString("name");
        portInfo.type = json.getString("type");
        portInfo.streamName = json.getString("streamName");
        info.ports.add(portInfo);
        portMap.put(String.valueOf(portInfo.id), portInfo);
      }

      // INDEX file processing
      ifbr = new IndexFileBufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, FSPartFileCollection.INDEX_FILE))), dir);
      info.windowIdRanges = new ArrayList<>();
      long prevHiWindowId = -1;
      RecordingsIndexLine indexLine;
      while ((indexLine = (RecordingsIndexLine)ifbr.readIndexLine()) != null) {
        if (indexLine.isEndLine) {
          info.ended = true;
        } else {
          info.totalTuples += indexLine.tupleCount;
          for (Map.Entry<String, MutableLong> entry : indexLine.portTupleCount.entrySet()) {
            PortInfo portInfo = portMap.get(entry.getKey());
            if (portInfo == null) {
              throw new Exception("port info does not exist for " + entry.getKey());
            }
            portInfo.tupleCount += entry.getValue().longValue();
          }
          for (TupleRecorder.Range r : indexLine.windowIdRanges) {
            if (info.windowIdRanges.isEmpty()) {
              TupleRecorder.Range range = new TupleRecorder.Range();
              range.low = r.low;
              info.windowIdRanges.add(range);
            } else if (prevHiWindowId + 1 != r.low) {
              TupleRecorder.Range range = info.windowIdRanges.get(info.windowIdRanges.size() - 1);
              range.high = prevHiWindowId;
              range = new TupleRecorder.Range();
              range.low = r.low;
              info.windowIdRanges.add(range);
            }
            prevHiWindowId = r.high;
          }
        }
      }
      if (!info.windowIdRanges.isEmpty()) {
        TupleRecorder.Range range = info.windowIdRanges.get(info.windowIdRanges.size() - 1);
        range.high = prevHiWindowId;
      }
    } catch (Exception ex) {
      LOG.warn("Cannot get recording info for app id {}: {}", appId, ex);
      return null;
    } finally {
      IOUtils.closeQuietly(ifbr);
      IOUtils.closeQuietly(br);
    }

    return info;
  }

  private enum QueryType
  {
    OFFSET, WINDOW, TIME
  }

  public TuplesInfo getTuplesInfoByTime(String appId, String opId, String id, long fromTime, long toTime, long limit, String[] ports)
  {
    return getTuplesInfo(appId, opId, id, fromTime, toTime, limit, ports, QueryType.TIME);
  }

  public TuplesInfo getTuplesInfoByOffset(String appId, String opId, String id, long offset, long limit, String[] ports)
  {
    return getTuplesInfo(appId, opId, id, offset, 0, limit, ports, QueryType.OFFSET);
  }

  public TuplesInfo getTuplesInfoByWindow(String appId, String opId, String id, long startWindow, long limit, String[] ports)
  {
    return getTuplesInfo(appId, opId, id, startWindow, 0, limit, ports, QueryType.WINDOW);
  }

  private TuplesInfo getTuplesInfo(String appId, String opId, String id, long low, long high, long limit, String[] ports, QueryType queryType)
  {
    TuplesInfo info = new TuplesInfo();
    info.startOffset = -1;
    String dir = getRecordingDirectory(appId, opId, id);
    if (dir == null) {
      return null;
    }
    try (IndexFileBufferedReader ifbr = new IndexFileBufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, FSPartFileCollection.INDEX_FILE))), dir)) {
      long currentOffset = 0;
      boolean readPartFile = false;
      MutableLong numRemainingTuples = new MutableLong(limit);
      MutableLong currentTimestamp = new MutableLong();
      RecordingsIndexLine indexLine;
      String lastProcessPartFile = null;
      while ((indexLine = (RecordingsIndexLine)ifbr.readIndexLine()) != null) {
        if (indexLine.isEndLine) {
          continue;
        }
        MutableLong currentWindowLow = new MutableLong();
        MutableLong currentWindowHigh = new MutableLong();
        long numTuples = 0;

        if (ports == null || ports.length == 0) {
          numTuples = indexLine.tupleCount;
        } else {
          for (String port : ports) {
            if (indexLine.portTupleCount.containsKey(port)) {
              numTuples += indexLine.portTupleCount.get(port).longValue();
            } else {
              LOG.warn("Port index {} is not found, ignoring...", port);
            }
          }
        }
        currentWindowLow.setValue(indexLine.windowIdRanges.get(0).low);
        currentWindowHigh.setValue(indexLine.windowIdRanges.get(indexLine.windowIdRanges.size() - 1).high);

        if (!readPartFile) {
          if (queryType == QueryType.WINDOW) {
            if (currentWindowLow.longValue() > low) {
              break;
            } else if (currentWindowLow.longValue() <= low && low <= currentWindowHigh.longValue()) {
              readPartFile = true;
            }
          } else if (queryType == QueryType.OFFSET) {
            if (currentOffset + numTuples > low) {
              readPartFile = true;
            }
          } else { // time
            if (indexLine.fromTime > low) {
              break;
            } else if (indexLine.fromTime <= low && low <= indexLine.toTime) {
              readPartFile = true;
            }
          }
        }

        if (readPartFile) {
          lastProcessPartFile = indexLine.partFile;
          try (BufferedReader partBr = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, indexLine.partFile))))) {
            processPartFile(partBr, queryType, low, high, limit, ports, numRemainingTuples, currentTimestamp, currentWindowLow, currentOffset, info);
            currentOffset += numTuples;
          }
        }

        if (numRemainingTuples.longValue() <= 0 || (queryType == QueryType.TIME && currentTimestamp.longValue() > high)) {
          return info;
        }
      }
      BufferedReader partBr = null;
      try {
        String extraPartFile = getNextPartFile(lastProcessPartFile);
        if (extraPartFile != null) {
          partBr = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, extraPartFile))));
          processPartFile(partBr, queryType, low, high, limit, ports, numRemainingTuples, currentTimestamp, new MutableLong(), currentOffset, info);
        }
      } catch (Exception ex) {
        // ignore
      } finally {
        IOUtils.closeQuietly(partBr);
      }

    } catch (Exception ex) {
      LOG.warn("Cannot get recording tuple info for app id {}: {}", appId, ex);
      return null;
    }

    return info;
  }

  private void processPartFile(BufferedReader partBr, QueryType queryType, long low, long high, long limit, String[] ports, MutableLong numRemainingTuples, MutableLong currentTimestamp, MutableLong currentWindowLow, long currentOffset, TuplesInfo info) throws IOException
  {
    String partLine;
    long tmpOffset = currentOffset;
    // advance until offset is reached
    while ((partLine = partBr.readLine()) != null) {
      int partCursor = 2;
      if (partLine.startsWith("B:")) {
        int partCursor2 = partLine.indexOf(':', partCursor);
        currentTimestamp.setValue(Long.valueOf(partLine.substring(partCursor, partCursor2)));
        partCursor = partCursor2 + 1;
        currentWindowLow.setValue(Long.valueOf(partLine.substring(partCursor)));
        if (limit != numRemainingTuples.longValue()) {
          WindowTuplesInfo wtinfo;
          wtinfo = new WindowTuplesInfo();
          wtinfo.windowId = currentWindowLow.longValue();
          info.tuples.add(wtinfo);
        }
      } else if (partLine.startsWith("T:")) {
        int partCursor2 = partLine.indexOf(':', partCursor);
        currentTimestamp.setValue(Long.valueOf(partLine.substring(partCursor, partCursor2)));
        partCursor = partCursor2 + 1;
        partCursor2 = partLine.indexOf(':', partCursor);
        String port = partLine.substring(partCursor, partCursor2);
        boolean portMatch = (ports == null) || (ports.length == 0) || Arrays.asList(ports).contains(port);
        partCursor = partCursor2 + 1;

        if (portMatch
            && ((queryType == QueryType.WINDOW && currentWindowLow.longValue() >= low)
            || (queryType == QueryType.OFFSET && tmpOffset >= low)
            || (queryType == QueryType.TIME && currentTimestamp.longValue() >= low))) {

          if (numRemainingTuples.longValue() > 0) {
            if (info.startOffset == -1) {
              info.startOffset = tmpOffset;
            }
            WindowTuplesInfo wtinfo;
            if (info.tuples.isEmpty() || info.tuples.get(info.tuples.size() - 1).windowId != currentWindowLow
                .longValue()) {
              wtinfo = new WindowTuplesInfo();
              wtinfo.windowId = currentWindowLow.longValue();
              info.tuples.add(wtinfo);
            } else {
              wtinfo = info.tuples.get(info.tuples.size() - 1);
            }

            partCursor2 = partLine.indexOf(':', partCursor);
            int size = Integer.valueOf(partLine.substring(partCursor, partCursor2));
            partCursor = partCursor2 + 1;
            //partCursor2 = partCursor + size;
            String tupleValue = partLine.substring(partCursor);
            wtinfo.tuples.add(new TupleInfo(port, tupleValue));
            numRemainingTuples.decrement();
          } else {
            break;
          }
        }
        if (portMatch) {
          tmpOffset++;
        }
      }
    }
  }

  public JSONObject startRecording(String appId, String opId, String portName, long numWindows) throws IncompatibleVersionException
  {

    LOG.debug("Start recording requested for {}.{} ({} windows)", opId, portName, numWindows);
    try {
      final JSONObject request = new JSONObject();
      StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
      uriSpec = uriSpec.path(StramWebServices.PATH_PHYSICAL_PLAN_OPERATORS).path(opId);
      if (!StringUtils.isBlank(portName)) {
        uriSpec = uriSpec.path("ports").path(portName);
      }
      uriSpec = uriSpec.path(StramWebServices.PATH_RECORDINGS_START);
      request.put("numWindows", numWindows);
      WebServicesClient webServicesClient = new WebServicesClient();
      return stramAgent.issueStramWebRequest(webServicesClient, appId, uriSpec,
          new WebServicesClient.WebServicesHandler<JSONObject>()
          {
            @Override
            public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
            {
              return webResource.type(MediaType.APPLICATION_JSON).post(clazz, request);
            }

          }
      );
    } catch (Exception ex) {
      LOG.error("Cannot start recording: ", ex);
      return null;
    }
  }

  public JSONObject stopRecording(String appId, String opId, String portName) throws IncompatibleVersionException
  {
    try {
      final JSONObject request = new JSONObject();
      StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
      uriSpec = uriSpec.path(StramWebServices.PATH_PHYSICAL_PLAN_OPERATORS).path(opId);
      if (!StringUtils.isBlank(portName)) {
        uriSpec = uriSpec.path("ports").path(portName);
      }
      uriSpec = uriSpec.path(StramWebServices.PATH_RECORDINGS_STOP);
      WebServicesClient webServicesClient = new WebServicesClient();
      return stramAgent.issueStramWebRequest(webServicesClient, appId, uriSpec,
          new WebServicesClient.WebServicesHandler<JSONObject>()
          {
            @Override
            public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
            {
              return webResource.type(MediaType.APPLICATION_JSON).post(clazz, request);
            }

          }
      );
    } catch (Exception ex) {
      LOG.error("Cannot stop recording: ", ex);
      return null;
    }
  }

}
