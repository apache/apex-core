/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.annotation.XmlType;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.WebResource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import com.datatorrent.api.util.ObjectMapperString;

import com.datatorrent.stram.debug.TupleRecorder;
import com.datatorrent.stram.util.HdfsPartFileCollection;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.StramWebServices;

/**
 * <p>RecordingsAgent class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.2
 */
public class RecordingsAgent extends StramAgent
{
  private static final Logger LOG = LoggerFactory.getLogger(RecordingsAgent.class);
  private static final long MAX_LIMIT_TUPLES = 1000;
  private boolean localMode = false;

  public static class RecordingInfo
  {
    public long startTime;
    public String appId;
    public String operatorId;
    public String recordingName;
    public String containerId;
    public long totalTuples = 0;
    public List<PortInfo> ports;
    public boolean ended = false;
    public List<TupleRecorder.Range> windowIdRanges;
    public Map<String, Object> properties;
  }

  private static class IndexLine
  {
    public List<TupleRecorder.Range> windowIdRanges;
    public long startTime;
    public long endTime;
    public long tupleCount;
    public Map<String, MutableLong> portTupleCount;
    public String partFile;
  }

  @XmlType(name = "port_info") // not really used, but this is to shut jackson up for conflicting xml names with TupleRecorder.PortInfo
  public static class PortInfo extends TupleRecorder.PortInfo
  {
    public long tupleCount = 0;
  }

  public static class WindowTuplesInfo
  {
    public long windowId;
    public List<TupleInfo> tuples = new ArrayList<TupleInfo>();
  }

  public static class TuplesInfo
  {
    public long startOffset;
    public List<WindowTuplesInfo> tuples = new ArrayList<WindowTuplesInfo>();
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

  public void setLocalMode(boolean localMode)
  {
    this.localMode = localMode;
  }

  @Override
  public void setup() throws IOException
  {
    if (localMode) {
      fs = new RawLocalFileSystem();
      try {
        fs.initialize(new URI("file:/test"), new Configuration());
      }
      catch (Exception ex) {
      }
    }
    else {
      fs = FileSystem.get(new Configuration());
    }
  }

  public String getRecordingsDirectory(String appId, String opId)
  {
    return getRecordingsDirectory(appId) + Path.SEPARATOR + opId;
  }

  public String getRecordingsDirectory(String appId)
  {
    String appPath = getAppPath(appId);
    if (appPath == null) {
      return null;
    }
    return appPath + Path.SEPARATOR + "recordings";
  }

  public String getRecordingDirectory(String appId, String opId, long startTime)
  {
    String dir = getRecordingsDirectory(appId, opId);
    return (dir == null) ? null : dir + Path.SEPARATOR + String.valueOf(startTime);
  }

  private static IndexLine parseIndexLine(String line) throws JSONException
  {
    IndexLine info = new IndexLine();
    info.windowIdRanges = new ArrayList<TupleRecorder.Range>();
    info.portTupleCount = new HashMap<String, MutableLong>();

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
    cursor2 = line.indexOf(':', cursor);
    info.tupleCount = Long.valueOf(line.substring(cursor, cursor2));
    cursor = cursor2 + 1;
    if (!line.substring(cursor, cursor + 2).equals("T:")) {
      return null;
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
      }
      else {
        info.portTupleCount.get(portIndex).add(tupleCount);
      }
    }
    return info;
  }

  private Set<String> getRunningContainerIds(String appId)
  {
    Set<String> result = new HashSet<String>();
    String url = "http://" + resourceManagerWebappAddress + "/proxy/" + appId + "/ws/v1/stram/containers";
    try {
      WebServicesClient webServicesClient = new WebServicesClient();
      JSONObject response = webServicesClient.process(url, JSONObject.class, new WebServicesClient.GetWebServicesHandler<JSONObject>());
      Object containersObj = response.get("containers");
      JSONArray containers;
      if (containersObj instanceof JSONArray) {
        containers = (JSONArray)containersObj;
      }
      else {
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
    }
    catch (Exception ex) {
      LOG.warn("Error getting running containers for {}. Assuming no containers are running.", appId);
    }
    return result;
  }

  public List<RecordingInfo> getRecordingInfo(String appId)
  {
    List<RecordingInfo> result = new ArrayList<RecordingInfo>();
    String dir = getRecordingsDirectory(appId);
    if (dir == null) {
      return result;
    }
    Path path = new Path(dir);
    try {
      FileStatus fileStatus = fs.getFileStatus(path);

      if (!fileStatus.isDirectory()) {
        return result;
      }
      RemoteIterator<LocatedFileStatus> ri = fs.listLocatedStatus(path);
      while (ri.hasNext()) {
        LocatedFileStatus lfs = ri.next();
        if (lfs.isDirectory()) {
          try {
            String opId = lfs.getPath().getName();
            result.addAll(getRecordingInfo(appId, opId));
          }
          catch (NumberFormatException ex) {
            continue;
          }
        }
      }
    }
    catch (IOException ex) {
      LOG.warn("Got exception when getting recording info", ex);
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
    ArrayList<RecordingInfo> result = new ArrayList<RecordingInfo>();
    String dir = getRecordingsDirectory(appId, opId);
    if (dir == null) {
      return result;
    }
    Path path = new Path(dir);
    try {
      FileStatus fileStatus = fs.getFileStatus(path);

      if (!fileStatus.isDirectory()) {
        return result;
      }
      RemoteIterator<LocatedFileStatus> ri = fs.listLocatedStatus(path);
      while (ri.hasNext()) {
        LocatedFileStatus lfs = ri.next();
        if (lfs.isDirectory()) {
          try {
            Long startTime = Long.valueOf(lfs.getPath().getName());
            result.add(getRecordingInfoHelper(appId, opId, startTime, containers));
          }
          catch (NumberFormatException ex) {
            continue;
          }
        }
      }
    }
    catch (IOException ex) {
      LOG.warn("Got exception when getting recording info", ex);
      return result;
    }

    return result;
  }

  public RecordingInfo getRecordingInfo(String appId, String opId, long startTime)
  {
    Set<String> containers = getRunningContainerIds(appId);
    return getRecordingInfoHelper(appId, opId, startTime, containers);
  }

  private RecordingInfo getRecordingInfoHelper(String appId, String opId, long startTime, Set<String> containers)
  {
    RecordingInfo info = new RecordingInfo();
    info.appId = appId;
    info.operatorId = opId;
    String dir = getRecordingDirectory(appId, opId, startTime);
    if (dir == null) {
      return null;
    }

    Path path = new Path(dir);
    JSONObject json;

    try {
      FileStatus fileStatus = fs.getFileStatus(path);
      HashMap<String, PortInfo> portMap = new HashMap<String, PortInfo>();
      if (!fileStatus.isDirectory()) {
        return null;
      }

      // META file processing
      FSDataInputStream in = fs.open(new Path(dir, HdfsPartFileCollection.META_FILE));
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String line;
      line = br.readLine();
      if (!line.equals("1.1")) {
        return null;
      }
      line = br.readLine();
      json = new JSONObject(line);
      info.startTime = json.getLong("startTime");
      info.recordingName = json.getString("recordingName");
      info.containerId = json.optString("containerId");
      info.properties = new HashMap<String, Object>();

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
      info.ports = new ArrayList<PortInfo>();
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
      in = fs.open(new Path(dir, HdfsPartFileCollection.INDEX_FILE));
      br = new BufferedReader(new InputStreamReader(in));
      info.windowIdRanges = new ArrayList<TupleRecorder.Range>();
      long prevHiWindowId = -1;
      while ((line = br.readLine()) != null) {
        if (line.startsWith("E")) {
          info.ended = true;
        }
        else if (line.startsWith("F:")) {
          IndexLine indexLine = parseIndexLine(line);
          info.totalTuples += indexLine.tupleCount;
          for (Map.Entry<String, MutableLong> entry : indexLine.portTupleCount.entrySet()) {
            PortInfo portInfo = portMap.get(entry.getKey());
            if (portInfo == null) {
              return null;
            }
            portInfo.tupleCount += entry.getValue().longValue();
          }
          for (TupleRecorder.Range r : indexLine.windowIdRanges) {
            if (info.windowIdRanges.isEmpty()) {
              TupleRecorder.Range range = new TupleRecorder.Range();
              range.low = r.low;
              info.windowIdRanges.add(range);
            }
            else if (prevHiWindowId + 1 != r.low) {
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
      in.close();
      if (!info.windowIdRanges.isEmpty()) {
        TupleRecorder.Range range = info.windowIdRanges.get(info.windowIdRanges.size() - 1);
        range.high = prevHiWindowId;
      }
    }
    catch (Exception ex) {
      LOG.warn("Got exception when getting recording info", ex);
      return null;
    }

    return info;
  }

  public TuplesInfo getTuplesInfo(String appId, String opId, long startTime, long offset, long limit, String[] ports, boolean treatOffsetAsWindow)
  {
    TuplesInfo info = new TuplesInfo();
    info.startOffset = -1;
    String dir = getRecordingDirectory(appId, opId, startTime);
    if (dir == null) {
      return null;
    }

    try {
      FSDataInputStream in = fs.open(new Path(dir, HdfsPartFileCollection.INDEX_FILE));
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String line;
      long currentOffset = 0;
      boolean readPartFile = false;
      if (limit == 0 || limit > MAX_LIMIT_TUPLES) {
        limit = MAX_LIMIT_TUPLES;
      }
      long numRemainingTuples = limit;

      while ((line = br.readLine()) != null) {
        if (!line.startsWith("F:")) {
          continue;
        }
        IndexLine indexLine = parseIndexLine(line);
        long currentWindowLow;
        long currentWindowHigh;
        long numTuples = 0;

        if (ports == null || ports.length == 0) {
          numTuples = indexLine.tupleCount;
        }
        else {
          for (String port : ports) {
            if (indexLine.portTupleCount.containsKey(port)) {
              numTuples += indexLine.portTupleCount.get(port).longValue();
            }
            else {
              LOG.warn("Port index {} is not found, ignoring...", port);
            }
          }
        }
        currentWindowLow = indexLine.windowIdRanges.get(0).low;
        currentWindowHigh = indexLine.windowIdRanges.get(indexLine.windowIdRanges.size() - 1).high;

        if (!readPartFile) {
          if (treatOffsetAsWindow) {
            if (currentWindowLow > offset) {
              break;
            }
            else if (currentWindowLow <= offset && offset <= currentWindowHigh) {
              readPartFile = true;
            }
          }
          else {
            if (currentOffset + numTuples > offset) {
              readPartFile = true;
            }
          }
        }

        if (readPartFile) {
          FSDataInputStream partIn = fs.open(new Path(dir, indexLine.partFile));
          BufferedReader partBr = new BufferedReader(new InputStreamReader(partIn));
          String partLine;
          long tmpOffset = currentOffset;
          // advance until offset is reached
          while ((partLine = partBr.readLine()) != null) {
            int partCursor = 2;
            if (partLine.startsWith("B:")) {
              currentWindowLow = Long.valueOf(partLine.substring(partCursor));
              if (limit != numRemainingTuples) {
                WindowTuplesInfo wtinfo;
                wtinfo = new WindowTuplesInfo();
                wtinfo.windowId = currentWindowLow;
                info.tuples.add(wtinfo);
              }
            }
            else if (partLine.startsWith("T:")) {
              int partCursor2 = partLine.indexOf(':', partCursor);
              String port = partLine.substring(partCursor, partCursor2);
              boolean portMatch = (ports == null) || (ports.length == 0) || Arrays.asList(ports).contains(port);
              partCursor = partCursor2 + 1;

              if (portMatch && ((treatOffsetAsWindow && currentWindowLow >= offset)
                      || (!treatOffsetAsWindow && tmpOffset >= offset))) {

                if (numRemainingTuples > 0) {
                  if (info.startOffset == -1) {
                    info.startOffset = tmpOffset;
                  }
                  WindowTuplesInfo wtinfo;
                  if (info.tuples.isEmpty() || info.tuples.get(info.tuples.size() - 1).windowId != currentWindowLow) {
                    wtinfo = new WindowTuplesInfo();
                    wtinfo.windowId = currentWindowLow;
                    info.tuples.add(wtinfo);
                  }
                  else {
                    wtinfo = info.tuples.get(info.tuples.size() - 1);
                  }

                  partCursor2 = partLine.indexOf(':', partCursor);
                  int size = Integer.valueOf(partLine.substring(partCursor, partCursor2));
                  partCursor = partCursor2 + 1;
                  //partCursor2 = partCursor + size;
                  String tupleValue = partLine.substring(partCursor);
                  wtinfo.tuples.add(new TupleInfo(port, tupleValue));
                  numRemainingTuples--;
                }
                else {
                  return info;
                }
              }
              if (portMatch) {
                tmpOffset++;
              }
            }
          }
        }
        currentOffset += numTuples;
        if (numRemainingTuples == 0) {
          return info;
        }
      }
    }
    catch (Exception ex) {
      LOG.warn("Got exception when getting tuples info", ex);
      return null;
    }

    return info;
  }

  public String startRecording(String appId, String opId, String portName)
  {
    WebServicesClient webServicesClient = new WebServicesClient();
    WebResource wr = getStramWebResource(webServicesClient, appId);
    if (wr == null) {
      throw new WebApplicationException(404);
    }
    try {
      final JSONObject request = new JSONObject();
      request.put("operId", opId);
      if (!StringUtils.isBlank(portName)) {
        request.put("portName", portName);
      }
      return webServicesClient.process(wr.path(StramWebServices.PATH_STARTRECORDING), String.class,
                                       new WebServicesClient.WebServicesHandler<String>()
      {
        @Override
        public String process(WebResource webResource, Class<String> clazz)
        {
          return webResource.type(MediaType.APPLICATION_JSON).post(clazz, request);
        }

      });
    }
    catch (Exception ex) {
      return null;
    }
  }

  public String stopRecording(String appId, String opId, String portName)
  {
    WebServicesClient webServicesClient = new WebServicesClient();
    WebResource wr = getStramWebResource(webServicesClient, appId);
    if (wr == null) {
      throw new WebApplicationException(404);
    }
    try {
      final JSONObject request = new JSONObject();
      request.put("operId", opId);
      if (!StringUtils.isBlank(portName)) {
        request.put("portName", portName);
      }
      return webServicesClient.process(wr.path(StramWebServices.PATH_STOPRECORDING), String.class,
                                       new WebServicesClient.WebServicesHandler<String>()
      {
        @Override
        public String process(WebResource webResource, Class<String> clazz)
        {
          return webResource.type(MediaType.APPLICATION_JSON).post(clazz, request);
        }

      });
    }
    catch (Exception ex) {
      return null;
    }
  }

  public void syncRecording(String appId, String opId, String portName) throws IOException, AppNotFoundException
  {
    WebServicesClient webServicesClient = new WebServicesClient();
    WebResource wr = getStramWebResource(webServicesClient, appId);
    if (wr == null) {
      throw new AppNotFoundException(appId);
    }
    try {
      final JSONObject request = new JSONObject();
      request.put("operId", opId);
      if (!StringUtils.isBlank(portName)) {
        request.put("portName", portName);
      }
      webServicesClient.process(wr.path(StramWebServices.PATH_SYNCRECORDING), String.class,
                                new WebServicesClient.WebServicesHandler<String>()
      {
        @Override
        public String process(WebResource webResource, Class<String> clazz)
        {
          return webResource.type(MediaType.APPLICATION_JSON).post(clazz, request);
        }

      });
    }
    catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
  }

}
