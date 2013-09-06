/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import com.datatorrent.api.util.ObjectMapperString;
import com.datatorrent.stram.util.HdfsPartFileCollection;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.StramWebServices;
import com.sun.jersey.api.client.WebResource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.*;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>StatsAgent class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.3
 */
public class StatsAgent extends StramAgent
{
  private static final Logger LOG = LoggerFactory.getLogger(StatsAgent.class);

  public static class ContainerInfo
  {
    public String id;
    public String jvmName;
    public String host;
    public int memoryMBAllocated;
  }

  public static class ContainersInfo
  {
    public String appId;
    public Map<Integer, ContainerInfo> containers;
    public long startTime;
    public long endTime;
    public long count;
    public boolean ended;
  }

  public static class OperatorsInfo
  {
    public String appId;
    public String operatorName;
    public List<Integer> operatorIds;
    public long startTime;
    public long endTime;
    public long count;
    public boolean ended;
  }

  private static class IndexLine
  {
    public long startTime;
    public long endTime;
    public long count;
    public String partFile;
  }

  public static class OperatorStats
  {
    public int operatorId;
    public long timestamp;
    public ObjectMapperString stats;
  }

  public static class ContainerStats
  {
    public int containerId;
    public long timestamp;
    public ObjectMapperString stats;
  }

  public String getOperatorStatsDirectory(String stramRoot, String appId, String opName)
  {
    return getStatsDirectory(stramRoot, appId) + Path.SEPARATOR + "operators" + Path.SEPARATOR + opName;
  }

  public String getContainerStatsDirectory(String stramRoot, String appId)
  {
    return getStatsDirectory(stramRoot, appId) + Path.SEPARATOR + "containers";
  }

  public String getStatsDirectory(String stramRoot, String appId)
  {
    if (stramRoot == null) {
      stramRoot = getStramRootForLiveApp(appId);
      if (stramRoot == null) {
        return null;
      }
    }
    return stramRoot + Path.SEPARATOR + appId + Path.SEPARATOR + "stats";
  }

  private static IndexLine parseIndexLine(String line) throws JSONException
  {
    IndexLine info = new IndexLine();

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
    info.count = Long.valueOf(line.substring(cursor));
    return info;
  }

  public ContainersInfo getContainersInfo(String stramRoot, String appId)
  {
    ContainersInfo info = new ContainersInfo();
    info.appId = appId;
    info.containers = new HashMap<Integer, ContainerInfo>();
    String dir = getContainerStatsDirectory(stramRoot, appId);
    if (dir == null) {
      return null;
    }
    Path path = new Path(dir);
    JSONObject json;

    try {
      FileStatus fileStatus = fs.getFileStatus(path);
      if (!fileStatus.isDirectory()) {
        return null;
      }

      // META file processing
      FSDataInputStream in = fs.open(new Path(dir, HdfsPartFileCollection.META_FILE));
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String line;
      line = br.readLine();
      if (!line.equals("1.0")) {
        return null;
      }
      while ((line = br.readLine()) != null) {
        int cursor = line.indexOf(':');
        int index = Integer.valueOf(line.substring(0, cursor));
        json = new JSONObject(line.substring(cursor + 1));
        ContainerInfo containerInfo = new ContainerInfo();
        containerInfo.host = json.getString("host");
        containerInfo.jvmName = json.getString("jvmName");
        containerInfo.id = json.getString("id");
        containerInfo.memoryMBAllocated = json.getInt("memoryMBAllocated");
        info.containers.put(index, containerInfo);
      }
      // INDEX file processing
      in = fs.open(new Path(dir, HdfsPartFileCollection.INDEX_FILE));
      br = new BufferedReader(new InputStreamReader(in));

      while ((line = br.readLine()) != null) {
        if (line.startsWith("E")) {
          info.ended = true;
        }
        else if (line.startsWith("F:")) {
          IndexLine indexLine = parseIndexLine(line);
          info.count += indexLine.count;
          if (info.startTime == 0 || info.startTime > indexLine.startTime) {
            info.startTime = indexLine.startTime;
          }
          if (info.endTime == 0 || info.endTime < indexLine.endTime) {
            info.endTime = indexLine.endTime;
          }
        }
      }
    }
    catch (Exception ex) {
      LOG.warn("Got exception when reading containers info", ex);
      return null;
    }

    return info;
  }

  public OperatorsInfo getOperatorsInfo(String stramRoot, String appId, String opName)
  {
    OperatorsInfo info = new OperatorsInfo();
    info.appId = appId;
    info.operatorName = opName;
    info.operatorIds = new ArrayList<Integer>();
    String dir = getOperatorStatsDirectory(stramRoot, appId, opName);
    if (dir == null) {
      return null;
    }

    Path path = new Path(dir);
    JSONObject json;

    try {
      FileStatus fileStatus = fs.getFileStatus(path);
      if (!fileStatus.isDirectory()) {
        return null;
      }

      // META file processing
      FSDataInputStream in = fs.open(new Path(dir, HdfsPartFileCollection.META_FILE));
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String line;
      line = br.readLine();
      if (!line.equals("1.0")) {
        return null;
      }
      while ((line = br.readLine()) != null) {
        json = new JSONObject(line);
        info.operatorIds.add(json.getInt("id"));
      }

      // INDEX file processing
      in = fs.open(new Path(dir, HdfsPartFileCollection.INDEX_FILE));
      br = new BufferedReader(new InputStreamReader(in));
      while ((line = br.readLine()) != null) {
        if (line.startsWith("E")) {
          info.ended = true;
        }
        else if (line.startsWith("F:")) {
          IndexLine indexLine = parseIndexLine(line);
          info.count += indexLine.count;
          if (info.startTime == 0 || info.startTime > indexLine.startTime) {
            info.startTime = indexLine.startTime;
          }
          if (info.endTime == 0 || info.endTime < indexLine.endTime) {
            info.endTime = indexLine.endTime;
          }
        }
      }
      in.close();
    }
    catch (Exception ex) {
      LOG.warn("Got exception when reading operators info", ex);
      return null;
    }

    return info;
  }

  public List<OperatorStats> getOperatorsStats(String stramRoot, String appId, String opName, Long startTime, Long endTime)
  {
    List<OperatorStats> result = new ArrayList<OperatorStats>();
    String dir = getOperatorStatsDirectory(stramRoot, appId, opName);
    if (dir == null) {
      return null;
    }

    try {
      FSDataInputStream in = fs.open(new Path(dir, HdfsPartFileCollection.INDEX_FILE));
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String line;

      while ((line = br.readLine()) != null) {
        if (!line.startsWith("F:")) {
          continue;
        }
        IndexLine indexLine = parseIndexLine(line);
        if (startTime != null) {
          if (startTime.longValue() > indexLine.endTime) {
            continue;
          }
        }

        if (endTime != null) {
          if (endTime.longValue() < indexLine.startTime) {
            return result;
          }
        }

        FSDataInputStream partIn = fs.open(new Path(dir, indexLine.partFile));
        BufferedReader partBr = new BufferedReader(new InputStreamReader(partIn));
        String partLine;
        // advance until offset is reached
        while ((partLine = partBr.readLine()) != null) {
          OperatorStats os = new OperatorStats();
          int cursor = 0;
          int cursor2;
          cursor2 = partLine.indexOf(':', cursor);
          os.operatorId = Integer.valueOf(partLine.substring(cursor, cursor2));
          cursor = cursor2 + 1;
          cursor2 = partLine.indexOf(':', cursor);
          os.timestamp = Long.valueOf(partLine.substring(cursor, cursor2));
          cursor = cursor2 + 1;
          os.stats = new ObjectMapperString(partLine.substring(cursor));
          if ((startTime != null || os.timestamp >= startTime) && (endTime != null || os.timestamp <= endTime)) {
            result.add(os);
          }
        }
      }
    }
    catch (Exception ex) {
      LOG.warn("Got exception when reading operators stats", ex);
    }
    return result;
  }

  public List<ContainerStats> getContainersStats(String stramRoot, String appId, Long startTime, Long endTime)
  {
    List<ContainerStats> result = new ArrayList<ContainerStats>();
    String dir = getContainerStatsDirectory(stramRoot, appId);
    if (dir == null) {
      return null;
    }

    try {
      FSDataInputStream in = fs.open(new Path(dir, HdfsPartFileCollection.INDEX_FILE));
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String line;

      while ((line = br.readLine()) != null) {
        if (!line.startsWith("F:")) {
          continue;
        }
        IndexLine indexLine = parseIndexLine(line);
        if (startTime != null) {
          if (startTime.longValue() > indexLine.endTime) {
            continue;
          }
        }

        if (endTime != null) {
          if (endTime.longValue() < indexLine.startTime) {
            return result;
          }
        }

        FSDataInputStream partIn = fs.open(new Path(dir, indexLine.partFile));
        BufferedReader partBr = new BufferedReader(new InputStreamReader(partIn));
        String partLine;
        while ((partLine = partBr.readLine()) != null) {
          ContainerStats cs = new ContainerStats();
          int cursor = 0;
          int cursor2;
          cursor2 = partLine.indexOf(':', cursor);
          cs.containerId = Integer.valueOf(partLine.substring(cursor, cursor2));
          cursor = cursor2 + 1;
          cursor2 = partLine.indexOf(':', cursor);
          cs.timestamp = Long.valueOf(partLine.substring(cursor, cursor2));
          cursor = cursor2 + 1;
          cs.stats = new ObjectMapperString(partLine.substring(cursor));
          if ((startTime == null || cs.timestamp >= startTime) && (endTime == null || cs.timestamp <= endTime)) {
            result.add(cs);
          }
        }
      }
    }
    catch (Exception ex) {
      LOG.warn("Got exception when reading containers stats", ex);
    }
    return result;
  }

  public void syncStats(String appId) throws IOException, AppNotFoundException
  {
    WebServicesClient webServicesClient = new WebServicesClient();
    WebResource wr = getStramWebResource(webServicesClient, appId);
    if (wr == null) {
      throw new AppNotFoundException(appId);
    }
    final JSONObject request = new JSONObject();
    webServicesClient.process(wr.path(StramWebServices.PATH_SYNCSTATS), String.class,
                              new WebServicesClient.WebServicesHandler<String>()
    {
      @Override
      public String process(WebResource webResource, Class<String> clazz)
      {
        return webResource.type(MediaType.APPLICATION_JSON).post(clazz, request);
      }

    });
  }

}
