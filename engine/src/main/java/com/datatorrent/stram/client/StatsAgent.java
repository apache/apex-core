/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import com.datatorrent.lib.util.ObjectMapperString;
import com.datatorrent.stram.util.FSPartFileCollection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
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
public final class StatsAgent extends FSPartFileAgent
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

  private static class StatsIndexLine extends IndexLine
  {
    public long startTime;
    public long endTime;
    public long count;
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

  public StatsAgent(Configuration conf)
  {
    super(conf);
  }

  public String getOperatorStatsDirectory(String appId, String opName)
  {
    return getStatsDirectory(appId) + Path.SEPARATOR + "operators" + Path.SEPARATOR + opName;
  }

  public String getContainerStatsDirectory(String appId)
  {
    return getStatsDirectory(appId) + Path.SEPARATOR + "containers";
  }

  public String getStatsDirectory(String appId)
  {
    String appPath = getAppPath(appId);
    if (appPath == null) {
      return null;
    }
    return appPath + Path.SEPARATOR + "stats";
  }

  @Override
  protected StatsIndexLine parseIndexLine(String line) throws JSONException
  {
    StatsIndexLine info = new StatsIndexLine();

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
    info.count = Long.valueOf(line.substring(cursor));
    return info;
  }

  public ContainersInfo getContainersInfo(String appId)
  {
    ContainersInfo info = new ContainersInfo();
    info.appId = appId;
    info.containers = new HashMap<Integer, ContainerInfo>();
    String dir = getContainerStatsDirectory(appId);
    if (dir == null) {
      return null;
    }
    Path path = new Path(dir);
    JSONObject json;
    BufferedReader br = null;
    IndexFileBufferedReader ifbr = null;
    try {
      FileStatus fileStatus = fs.getFileStatus(path);
      if (!fileStatus.isDirectory()) {
        return null;
      }

      // META file processing
      br = new BufferedReader(new InputStreamReader(fs.open(new Path(dir, FSPartFileCollection.META_FILE))));
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
      ifbr = new IndexFileBufferedReader(new InputStreamReader(fs.open(new Path(dir, FSPartFileCollection.INDEX_FILE))), dir);
      StatsIndexLine indexLine;

      while ((indexLine = (StatsIndexLine)ifbr.readIndexLine()) != null) {
        if (indexLine.isEndLine) {
          info.ended = true;
        }
        else {
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
    finally {
      try {
        if (br != null) {
          br.close();
        }
        if (ifbr != null) {
          ifbr.close();
        }
      }
      catch (IOException ex) {
        // ignore
      }
    }

    return info;
  }

  public OperatorsInfo getOperatorsInfo(String appId, String opName)
  {
    OperatorsInfo info = new OperatorsInfo();
    info.appId = appId;
    info.operatorName = opName;
    info.operatorIds = new ArrayList<Integer>();
    String dir = getOperatorStatsDirectory(appId, opName);
    if (dir == null) {
      return null;
    }

    Path path = new Path(dir);
    JSONObject json;
    BufferedReader br = null;
    IndexFileBufferedReader ifbr = null;

    try {
      FileStatus fileStatus = fs.getFileStatus(path);
      if (!fileStatus.isDirectory()) {
        return null;
      }

      // META file processing
      br = new BufferedReader(new InputStreamReader(fs.open(new Path(dir, FSPartFileCollection.META_FILE))));
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
      ifbr = new IndexFileBufferedReader(new InputStreamReader(fs.open(new Path(dir, FSPartFileCollection.INDEX_FILE))), dir);
      StatsIndexLine indexLine;
      while ((indexLine = (StatsIndexLine)ifbr.readIndexLine()) != null) {
        if (indexLine.isEndLine) {
          info.ended = true;
        }
        else {
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
      LOG.warn("Got exception when reading operators info", ex);
      return null;
    }
    finally {
      try {
        if (ifbr != null) {
          ifbr.close();
        }
        if (br != null) {
          br.close();
        }
      }
      catch (IOException ex) {
      }
    }

    return info;
  }

  public List<OperatorStats> getOperatorsStats(String appId, String opName, Long startTime, Long endTime)
  {
    List<OperatorStats> result = new ArrayList<OperatorStats>();
    String dir = getOperatorStatsDirectory(appId, opName);
    if (dir == null) {
      return null;
    }

    IndexFileBufferedReader ifbr = null;

    try {
      ifbr = new IndexFileBufferedReader(new InputStreamReader(fs.open(new Path(dir, FSPartFileCollection.INDEX_FILE))), dir);
      StatsIndexLine indexLine;
      while ((indexLine = (StatsIndexLine)ifbr.readIndexLine()) != null) {
        if (indexLine.isEndLine) {
          continue;
        }
        else {

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

          BufferedReader partBr = new BufferedReader(new InputStreamReader(fs.open(new Path(dir, indexLine.partFile))));
          try {
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
          finally {
            partBr.close();
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
      }
      catch (IOException ex) {
      }
    }
    return result;
  }

  public List<ContainerStats> getContainersStats(String appId, Long startTime, Long endTime)
  {
    List<ContainerStats> result = new ArrayList<ContainerStats>();
    String dir = getContainerStatsDirectory(appId);
    if (dir == null) {
      return null;
    }
    BufferedReader br = null;

    try {
      br = new BufferedReader(new InputStreamReader(fs.open(new Path(dir, FSPartFileCollection.INDEX_FILE))));
      String line;

      while ((line = br.readLine()) != null) {
        if (!line.startsWith("F:")) {
          continue;
        }
        StatsIndexLine indexLine = parseIndexLine(line);
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

        BufferedReader partBr = new BufferedReader(new InputStreamReader(fs.open(new Path(dir, indexLine.partFile))));
        try {
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
        finally {
          partBr.close();
        }
      }
    }
    catch (Exception ex) {
      LOG.warn("Got exception when reading containers stats", ex);
    }
    finally {
      try {
        if (br != null) {
          br.close();
        }
      }
      catch (IOException ex) {
      }
    }
    return result;
  }

}
