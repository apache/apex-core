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
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.ser.std.ToStringSerializer;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.datatorrent.common.util.ObjectMapperString;
import com.datatorrent.stram.util.FSPartFileCollection;

/**
 * <p>StatsAgent class.</p>
 *
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
    @JsonSerialize(using = ToStringSerializer.class)
    public long startTime;
    @JsonSerialize(using = ToStringSerializer.class)
    public long endTime;
    @JsonSerialize(using = ToStringSerializer.class)
    public long count;
    public boolean ended;
  }

  public static class OperatorsInfo
  {
    public String appId;
    public String operatorName;
    public List<Integer> operatorIds;
    @JsonSerialize(using = ToStringSerializer.class)
    public long startTime;
    @JsonSerialize(using = ToStringSerializer.class)
    public long endTime;
    @JsonSerialize(using = ToStringSerializer.class)
    public long count;
    public boolean ended;
  }

  private static class StatsIndexLine extends IndexLine
  {
    @JsonSerialize(using = ToStringSerializer.class)
    public long startTime;
    @JsonSerialize(using = ToStringSerializer.class)
    public long endTime;
    @JsonSerialize(using = ToStringSerializer.class)
    public long count;
  }

  public static class OperatorStatsInfo
  {
    public int operatorId;
    @JsonSerialize(using = ToStringSerializer.class)
    public long timestamp;
    public ObjectMapperString stats;
  }

  public static class ContainerStatsInfo
  {
    public int containerId;
    @JsonSerialize(using = ToStringSerializer.class)
    public long timestamp;
    public ObjectMapperString stats;
  }

  public StatsAgent(StramAgent stramAgent)
  {
    super(stramAgent);
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
    String appPath = stramAgent.getAppPath(appId);
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
    info.containers = new HashMap<>();
    String dir = getContainerStatsDirectory(appId);
    if (dir == null) {
      return null;
    }
    Path path = new Path(dir);
    JSONObject json;
    BufferedReader br = null;
    IndexFileBufferedReader ifbr = null;
    try {
      FileStatus fileStatus = stramAgent.getFileSystem().getFileStatus(path);
      if (!fileStatus.isDirectory()) {
        return null;
      }

      // META file processing
      br = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, FSPartFileCollection.META_FILE))));
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
      ifbr = new IndexFileBufferedReader(new InputStreamReader(stramAgent.getFileSystem()
          .open(new Path(dir, FSPartFileCollection.INDEX_FILE))), dir);
      StatsIndexLine indexLine;

      while ((indexLine = (StatsIndexLine)ifbr.readIndexLine()) != null) {
        if (indexLine.isEndLine) {
          info.ended = true;
        } else {
          info.count += indexLine.count;
          if (info.startTime == 0 || info.startTime > indexLine.startTime) {
            info.startTime = indexLine.startTime;
          }
          if (info.endTime == 0 || info.endTime < indexLine.endTime) {
            info.endTime = indexLine.endTime;
          }
        }
      }
    } catch (Exception ex) {
      LOG.warn("Got exception when reading containers info", ex);
      return null;
    } finally {
      IOUtils.closeQuietly(br);
      IOUtils.closeQuietly(ifbr);
    }

    return info;
  }

  public OperatorsInfo getOperatorsInfo(String appId, String opName)
  {
    OperatorsInfo info = new OperatorsInfo();
    info.appId = appId;
    info.operatorName = opName;
    info.operatorIds = new ArrayList<>();
    String dir = getOperatorStatsDirectory(appId, opName);
    if (dir == null) {
      return null;
    }

    Path path = new Path(dir);
    JSONObject json;
    BufferedReader br = null;
    IndexFileBufferedReader ifbr = null;

    try {
      FileStatus fileStatus = stramAgent.getFileSystem().getFileStatus(path);
      if (!fileStatus.isDirectory()) {
        return null;
      }

      // META file processing
      br = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, FSPartFileCollection.META_FILE))));
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
      ifbr = new IndexFileBufferedReader(new InputStreamReader(stramAgent.getFileSystem()
          .open(new Path(dir, FSPartFileCollection.INDEX_FILE))), dir);
      StatsIndexLine indexLine;
      while ((indexLine = (StatsIndexLine)ifbr.readIndexLine()) != null) {
        if (indexLine.isEndLine) {
          info.ended = true;
        } else {
          info.count += indexLine.count;
          if (info.startTime == 0 || info.startTime > indexLine.startTime) {
            info.startTime = indexLine.startTime;
          }
          if (info.endTime == 0 || info.endTime < indexLine.endTime) {
            info.endTime = indexLine.endTime;
          }
        }
      }
    } catch (Exception ex) {
      LOG.warn("Got exception when reading operators info", ex);
      return null;
    } finally {
      IOUtils.closeQuietly(ifbr);
      IOUtils.closeQuietly(br);
    }

    return info;
  }

  public List<OperatorStatsInfo> getOperatorsStats(String appId, String opName, Long startTime, Long endTime)
  {
    List<OperatorStatsInfo> result = new ArrayList<>();
    String dir = getOperatorStatsDirectory(appId, opName);
    if (dir == null) {
      return null;
    }

    IndexFileBufferedReader ifbr = null;

    try {
      ifbr = new IndexFileBufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, FSPartFileCollection.INDEX_FILE))), dir);
      StatsIndexLine indexLine;
      String lastProcessPartFile = null;
      while ((indexLine = (StatsIndexLine)ifbr.readIndexLine()) != null) {
        if (!indexLine.isEndLine) {
          lastProcessPartFile = indexLine.partFile;
          if (startTime != null) {
            if (startTime > indexLine.endTime) {
              continue;
            }
          }

          if (endTime != null) {
            if (endTime < indexLine.startTime) {
              return result;
            }
          }

          try (BufferedReader partBr = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, indexLine.partFile))))) {
            processOperatorPartFile(partBr, startTime, endTime, result);
          }
        }
      }

      BufferedReader partBr = null;
      try {
        String extraPartFile = getNextPartFile(lastProcessPartFile);
        if (extraPartFile != null) {
          partBr = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem()
              .open(new Path(dir, extraPartFile))));
          processOperatorPartFile(partBr, startTime, endTime, result);
        }
      } catch (Exception ex) {
        // ignore
      } finally {
        IOUtils.closeQuietly(partBr);
      }
    } catch (Exception ex) {
      LOG.warn("Got exception when reading operators stats", ex);
    } finally {
      IOUtils.closeQuietly(ifbr);
    }
    return result;
  }

  private void processOperatorPartFile(BufferedReader partBr, Long startTime, Long endTime, List<OperatorStatsInfo> result) throws IOException
  {
    String partLine;
    // advance until offset is reached
    while ((partLine = partBr.readLine()) != null) {
      OperatorStatsInfo os = new OperatorStatsInfo();
      int cursor = 0;
      int cursor2;
      cursor2 = partLine.indexOf(':', cursor);
      os.operatorId = Integer.valueOf(partLine.substring(cursor, cursor2));
      cursor = cursor2 + 1;
      cursor2 = partLine.indexOf(':', cursor);
      os.timestamp = Long.valueOf(partLine.substring(cursor, cursor2));
      cursor = cursor2 + 1;
      os.stats = new ObjectMapperString(partLine.substring(cursor));
      if ((startTime == null || os.timestamp >= startTime) && (endTime == null || os.timestamp <= endTime)) {
        result.add(os);
      }
    }
  }

  public List<ContainerStatsInfo> getContainersStats(String appId, Long startTime, Long endTime)
  {
    List<ContainerStatsInfo> result = new ArrayList<>();
    String dir = getContainerStatsDirectory(appId);
    if (dir == null) {
      return null;
    }
    BufferedReader br = null;
    String lastProcessPartFile = null;
    try {
      br = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new Path(dir, FSPartFileCollection.INDEX_FILE))));
      String line;

      while ((line = br.readLine()) != null) {
        if (!line.startsWith("F:")) {
          continue;
        }
        StatsIndexLine indexLine = parseIndexLine(line);
        lastProcessPartFile = indexLine.partFile;
        if (startTime != null) {
          if (startTime > indexLine.endTime) {
            continue;
          }
        }

        if (endTime != null) {
          if (endTime < indexLine.startTime) {
            return result;
          }
        }

        try (BufferedReader partBr = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem().open(new
            Path(dir, indexLine.partFile))))) {
          processContainerPartFile(partBr, startTime, endTime, result);
        }
      }
      BufferedReader partBr = null;
      try {
        String extraPartFile = getNextPartFile(lastProcessPartFile);
        if (extraPartFile != null) {
          partBr = new BufferedReader(new InputStreamReader(stramAgent.getFileSystem()
              .open(new Path(dir, extraPartFile))));
          processContainerPartFile(partBr, startTime, endTime, result);
        }
      } catch (Exception ex) {
        // ignore
      } finally {
        IOUtils.closeQuietly(partBr);
      }
    } catch (Exception ex) {
      LOG.warn("Got exception when reading containers stats", ex);
    } finally {
      IOUtils.closeQuietly(br);
    }
    return result;
  }

  private void processContainerPartFile(BufferedReader partBr, Long startTime, Long endTime, List<ContainerStatsInfo> result) throws IOException
  {
    String partLine;
    while ((partLine = partBr.readLine()) != null) {
      ContainerStatsInfo cs = new ContainerStatsInfo();
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
