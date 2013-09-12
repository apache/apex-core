/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import com.datatorrent.stram.util.HdfsPartFileCollection;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.StramWebServices;
import com.sun.jersey.api.client.WebResource;
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
public class EventsAgent extends StramAgent
{
  private static final Logger LOG = LoggerFactory.getLogger(EventsAgent.class);

  private static class IndexLine
  {
    public long startTime;
    public long endTime;
    public long count;
    public String partFile;
  }

  public static class Event
  {
    public long timestamp;
    public String type;
    public Map<String, Object> data;
  }

  private String getEventsDirectory(String appId)
  {
    String stramRoot = getAppPath(appId);
    if (stramRoot == null) {
      return null;
    }
    return stramRoot + Path.SEPARATOR + appId + Path.SEPARATOR + "events";
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

  @SuppressWarnings("unchecked")
  public List<Event> getEvents(String appId, Long startTime, Long endTime)
  {
    List<Event> result = new ArrayList<Event>();
    String dir = getEventsDirectory(appId);
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
          Event ev = new Event();
          int cursor = 0;
          int cursor2;
          cursor2 = partLine.indexOf(':', cursor);
          ev.timestamp = Long.valueOf(partLine.substring(cursor, cursor2));
          cursor = cursor2 + 1;
          cursor2 = partLine.indexOf(':', cursor);
          ev.type = partLine.substring(cursor, cursor2);
          cursor = cursor2 + 1;
          if ((startTime != null || ev.timestamp >= startTime) && (endTime != null || ev.timestamp <= endTime)) {
            ev.data = new ObjectMapper().readValue(partLine.substring(cursor), HashMap.class);
            result.add(ev);
          }
        }
      }
    }
    catch (Exception ex) {
      LOG.warn("Got exception when reading operators stats", ex);
    }
    return result;
  }

  public void syncEvents(String appId) throws AppNotFoundException, IOException
  {
    WebServicesClient webServicesClient = new WebServicesClient();
    WebResource wr = getStramWebResource(webServicesClient, appId);
    if (wr == null) {
      throw new AppNotFoundException(appId);
    }
    webServicesClient.process(wr.path(StramWebServices.PATH_SYNCEVENTS), String.class,
                              new WebServicesClient.GetWebServicesHandler<String>());

  }

}
