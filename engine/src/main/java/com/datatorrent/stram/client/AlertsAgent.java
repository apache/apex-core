/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import com.datatorrent.stram.client.WebServicesVersionConversion.IncompatibleVersionException;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.StramWebServices;
import com.sun.jersey.api.client.WebResource;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>AlertsAgent class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.5
 */
public class AlertsAgent extends StramAgent
{
  private static final Logger LOG = LoggerFactory.getLogger(AlertsAgent.class);

  public AlertsAgent(Configuration conf)
  {
    super(conf);
  }

  private String getAlertTemplatesDirectory()
  {
    return getAppsRoot() + Path.SEPARATOR + "alertTemplates";
  }

  public void createAlert(String appId, String name, String streamName, String templateName, Map<String, String> parameters) throws AppNotFoundException, IncompatibleVersionException, IOException, JSONException
  {
    WebServicesClient webServicesClient = new WebServicesClient();
    WebResource wr = StramAgent.getStramWebResource(webServicesClient, appId);
    if (wr == null) {
      LOG.warn("Web resource not found for appId {}", appId);
      throw new AppNotFoundException(appId);
    }
    JSONObject tmplJson = new JSONObject(getAlertTemplate(templateName));
    tmplJson.remove("parameters");
    tmplJson = (JSONObject)replaceObject(tmplJson, parameters);
    tmplJson.put("streamName", streamName);
    JSONObject createFrom = new JSONObject();
    createFrom.put("templateName", templateName);
    createFrom.put("parameters", new JSONObject(parameters));
    tmplJson.put("createFrom", createFrom);
    final JSONObject json = tmplJson;
    LOG.debug("Sending create alert to {}: {}", wr.path(StramWebServices.PATH_ALERTS).path(name).toString(), json.toString());
    webServicesClient.process(wr.path(StramWebServices.PATH_ALERTS).path(name), String.class,
                              new WebServicesClient.WebServicesHandler<String>()
    {
      @Override
      public String process(WebResource webResource, Class<String> clazz)
      {
        return webResource.type(MediaType.APPLICATION_JSON).put(clazz, json.toString());
      }

    });
  }

  private Object replaceObject(Object val, Map<String, String> parameters) throws JSONException
  {
    if (val instanceof JSONObject) {
      JSONObject obj = (JSONObject)val;
      @SuppressWarnings("unchecked")
      Iterator<String> keys = obj.keys();
      while (keys.hasNext()) {
        String key = keys.next();
        obj.put(key, replaceObject(obj.get(key), parameters));
      }
      return obj;
    }
    else if (val instanceof JSONArray) {
      JSONArray arr = (JSONArray)val;
      for (int i = 0; i < arr.length(); i++) {
        arr.put(i, replaceObject(arr.get(i), parameters));
      }
      return arr;
    }
    else if (val instanceof String) {
      String strval = (String)val;
      int cur = 0;
      StringBuilder sb = new StringBuilder();
      while (cur >= 0 && cur < strval.length()) {
        int begin = strval.indexOf("${", cur);
        if (begin != -1) {
          sb.append(strval.substring(cur, begin));
          int end = strval.indexOf('}', cur);
          if (end != -1) {
            String varName = strval.substring(begin + 2, end);
            if (parameters.containsKey(varName)) {
              sb.append(parameters.get(varName));
            }
            cur = end + 1;
          }
        }
        else {
          sb.append(strval.substring(cur));
          break;
        }
      }
      return sb.toString();
    }
    else {
      return val;
    }
  }

  public void deleteAlert(String appId, String name) throws AppNotFoundException, IncompatibleVersionException, IOException
  {
    WebServicesClient webServicesClient = new WebServicesClient();
    WebResource wr = StramAgent.getStramWebResource(webServicesClient, appId);
    if (wr == null) {
      LOG.warn("Web resource not found for appId {}", appId);
      throw new AppNotFoundException(appId);
    }

    webServicesClient.process(wr.path(StramWebServices.PATH_ALERTS).path(name), String.class,
                              new WebServicesClient.WebServicesHandler<String>()
    {
      @Override
      public String process(WebResource webResource, Class<String> clazz)
      {
        return webResource.delete(clazz);
      }

    });
  }

  public void createAlertTemplate(String name, String content) throws IOException
  {
    String dir = getAlertTemplatesDirectory();
    Path path = new Path(dir);
    fs.mkdirs(path);
    FileStatus fileStatus = fs.getFileStatus(path);
    if (!fileStatus.isDirectory()) {
      throw new FileNotFoundException("Cannot read directory " + dir);
    }
    createFile(new Path(path, name), content.getBytes());
  }

  public void deleteAlertTemplate(String name) throws IOException
  {
    String dir = getAlertTemplatesDirectory();
    Path path = new Path(dir);

    FileStatus fileStatus = fs.getFileStatus(path);
    if (!fileStatus.isDirectory()) {
      throw new FileNotFoundException("Cannot read directory " + dir);
    }
    path = new Path(path, name);
    deleteFile(path);
  }

  public Map<String, String> listAlertTemplates() throws IOException
  {
    String dir = getAlertTemplatesDirectory();
    Map<String, String> map = new HashMap<String, String>();
    Path path = new Path(dir);

    FileStatus fileStatus;
    try {
      fileStatus = fs.getFileStatus(path);
    }
    catch (FileNotFoundException ex) {
      return map;
    }

    if (!fileStatus.isDirectory()) {
      return map;
    }
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, false);
    while (it.hasNext()) {
      LocatedFileStatus lfs = it.next();
      FSDataInputStream is = fs.open(lfs.getPath());
      byte[] bytes = new byte[is.available()];
      is.readFully(bytes);
      String content = new String(bytes);
      map.put(lfs.getPath().getName(), content);
    }
    return map;
  }

  public String getAlertTemplate(String name) throws IOException
  {
    String dir = getAlertTemplatesDirectory();
    Path path = new Path(dir);

    FileStatus fileStatus = fs.getFileStatus(path);
    if (!fileStatus.isDirectory()) {
      throw new FileNotFoundException("Cannot read directory " + dir);
    }
    FSDataInputStream is = fs.open(new Path(path, name));
    byte[] bytes = new byte[is.available()];
    is.readFully(bytes);
    return new String(bytes);
  }

}
