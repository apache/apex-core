/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.StramWebServices;
import com.sun.jersey.api.client.WebResource;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import org.apache.hadoop.fs.*;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class AlertsAgent extends StramAgent
{
  private static final Logger LOG = LoggerFactory.getLogger(AlertsAgent.class);

  public static String getAlertTemplatesDirectory(String stramRoot)
  {
    return stramRoot + Path.SEPARATOR + "alertTemplates";
  }

  public void createAlert(String appId, String name, String streamName,
          String stramRoot, String templateName, Map<String, String> parameters) throws AppNotFoundException, IOException, JSONException
  {
    WebServicesClient webServicesClient = new WebServicesClient();
    WebResource wr = StramAgent.getStramWebResource(webServicesClient, appId);
    if (wr == null) {
      LOG.warn("Web resource not found for appId {}", appId);
      throw new AppNotFoundException(appId);
    }
    JSONObject tmplJson = new JSONObject(getAlertTemplate(stramRoot, templateName));
    tmplJson.remove("parameters");
    tmplJson = replaceTemplate(tmplJson, parameters);
    final JSONObject json = tmplJson;
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

  private JSONObject replaceTemplate(JSONObject tmplJson, Map<String, String> parameters) throws JSONException
  {
    JSONObject result = new JSONObject();
    @SuppressWarnings("unchecked")
    Iterator<String> keys = tmplJson.keys();
    while (keys.hasNext()) {
      String key = keys.next();
      Object val = tmplJson.get(key);
      result.put(key, replaceObject(val, parameters));
    }
    return result;
  }

  private Object replaceObject(Object val, Map<String, String> parameters) throws JSONException
  {
    if (val instanceof JSONObject) {
      return replaceTemplate((JSONObject)val, parameters);
    }
    else if (val instanceof String) {
      String strval = (String)val;
      int cur = 0;
      StringBuilder sb = new StringBuilder();
      while (cur > 0 && cur < strval.length()) {
        int begin = strval.indexOf("${", cur);
        if (begin != -1) {
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

  public void deleteAlert(String appId, String name) throws AppNotFoundException, IOException
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

  public void createAlertTemplate(String stramRoot, String name, String content) throws IOException
  {
    String dir = getAlertTemplatesDirectory(stramRoot);
    Path path = new Path(dir);

    FileStatus fileStatus = fs.getFileStatus(path);
    if (!fileStatus.isDirectory()) {
      throw new FileNotFoundException("Cannot read directory " + dir);
    }
    createFile(path, content.getBytes());
  }

  public void deleteAlertTemplate(String stramRoot, String name) throws IOException
  {
    String dir = getAlertTemplatesDirectory(stramRoot);
    Path path = new Path(dir);

    FileStatus fileStatus = fs.getFileStatus(path);
    if (!fileStatus.isDirectory()) {
      throw new FileNotFoundException("Cannot read directory " + dir);
    }
    path = new Path(path, name);
    deleteFile(path);
  }

  public Map<String, String> listAlertTemplates(String stramRoot) throws IOException
  {
    String dir = getAlertTemplatesDirectory(stramRoot);
    Map<String, String> map = new HashMap<String, String>();
    Path path = new Path(dir);

    FileStatus fileStatus = fs.getFileStatus(path);
    if (!fileStatus.isDirectory()) {
      throw new FileNotFoundException("Cannot read directory " + dir);
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

  public String getAlertTemplate(String stramRoot, String name) throws IOException
  {
    String dir = getAlertTemplatesDirectory(stramRoot);
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
