/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import com.datatorrent.stram.StramClient;
import com.datatorrent.stram.util.LRUCache;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.WebServices;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import java.util.Map;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Abstract StramAgent class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.3
 */
public class StramAgent extends FSAgent
{
  private static class StramWebServicesInfo
  {
    StramWebServicesInfo(String appMasterTrackingUrl, String version, String appPath)
    {
      this.appMasterTrackingUrl = appMasterTrackingUrl;
      this.version = version;
      this.appPath = appPath;
    }

    String appMasterTrackingUrl;
    String version;
    String appPath;
  }

  private static final Logger LOG = LoggerFactory.getLogger(StramAgent.class);
  protected static String resourceManagerWebappAddress;
  private static Map<String, StramWebServicesInfo> webServicesInfoMap = new LRUCache<String, StramWebServicesInfo>(100);
  protected static String defaultStramRoot = null;

  public class AppNotFoundException extends Exception
  {
    private static final long serialVersionUID = 1L;
    private String appId;

    public AppNotFoundException(String appId)
    {
      this.appId = appId;
    }

    @Override
    public String toString()
    {
      return "App id " + appId + " is not found";
    }

  }

  public static void setResourceManagerWebappAddress(String addr)
  {
    resourceManagerWebappAddress = addr;
  }

  public static void setDefaultStramRoot(String dir)
  {
    defaultStramRoot = dir;
  }

  private static synchronized void deleteCachedWebServicesInfo(String appid)
  {
    webServicesInfoMap.remove(appid);
  }

  private static synchronized void setCachedWebServicesInfo(String appid, StramWebServicesInfo info)
  {
    webServicesInfoMap.put(appid, info);
  }

  private static synchronized StramWebServicesInfo getCachedSebServicesInfo(String appid)
  {
    return webServicesInfoMap.get(appid);
  }

  private static synchronized StramWebServicesInfo getWebServicesInfo(String appid)
  {
    StramWebServicesInfo info = getCachedSebServicesInfo(appid);
    if (info == null) {
      info = retrieveWebServicesInfo(appid);
      if (info != null) {
        setCachedWebServicesInfo(appid, info);
      }
    }
    return info;
  }

  public static String getWebServicesVersion(String appid)
  {
    return getWebServicesInfo(appid).version;
  }

  public static WebResource getStramWebResource(WebServicesClient webServicesClient, String appid)
  {
    Client wsClient = webServicesClient.getClient();
    wsClient.setFollowRedirects(true);
    StramWebServicesInfo info = getWebServicesInfo(appid);
    return info == null ? null : wsClient.resource("http://" + info.appMasterTrackingUrl).path(WebServices.PATH).path(info.version).path("stram");
  }

  public static void invalidateStramWebResource(String appid)
  {
    deleteCachedWebServicesInfo(appid);
  }

  public String getDefaultStramRoot()
  {
    return (defaultStramRoot == null) ? (fs.getHomeDirectory() + "/" + StramClient.DEFAULT_APPNAME) : defaultStramRoot;
  }

  public String getAppPath(String appId)
  {
    try {
      return getWebServicesInfo(appId).appPath;
    }
    catch (Exception ex) {
      return getDefaultStramRoot() + "/" + appId;
    }
  }

  private static StramWebServicesInfo retrieveWebServicesInfo(String appId)
  {
    WebServicesClient webServicesClient = new WebServicesClient();
    try {
      String url = "http://" + resourceManagerWebappAddress + "/proxy/" + appId + WebServices.PATH;
      JSONObject response = new JSONObject(webServicesClient.process(url,
                                                                     String.class,
                                                                     new WebServicesClient.GetWebServicesHandler<String>()));
      String version = response.getString("version");
      response = webServicesClient.process(url + "/" + version + "/stram/info",
                                           JSONObject.class,
                                           new WebServicesClient.GetWebServicesHandler<JSONObject>());
      String appMasterUrl = response.getString("appMasterTrackingUrl");
      String appPath = response.getString("appPath");
      return new StramWebServicesInfo(appMasterUrl, version, appPath);
    }
    catch (Exception ex) {
      //LOG.debug("Caught exception when retrieving web service info for app " + appId, ex);
      return null;
    }
  }

}
