/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import com.datatorrent.stram.util.LRUCache;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.StramWebServices;
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
public class StramAgent extends HdfsAgent
{
  private static final Logger LOG = LoggerFactory.getLogger(StramAgent.class);
  protected static String resourceManagerWebappAddress;
  private static Map<String, String> appMasterTrackingUrls = new LRUCache<String, String>(100);

  public class AppNotFoundException extends Exception {
    private static final long serialVersionUID = 1L;
    private String appId;
    public AppNotFoundException(String appId) {
      this.appId = appId;
    }
    @Override
    public String toString() {
      return "App id " + appId + " is not found";
    }
  }

  public static void setResourceManagerWebappAddress(String addr)
  {
    resourceManagerWebappAddress = addr;
  }

  private static synchronized void deleteAppMasterUrl(String appid)
  {
    appMasterTrackingUrls.remove(appid);
  }

  private static synchronized void setAppMasterTrackingUrl(String appid, String trackingUrl)
  {
    appMasterTrackingUrls.put(appid, trackingUrl);
  }

  private static synchronized String getAppMasterUrl(String appid)
  {
    return appMasterTrackingUrls.get(appid);
  }

  public static WebResource getStramWebResource(WebServicesClient webServicesClient, String appid)
  {
    Client wsClient = webServicesClient.getClient();
    wsClient.setFollowRedirects(true);
    String trackingUrl = getAppMasterUrl(appid);
    if (trackingUrl == null) {
      trackingUrl = getAppMasterTrackingUrl(appid);
      if (trackingUrl != null) {
        setAppMasterTrackingUrl(appid, trackingUrl);
      }
    }
    return trackingUrl == null ? null : wsClient.resource("http://" + trackingUrl).path(StramWebServices.PATH);
  }

  public static String getDefaultStramRoot()
  {
    return "/user/" + System.getProperty("user.name") + "/Stram";
  }

  public static String getAppPath(String appId)
  {
    WebServicesClient webServicesClient = new WebServicesClient();
    try {
      JSONObject response = webServicesClient.process("http://" + resourceManagerWebappAddress + "/proxy/" + appId + "/ws/v1/stram/info",
                                                      JSONObject.class,
                                                      new WebServicesClient.GetWebServicesHandler<JSONObject>());
      String appPath = response.getJSONObject("info").getString("appPath");
      int i = appPath.indexOf("/" + appId);
      if (i <= 0) {
        LOG.warn("Cannot get the live Stram root for {}", appId);
        return null;
      }
      return appPath.substring(0, i);
    }
    catch (Exception ex) {
      return getDefaultStramRoot() + "/" + appId;
    }
  }

  private static String getAppMasterTrackingUrl(String appId)
  {
    WebServicesClient webServicesClient = new WebServicesClient();
    try {
      JSONObject response = webServicesClient.process("http://" + resourceManagerWebappAddress + "/proxy/" + appId + "/ws/v1/stram/info",
                                                      JSONObject.class,
                                                      new WebServicesClient.GetWebServicesHandler<JSONObject>());
      return response.getJSONObject("info").getString("appMasterTrackingUrl");
    }
    catch (Exception ex) {
      LOG.warn("Cannot get the live Stram root for {}", appId);
      return null;
    }
  }

}
