/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import com.datatorrent.stram.StramClient;
import com.datatorrent.stram.util.LRUCache;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.StramWebServices;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
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
  private static final Logger LOG = LoggerFactory.getLogger(StramAgent.class);
  protected static String resourceManagerWebappAddress;
  private static Map<String, String> appMasterTrackingUrls = new LRUCache<String, String>(100);
  protected static String defaultStramRoot = null;

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

  public static void setDefaultStramRoot(String dir)
  {
    defaultStramRoot = dir;
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

  public static void invalidateStramWebResource(String appid)
  {
    deleteAppMasterUrl(appid);
  }

  public String getDefaultStramRoot()
  {
    return (defaultStramRoot == null) ? (fs.getHomeDirectory() + "/" + StramClient.DEFAULT_APPNAME) : defaultStramRoot;
  }

  public String getAppPath(String appId)
  {
    WebServicesClient webServicesClient = new WebServicesClient();
    try {
      JSONObject response = webServicesClient.process("http://" + resourceManagerWebappAddress + "/proxy/" + appId + "/ws/v1/stram/info",
                                                      JSONObject.class,
                                                      new WebServicesClient.GetWebServicesHandler<JSONObject>());
      String appPath = response.getString("appPath");
      int i = appPath.indexOf("/" + appId);
      if (i <= 0) {
        LOG.warn("Cannot get the app path for {} {}", appId, appPath);
        return null;
      }
      return appPath;
    }
    catch (Exception ex) {
      return getDefaultStramRoot() + "/" + appId;
    }
  }

  private static String getAppMasterTrackingUrl(String appId)
  {
    // Currently proxy does not support secure mode hence using rpc to get the tracking url in that case
    if (!UserGroupInformation.isSecurityEnabled()) {
      WebServicesClient webServicesClient = new WebServicesClient();
      String url = "http://" + resourceManagerWebappAddress + "/proxy/" + appId + "/ws/v1/stram/info";
      try {
        JSONObject response = webServicesClient.process(url,
                                                        JSONObject.class,
                                                        new WebServicesClient.GetWebServicesHandler<JSONObject>());
        return response.getString("appMasterTrackingUrl");
      }
      catch (Exception ex) {
        LOG.warn("Cannot get the tracking url for {} from {}", appId, url);
        LOG.warn("Caught exception", ex);
        return null;
      }
    } else {
      StramClientUtils.YarnClientHelper yarnClient = new StramClientUtils.YarnClientHelper(new Configuration());
      try {
        StramClientUtils.ClientRMHelper clientRM = new StramClientUtils.ClientRMHelper(yarnClient);
        ApplicationReport report = clientRM.getApplicationReport(appId);
        if (report != null) {
          return report.getOriginalTrackingUrl();
        } else {
          LOG.warn("No matching application found for {} in yarn", appId);
          return null;
        }
      } catch (Exception ex) {
        LOG.warn("Cannot get the tracking url for {} from yarn", appId);
        LOG.warn("Caught exception", ex);
        return null;
      }
    }
  }

}
