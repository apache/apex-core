/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import com.datatorrent.stram.StramClient;
import com.datatorrent.stram.client.WebServicesVersionConversion.IncompatibleVersionException;
import com.datatorrent.stram.client.WebServicesVersionConversion.VersionConversionFilter;
import com.datatorrent.stram.security.StramWSFilter;
import com.datatorrent.stram.util.HeaderClientFilter;
import com.datatorrent.stram.util.LRUCache;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.WebServices;
import com.sun.jersey.api.client.*;

import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.NewCookie;

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
    StramWebServicesInfo(String appMasterTrackingUrl, String version, String appPath, String secToken)
    {
      this.appMasterTrackingUrl = appMasterTrackingUrl;
      this.version = version;
      this.appPath = appPath;
      if (secToken != null) {
        securityInfo = new SecurityInfo(secToken);
      }
    }

    String appMasterTrackingUrl;
    String version;
    String appPath;
    SecurityInfo securityInfo;
  }

  private static class SecurityInfo {
    public static final long DEFAULT_EXPIRY_INTERVAL = 60 * 60 * 1000;

    HeaderClientFilter secClientFilter;
    long expiryInterval = DEFAULT_EXPIRY_INTERVAL;
    long issueTime;

    SecurityInfo(String secToken) {
      issueTime = System.currentTimeMillis();
      secClientFilter = new HeaderClientFilter();
      secClientFilter.addCookie(new Cookie(StramWSFilter.CLIENT_COOKIE, secToken));
    }

    boolean isExpiredToken() {
      return ((System.currentTimeMillis() - issueTime) >= expiryInterval);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(StramAgent.class);
  protected static String resourceManagerWebappAddress;
  private static Map<String, StramWebServicesInfo> webServicesInfoMap = new LRUCache<String, StramWebServicesInfo>(100, true);
  protected static String defaultStramRoot = null;

  public class AppNotFoundException extends Exception
  {
    private static final long serialVersionUID = 1L;
    private final String appId;

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

  private static synchronized StramWebServicesInfo getCachedWebServicesInfo(String appid)
  {
    return webServicesInfoMap.get(appid);
  }

  private static synchronized StramWebServicesInfo getWebServicesInfo(String appid)
  {
    StramWebServicesInfo info = getCachedWebServicesInfo(appid);
    if ((info == null) || checkSecExpiredToken(appid, info)) {
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

  public static WebResource getStramWebResource(WebServicesClient webServicesClient, String appid) throws IncompatibleVersionException
  {
    Client wsClient = webServicesClient.getClient();
    wsClient.setFollowRedirects(true);
    StramWebServicesInfo info = getWebServicesInfo(appid);
    WebResource ws = null;
    if (info != null) {
      //ws = wsClient.resource("http://" + info.appMasterTrackingUrl).path(WebServices.PATH).path(info.version).path("stram");
      // the filter should convert to the right version
      ws = wsClient.resource("http://" + info.appMasterTrackingUrl).path(WebServices.PATH).path(WebServices.VERSION).path("stram");
      WebServicesVersionConversion.Converter versionConverter = WebServicesVersionConversion.getConverter(info.version);
      if (versionConverter != null) {
        VersionConversionFilter versionConversionFilter = new VersionConversionFilter(versionConverter);
        if (!wsClient.isFilterPreset(versionConversionFilter)) {
          wsClient.addFilter(versionConversionFilter);
        }
      }
      if (info.securityInfo != null) {
        if (!wsClient.isFilterPreset(info.securityInfo.secClientFilter)) {
          wsClient.addFilter(info.securityInfo.secClientFilter);
        }
      }
    }
    return ws;
  }

  public static void invalidateStramWebResource(String appid)
  {
    deleteCachedWebServicesInfo(appid);
  }

  public String getDefaultStramRoot()
  {
    return (defaultStramRoot == null) ? (StramClientUtils.getDTRootDir(fs, conf) + "/" + StramClient.DEFAULT_APPNAME) : defaultStramRoot;
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
    String url = "http://" + resourceManagerWebappAddress + "/proxy/" + appId + WebServices.PATH;
    /*
    // Currently proxy does not support secure mode hence using rpc to get the tracking url in that case
    if (UserGroupInformation.isSecurityEnabled()) {
      StramClientUtils.YarnClientHelper yarnClient = new StramClientUtils.YarnClientHelper(new Configuration());
      try {
        StramClientUtils.ClientRMHelper clientRM = new StramClientUtils.ClientRMHelper(yarnClient);
        ApplicationReport report = clientRM.getApplicationReport(appId);
        if (report != null) {
          url = "http://" + report.getOriginalTrackingUrl() + WebServices.PATH;
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
    */
    WebServicesClient webServicesClient = new WebServicesClient();
    try {
      JSONObject response;
      String secToken = null;
      if (!UserGroupInformation.isSecurityEnabled()) {
        response = new JSONObject(webServicesClient.process(url,
                                                             String.class,
                                                             new WebServicesClient.GetWebServicesHandler<String>()));
      } else {
        ClientResponse clientResponse = webServicesClient.process(url,
                                                                  ClientResponse.class,
                                                                  new WebServicesClient.GetWebServicesHandler<ClientResponse>());
        if (UserGroupInformation.isSecurityEnabled()) {
          for (NewCookie nc : clientResponse.getCookies()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Cookie " + nc.getName() + " " + nc.getValue());
            }
            if (nc.getName().equals(StramWSFilter.CLIENT_COOKIE)) {
              secToken = nc.getValue();
            }
          }
        }
        response = new JSONObject(clientResponse.getEntity(String.class));
      }
      String version = response.getString("version");
      response = webServicesClient.process(url + "/" + version + "/stram/info",
                                           JSONObject.class,
                                           new WebServicesClient.GetWebServicesHandler<JSONObject>());
      String appMasterUrl = response.getString("appMasterTrackingUrl");
      String appPath = response.getString("appPath");
      return new StramWebServicesInfo(appMasterUrl, version, appPath, secToken);
    }
    catch (Exception ex) {
      LOG.debug("Caught exception when retrieving web service info for app " + appId, ex);
      return null;
    }
  }

  private static boolean checkSecExpiredToken(String appId, StramWebServicesInfo info) {
    boolean expired = false;
    if (info.securityInfo != null) {
      if (info.securityInfo.isExpiredToken()) {
        invalidateStramWebResource(appId);
        expired = true;
      }
    }
    return expired;
  }

}
