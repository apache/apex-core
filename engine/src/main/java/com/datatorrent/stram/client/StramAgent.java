/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.NewCookie;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.datatorrent.stram.client.WebServicesVersionConversion.IncompatibleVersionException;
import com.datatorrent.stram.client.WebServicesVersionConversion.VersionConversionFilter;
import com.datatorrent.stram.security.StramWSFilter;
import com.datatorrent.stram.util.HeaderClientFilter;
import com.datatorrent.stram.util.LRUCache;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.WebServices;

/**
 * <p>Abstract StramAgent class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.3
 */
public class StramAgent extends FSAgent
{
  private static final int MAX_REDIRECTS = 5;

  private static class StramWebServicesInfo
  {
    StramWebServicesInfo(String appMasterTrackingUrl, String version, String appPath, String user, String secToken, JSONObject permissionsInfo)
    {
      this.appMasterTrackingUrl = appMasterTrackingUrl;
      this.version = version;
      this.appPath = appPath;
      this.user = user;
      if (secToken != null) {
        securityInfo = new SecurityInfo(secToken);
      }
      try {
        if (permissionsInfo != null) {
          this.permissionsInfo = new PermissionsInfo(permissionsInfo);
        }
        else {
          this.permissionsInfo = null;
        }
      }
      catch (JSONException ex) {
        LOG.error("Caught exception when processing permissions info", ex);
      }
    }

    String appMasterTrackingUrl;
    String version;
    String appPath;
    String user;
    SecurityInfo securityInfo;
    PermissionsInfo permissionsInfo;
  }

  private static class SecurityInfo
  {
    public static final long DEFAULT_EXPIRY_INTERVAL = 60 * 60 * 1000;
    HeaderClientFilter secClientFilter;
    long expiryInterval = DEFAULT_EXPIRY_INTERVAL;
    long issueTime;

    SecurityInfo(String secToken)
    {
      issueTime = System.currentTimeMillis();
      secClientFilter = new HeaderClientFilter();
      secClientFilter.addCookie(new Cookie(StramWSFilter.CLIENT_COOKIE, secToken));
    }

    boolean isExpiredToken()
    {
      return ((System.currentTimeMillis() - issueTime) >= expiryInterval);
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(StramAgent.class);
  protected String resourceManagerWebappAddress;
  private final Map<String, StramWebServicesInfo> webServicesInfoMap = new LRUCache<String, StramWebServicesInfo>(100, true);
  protected String defaultStramRoot = null;
  protected Configuration conf;

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

  public StramAgent(FileSystem fs, Configuration conf)
  {
    super(fs);
    this.conf = conf;
  }

  public void setDefaultStramRoot(String dir)
  {
    this.defaultStramRoot = dir;
  }

  private synchronized void deleteCachedWebServicesInfo(String appid)
  {
    webServicesInfoMap.remove(appid);
  }

  private synchronized void setCachedWebServicesInfo(String appid, StramWebServicesInfo info)
  {
    webServicesInfoMap.put(appid, info);
  }

  private synchronized StramWebServicesInfo getCachedWebServicesInfo(String appid)
  {
    return webServicesInfoMap.get(appid);
  }

  private StramWebServicesInfo getWebServicesInfo(String appid)
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

  public String getWebServicesVersion(String appid)
  {
    StramWebServicesInfo info = getWebServicesInfo(appid);
    return info == null ? null : info.version;
  }

  public PermissionsInfo getPermissionsInfo(String appid)
  {
    StramWebServicesInfo info = getWebServicesInfo(appid);
    return info == null ? null : info.permissionsInfo;
  }

  public WebResource getStramWebResource(WebServicesClient webServicesClient, String appid) throws IncompatibleVersionException
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

  public void invalidateStramWebResource(String appid)
  {
    deleteCachedWebServicesInfo(appid);
  }

  public String getAppsRoot()
  {
    return (defaultStramRoot == null) ? (StramClientUtils.getDTDFSRootDir(fileSystem, conf) + "/" + StramClientUtils.SUBDIR_APPS) : defaultStramRoot;
  }

  public String getAppPath(String appId)
  {
    StramWebServicesInfo info = getWebServicesInfo(appId);
    return info == null ? getAppsRoot() + "/" + appId : info.appPath;
  }

  public String getUser(String appid)
  {
    StramWebServicesInfo info = getWebServicesInfo(appid);
    return info == null ? null : info.user;
  }

  private StramWebServicesInfo retrieveWebServicesInfo(String appId)
  {
    YarnClient yarnClient = YarnClient.createYarnClient();
    String url;
    try {
      yarnClient.init(conf);
      yarnClient.start();
      ApplicationReport ar = yarnClient.getApplicationReport(ConverterUtils.toApplicationId(appId));
      String trackingUrl = ar.getTrackingUrl();
      if (!trackingUrl.startsWith("http://")
              && !trackingUrl.startsWith("https://")) {
        url = "http://" + trackingUrl;
      }
      else {
        url = trackingUrl;
      }
      if (StringUtils.isBlank(url)) {
        LOG.error("Cannot get tracking url from YARN");
        return null;
      }
      if (url.endsWith("/")) {
        url = url.substring(0, url.length() - 1);
      }
      url += WebServices.PATH;
    }
    catch (Exception ex) {
      //LOG.error("Caught exception when retrieving web services info", ex);
      return null;
    }
    finally {
      yarnClient.stop();
    }

    WebServicesClient webServicesClient = new WebServicesClient();
    try {
      JSONObject response;
      String secToken = null;
      ClientResponse clientResponse;
      int i = 0;
      while (true) {
        LOG.debug("Accessing url {}", url);
        clientResponse = webServicesClient.process(url,
                                                   ClientResponse.class,
                                                   new WebServicesClient.GetWebServicesHandler<ClientResponse>());
        String val = clientResponse.getHeaders().getFirst("Refresh");
        if (val == null) {
          break;
        }
        int index = val.indexOf("url=");
        if (index < 0) {
          break;
        }
        url = val.substring(index + 4);
        if (i++ > MAX_REDIRECTS) {
          LOG.error("Cannot get web service info -- exceeded the max number of redirects");
          return null;
        }
      }

      if (!UserGroupInformation.isSecurityEnabled()) {
        response = new JSONObject(clientResponse.getEntity(String.class));
      }
      else {
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
      String user = response.getString("user");
      JSONObject permissionsInfo = null;
      FSDataInputStream is = null;
      try {
        is = fileSystem.open(new Path(appPath, "permissions.json"));
        permissionsInfo = new JSONObject(IOUtils.toString(is));
      }
      catch (JSONException ex) {
        LOG.error("Error reading from the permissions info. Ignoring", ex);
      }
      catch (IOException ex) {
        // ignore
      }
      finally {
        IOUtils.closeQuietly(is);
      }
      return new StramWebServicesInfo(appMasterUrl, version, appPath, user, secToken, permissionsInfo);
    }
    catch (Exception ex) {
      LOG.debug("Caught exception when retrieving web service info for app " + appId, ex);
      return null;
    }
  }

  private boolean checkSecExpiredToken(String appId, StramWebServicesInfo info)
  {
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
