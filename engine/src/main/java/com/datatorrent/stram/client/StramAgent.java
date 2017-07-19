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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.UriBuilder;

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
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;

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
 * @since 0.3.3
 */
public class StramAgent extends FSAgent
{
  private static final int MAX_REDIRECTS = 5;
  private static final int STRAM_WEBSERVICE_RETRIES = 1;

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
        } else {
          this.permissionsInfo = null;
        }
      } catch (JSONException ex) {
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
  private final Map<String, StramWebServicesInfo> webServicesInfoMap = new LRUCache<>(100, true);
  protected String defaultStramRoot = null;
  protected Configuration conf;

  public static class AppNotFoundException extends Exception
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

  private UriBuilder getStramWebURIBuilder(WebServicesClient webServicesClient, String appid) throws IncompatibleVersionException
  {
    webServicesClient.getClient().setFollowRedirects(true);
    webServicesClient.clearFilters();
    StramWebServicesInfo info = getWebServicesInfo(appid);
    UriBuilder ub = null;
    if (info != null) {
      //ws = wsClient.resource("http://" + info.appMasterTrackingUrl).path(WebServices.PATH).path(info.version).path("stram");
      // the filter should convert to the right version
      String url;
      if (!info.appMasterTrackingUrl.startsWith("http://")
          && !info.appMasterTrackingUrl.startsWith("https://")) {
        url = "http://" + info.appMasterTrackingUrl;
      } else {
        url = info.appMasterTrackingUrl;
      }
      ub = UriBuilder.fromUri(url).path(WebServices.PATH).path(WebServices.VERSION).path("stram");
      WebServicesVersionConversion.Converter versionConverter = WebServicesVersionConversion.getConverter(info.version);
      if (versionConverter != null) {
        VersionConversionFilter versionConversionFilter = new VersionConversionFilter(versionConverter);
        webServicesClient.addFilter(versionConversionFilter);
      }
      if (info.securityInfo != null) {
        webServicesClient.addFilter(info.securityInfo.secClientFilter);
      }
    }
    return ub;
  }

  public void invalidateStramWebResource(String appid)
  {
    deleteCachedWebServicesInfo(appid);
  }

  public static class StramUriSpec
  {
    private final List<String> paths = new ArrayList<>();
    private final Multimap<String, Object> queryParams = HashMultimap.create();

    public StramUriSpec path(String elem)
    {
      paths.add(elem);
      return this;
    }

    public StramUriSpec queryParam(String name, Object... values)
    {
      queryParams.putAll(name, Arrays.asList(values));
      return this;
    }

    public StramUriSpec queryParam(Map<String, ? extends Object> map)
    {
      for (Map.Entry<String, ? extends Object> entry : map.entrySet()) {
        queryParams.put(entry.getKey(), entry.getValue());
      }
      return this;
    }

    List<String> getPaths()
    {
      return paths;
    }

    Multimap<String, Object> getQueryParams()
    {
      return queryParams;
    }

  }

  public <T> T issueStramWebRequest(WebServicesClient webServiceClient, String appId, StramUriSpec stramUriSpec, Class<T> clazz, WebServicesClient.WebServicesHandler<T> handler)
          throws AppNotFoundException, IOException, IncompatibleVersionException
  {
    int retries = STRAM_WEBSERVICE_RETRIES;
    while (true) {
      try {
        UriBuilder ub = getStramWebURIBuilder(webServiceClient, appId);
        if (ub == null) {
          throw new AppNotFoundException(appId);
        }
        for (String path : stramUriSpec.getPaths()) {
          ub = ub.path(path);
        }
        for (Map.Entry<String, Object> entry : stramUriSpec.getQueryParams().entries()) {
          ub = ub.queryParam(entry.getKey(), entry.getValue());
        }
        return webServiceClient.process(webServiceClient.getClient().resource(ub.build()).accept(MediaType.APPLICATION_JSON), clazz, handler);
      } catch (ClientHandlerException ex) {
        if (retries-- > 0) {
          invalidateStramWebResource(appId);
        } else {
          throw ex;
        }
      } catch (IOException ex) {
        if (retries-- > 0) {
          invalidateStramWebResource(appId);
        } else {
          throw ex;
        }
      }
    }
  }

  public JSONObject issueStramWebRequest(WebServicesClient webServiceClient, String appId, StramUriSpec stramUriSpec, WebServicesClient.WebServicesHandler<JSONObject> handler)
          throws AppNotFoundException, IOException, IncompatibleVersionException
  {
    return issueStramWebRequest(webServiceClient, appId, stramUriSpec, JSONObject.class, handler);
  }

  public JSONObject issueStramWebGetRequest(WebServicesClient webServiceClient, String appId, String resourcePath)
          throws AppNotFoundException, IOException, IncompatibleVersionException
  {
    return issueStramWebRequest(webServiceClient, appId, new StramUriSpec().path(resourcePath), new WebServicesClient.GetWebServicesHandler<JSONObject>());
  }

  public String getAppsRoot()
  {
    return (defaultStramRoot == null) ? (StramClientUtils.getApexDFSRootDir(fileSystem, conf) + "/" + StramClientUtils.SUBDIR_APPS) : defaultStramRoot;
  }

  public String getAppPath(String appId)
  {
    StramWebServicesInfo info = getWebServicesInfo(appId);
    // TODO: when we upgrade hadoop dependency to 2.4, we need to save app path as a tag
    return info == null ? getAppsRoot() + "/" + appId : info.appPath;
  }

  // Note that this method only works if the app is running.  We might want to deprecate this method.
  public String getUser(String appid)
  {
    StramWebServicesInfo info = getWebServicesInfo(appid);
    return info == null ? null : info.user;
  }

  private StramWebServicesInfo retrieveWebServicesInfo(String appId)
  {
    String url;
    try (YarnClient yarnClient = StramClientUtils.createYarnClient(conf)) {
      ApplicationReport ar = yarnClient.getApplicationReport(ConverterUtils.toApplicationId(appId));
      if (ar == null) {
        LOG.warn("YARN does not have record for this application {}", appId);
        return null;
      } else if (ar.getYarnApplicationState() != YarnApplicationState.RUNNING) {
        LOG.debug("Application {} is not running (state: {})", appId, ar.getYarnApplicationState());
        return null;
      }

      String trackingUrl = ar.getTrackingUrl();
      if (!trackingUrl.startsWith("http://")
          && !trackingUrl.startsWith("https://")) {
        url = "http://" + trackingUrl;
      } else {
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
    } catch (Exception ex) {
      LOG.error("Cannot retrieve web services info", ex);
      return null;
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
      } else {
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
      Path permissionsPath = new Path(appPath, "permissions.json");
      LOG.debug("Checking for permission information in file {}", permissionsPath);
      try {
        if (fileSystem.exists(permissionsPath)) {
          LOG.info("Loading permission information");
          try (FSDataInputStream is = fileSystem.open(permissionsPath)) {
            permissionsInfo = new JSONObject(IOUtils.toString(is));
          }
          LOG.debug("Loaded permission file successfully");
        } else {
          // ignore and log messages if file is not found
          LOG.info("Permission information is not available as the application is not configured with it");
        }
      } catch (IOException ex) {
        // ignore and log message when unable to read the file
        LOG.info("Permission information is not available", ex);
      }
      return new StramWebServicesInfo(appMasterUrl, version, appPath, user, secToken, permissionsInfo);
    } catch (Exception ex) {
      LOG.warn("Cannot retrieve web service info for app {}", appId, ex);
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
