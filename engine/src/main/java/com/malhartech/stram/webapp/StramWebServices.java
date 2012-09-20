/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.webapp;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.malhartech.stram.ModuleManager;
import com.malhartech.stram.StramAppContext;

/**
 *
 * The web services implementation in the stram<p>
 * <br>
 * This class would ensure the the caller is authorized and then provide access to all the dag data stored
 * in the stram<br>
 * <br>
 *
 */

@Path("/ws/v1/stram")
public class StramWebServices {
  private static final Logger LOG = LoggerFactory.getLogger(StramWebServices.class);

  private final StramAppContext appCtx;

  private @Context HttpServletResponse response;

  private @Inject @Nullable ModuleManager topologyManager;

  @Inject
  public StramWebServices(final StramAppContext context) {
    this.appCtx = context;
  }

  Boolean hasAccess(HttpServletRequest request) {
    String remoteUser = request.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    if (callerUGI != null) {
      return false;
    }
    return true;
  }

  private void init() {
    //clear content type
    response.setContentType(null);
  }

  void checkAccess(HttpServletRequest request) {
    if (!hasAccess(request)) {
      throw new WebApplicationException(Status.UNAUTHORIZED);
    }
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppInfo get() {
    return getAppInfo();
  }

  @GET
  @Path("info")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppInfo getAppInfo() {
    init();
    return new AppInfo(this.appCtx);
  }

  @GET
  @Path("nodes")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public ModulesInfo getNodes() throws Exception {
    init();
    LOG.info("topologyManager: {}", topologyManager);
    ModulesInfo nodeList = new ModulesInfo();
    nodeList.nodes = topologyManager.getNodeInfoList();
    return nodeList;
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path("shutdown")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JSONObject shutdown() {
      topologyManager.shutdownAllContainers();
      return new JSONObject();
  }


}

