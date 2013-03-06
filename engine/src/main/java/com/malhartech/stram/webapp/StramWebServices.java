/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.webapp;

import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.malhartech.stram.StramAppContext;
import com.malhartech.stram.StramChildAgent;
import com.malhartech.stram.StreamingContainerManager;

/**
 *
 * The web services implementation in the stram<p>
 * <br>
 * This class would ensure the the caller is authorized and then provide access to all the dag data stored
 * in the stram<br>
 * <br>
 *
 */
@Path(StramWebServices.PATH)
public class StramWebServices
{
  private static final Logger LOG = LoggerFactory.getLogger(StramWebServices.class);
  public static final String PATH = "/ws/v1/stram";
  public static final String PATH_INFO = "info";
  public static final String PATH_OPERATORS = "operators";
  public static final String PATH_SHUTDOWN = "shutdown";
  public static final String PATH_STARTRECORDING = "startRecording";
  public static final String PATH_STOPRECORDING = "stopRecording";
  public static final String PATH_CONTAINERS = "containers";
  public static final String PATH_LOGICAL_PLAN_OPERATORS = "logicalPlan/operators";

  private final StramAppContext appCtx;
  @Context
  private HttpServletResponse response;
  @Inject
  @Nullable
  private StreamingContainerManager dagManager;

  @Inject
  public StramWebServices(final StramAppContext context)
  {
    this.appCtx = context;
  }

  Boolean hasAccess(HttpServletRequest request)
  {
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

  private void init()
  {
    //clear content type
    response.setContentType(null);
  }

  void checkAccess(HttpServletRequest request)
  {
    if (!hasAccess(request)) {
      throw new WebApplicationException(Status.UNAUTHORIZED);
    }
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public AppInfo get()
  {
    return getAppInfo();
  }

  @GET
  @Path(PATH_INFO)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public AppInfo getAppInfo()
  {
    init();
    return new AppInfo(this.appCtx);
  }

  @GET
  @Path(PATH_OPERATORS)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public OperatorsInfo getOperatorsInfo() throws Exception
  {
    init();
    OperatorsInfo nodeList = new OperatorsInfo();
    nodeList.operators = dagManager.getOperatorInfoList();
    return nodeList;
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path(PATH_SHUTDOWN)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public JSONObject shutdown()
  {
    dagManager.shutdownAllContainers("Shutdown requested externally.");
    return new JSONObject();
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path(PATH_STARTRECORDING)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public JSONObject startRecording(JSONObject request)
  {
    JSONObject response = new JSONObject();
    try {
      int operId = Integer.valueOf(request.getString("operId"));
      String name = request.optString("name");
      dagManager.startRecording(operId, name);
    }
    catch (JSONException ex) {
      try {
        response.put("error", ex.toString());
      }
      catch (JSONException ex1) {
        throw new RuntimeException(ex1);
      }
    }
    return response;
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path(PATH_STOPRECORDING)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public JSONObject stopRecording(JSONObject request)
  {
    JSONObject response = new JSONObject();
    try {
      int operId = request.getInt("operId");
      dagManager.stopRecording(operId);
    }
    catch (JSONException ex) {
      ex.printStackTrace();
    }
    return response;
  }

  @GET
  @Path(PATH_CONTAINERS)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public List<ContainerInfo> listContainers() throws Exception
  {
    init();
    Collection<StramChildAgent> containerAgents = dagManager.getContainerAgents();
    List<ContainerInfo> l = new java.util.ArrayList<ContainerInfo>(containerAgents.size());
    for (StramChildAgent sca : containerAgents) {
      l.add(sca.getContainerInfo());
    }
    return l;
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path(PATH_CONTAINERS + "/{containerId}/kill")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public JSONObject killContainer(JSONObject request, @PathParam("containerId") String containerId)
  {
    JSONObject response = new JSONObject();
    dagManager.stopContainer(containerId);
    return response;
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path(PATH_LOGICAL_PLAN_OPERATORS + "/{operatorId}/setProperty")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public JSONObject setOperatorProperty(JSONObject request, @PathParam("operatorId") String operatorId)
  {
    JSONObject response = new JSONObject();
    try {
      String propertyName = request.getString("propertyName");
      String propertyValue = request.getString("propertyValue");
      dagManager.setOperatorProperty(operatorId, propertyName, propertyValue);
    }
    catch (JSONException ex) {
      ex.printStackTrace();
    }
    return response;
  }

}
