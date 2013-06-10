/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.webapp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.malhartech.codec.LogicalPlanSerializer;
import com.malhartech.stram.DAGPropertiesBuilder;
import com.malhartech.stram.StramAppContext;
import com.malhartech.stram.StramChildAgent;
import com.malhartech.stram.StreamingContainerManager;
import com.malhartech.stram.plan.logical.LogicalPlan;
import com.malhartech.stram.plan.logical.LogicalPlanRequest;
import com.malhartech.stram.plan.logical.LogicalPlan.OperatorMeta;

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
  public static final String PATH_SYNCRECORDING = "syncRecording";
  public static final String PATH_CONTAINERS = "containers";
  public static final String PATH_LOGICAL_PLAN = "logicalPlan";
  public static final String PATH_LOGICAL_PLAN_OPERATORS = PATH_LOGICAL_PLAN + "/operators";
  public static final String PATH_LOGICAL_PLAN_MODIFICATION = PATH_LOGICAL_PLAN + "/modification";
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

  @GET
  @Path(PATH_OPERATORS + "/{operatorId}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public OperatorInfo getOperatorInfo(@PathParam("operatorId") String operatorId) throws Exception
  {
    init();
    OperatorInfo oi = dagManager.getOperatorInfo(operatorId);
    if (oi == null) {
      throw new WebApplicationException(404);
    }
    return oi;
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
      String portName = request.optString("portName");
      dagManager.startRecording(operId, portName);
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
      String portName = request.optString("portName");
      dagManager.stopRecording(operId, portName);
    }
    catch (JSONException ex) {
      ex.printStackTrace();
    }
    return response;
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path(PATH_SYNCRECORDING)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public JSONObject syncRecording(JSONObject request)
  {
    JSONObject response = new JSONObject();
    try {
      int operId = request.getInt("operId");
      String portName = request.optString("portName");
      dagManager.syncRecording(operId, portName);
    }
    catch (JSONException ex) {
      ex.printStackTrace();
    }
    return response;
  }

  @GET
  @Path(PATH_CONTAINERS)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public ContainersInfo listContainers() throws Exception
  {
    init();
    Collection<StramChildAgent> containerAgents = dagManager.getContainerAgents();
    ContainersInfo ci = new ContainersInfo();
    for (StramChildAgent sca: containerAgents) {
      ci.add(sca.getContainerInfo());
    }
    return ci;
  }

  @GET
  @Path(PATH_CONTAINERS + "/{containerId}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public ContainerInfo getContainer(@PathParam("containerId") String containerId) throws Exception
  {
    init();
    StramChildAgent sca = dagManager.getContainerAgent(containerId);
    if (sca == null) {
      throw new WebApplicationException(404);
    }
    return sca.getContainerInfo();
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

  @GET
  @Path(PATH_LOGICAL_PLAN_OPERATORS + "/{operatorId}/getAttributes")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public JSONObject getOperatorAttributes(@PathParam("operatorId") String operatorId, @QueryParam("attributeName") String attributeName)
  {
    Map<String, Object> m = dagManager.getOperatorAttributes(operatorId);
    if (attributeName == null) {
      return new JSONObject(m);
    } else {
      JSONObject json = new JSONObject();
      try {
        json.put(attributeName, m.get(attributeName));
      } catch (JSONException ex) {
        ex.printStackTrace();
      }
      return json;
    }
  }

  @GET
  @Path(PATH_LOGICAL_PLAN + "/getAttributes")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public JSONObject getApplicationAttributes(@QueryParam("attributeName") String attributeName)
  {
    Map<String, Object> m = dagManager.getApplicationAttributes();
    if (attributeName == null) {
      return new JSONObject(m);
    } else {
      JSONObject json = new JSONObject();
      try {
        json.put(attributeName, m.get(attributeName));
      } catch (JSONException ex) {
        ex.printStackTrace();
      }
      return json;
    }
  }

  @GET
  @Path(PATH_LOGICAL_PLAN_OPERATORS + "/{operatorId}/{portName}/getAttributes")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public JSONObject getPortAttributes(@PathParam("operatorId") String operatorId, @PathParam("portName") String portName, @QueryParam("attributeName") String attributeName)
  {
    Map<String, Object> m = dagManager.getPortAttributes(operatorId, portName);
    if (attributeName == null) {
      return new JSONObject(m);
    } else {
      JSONObject json = new JSONObject();
      try {
        json.put(attributeName, m.get(attributeName));
      } catch (JSONException ex) {
        ex.printStackTrace();
      }
      return json;
    }
  }

  @GET
  @Path(PATH_LOGICAL_PLAN_OPERATORS + "/{operatorId}/getProperties")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public JSONObject getOperatorProperties(@PathParam("operatorId") String operatorId, @QueryParam("propertyName") String propertyName)
  {
    OperatorMeta logicalOperator = dagManager.getLogicalPlan().getOperatorMeta(operatorId);
    if (logicalOperator == null) {
      throw new IllegalArgumentException("Invalid operatorId " + operatorId);
    }
    Map<String, Object> m = DAGPropertiesBuilder.getOperatorProperties(logicalOperator.getOperator());

    if (propertyName == null) {
      return new JSONObject(m);
    } else {
      JSONObject json = new JSONObject();
      try {
        json.put(propertyName, m.get(propertyName));
      } catch (JSONException ex) {
        ex.printStackTrace();
      }
      return json;
    }
  }

  @GET
  @Path(PATH_LOGICAL_PLAN)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public JSONObject getLogicalPlan() throws JSONException, IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    LogicalPlan lp = dagManager.getLogicalPlan();
    return new JSONObject(mapper.writeValueAsString(LogicalPlanSerializer.convertToMap(lp)));
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path(PATH_LOGICAL_PLAN_MODIFICATION)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public JSONObject logicalPlanModification(JSONObject request)
  {
    JSONObject response = new JSONObject();
    try {
      JSONArray jsonArray = request.getJSONArray("requests");
      List<LogicalPlanRequest> requests = new ArrayList<LogicalPlanRequest>();
      for (int i = 0; i < jsonArray.length(); i++) {
        JSONObject jsonObj = (JSONObject)jsonArray.get(i);
        LogicalPlanRequest requestObj = (LogicalPlanRequest)Class.forName(jsonObj.getString("requestType")).newInstance();
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = BeanUtils.describe(requestObj);
        @SuppressWarnings("unchecked")
        Iterator<String> keys = jsonObj.keys();

        while (keys.hasNext()) {
          String key = keys.next();
          if (!key.equals("requestType")) {
            properties.put(key, jsonObj.get(key));
          }
        }
        BeanUtils.populate(requestObj, properties);
        requests.add(requestObj);
      }
      Future<?> fr = dagManager.logicalPlanModification(requests);
      fr.get(3000, TimeUnit.MILLISECONDS);
    }
    catch (Exception ex) {
      LOG.error("Error processing plan change", ex);
      try {
        if (ex instanceof ExecutionException) {
          response.put("error", ex.getCause().toString());
        } else {
          response.put("error", ex.toString());
        }
      } catch (Exception e) {
        // ignore
      }
    }

    return response;
  }

}
