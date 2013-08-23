/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp;

import java.io.IOException;
import java.util.*;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.codec.LogicalPlanSerializer;
import com.datatorrent.stram.DAGPropertiesBuilder;
import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StramChildAgent;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanRequest;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.google.inject.Inject;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;

/**
 *
 * The web services implementation in the stram<p>
 * <br>
 * This class would ensure the the caller is authorized and then provide access to all the dag data stored
 * in the stram<br>
 * <br>
 *
 * @since 0.3.2
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
  public static final String PATH_SYNCSTATS = "syncStats";
  public static final String PATH_CONTAINERS = "containers";
  public static final String PATH_LOGICAL_PLAN = "logicalPlan";
  public static final String PATH_LOGICAL_PLAN_OPERATORS = PATH_LOGICAL_PLAN + "/operators";
  public static final String PATH_LOGICAL_PLAN_MODIFICATION = PATH_LOGICAL_PLAN + "/modification";
  public static final String PATH_OPERATOR_CLASSES = "operatorClasses";
  public static final String PATH_DESCRIBE_OPERATOR = "describeOperator";
  private final StramAppContext appCtx;
  @Context
  private HttpServletResponse httpResponse;
  @Inject
  @Nullable
  private StreamingContainerManager dagManager;
  private final OperatorDiscoverer operatorDiscoverer = new OperatorDiscoverer();

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
    httpResponse.setContentType(null);
  }

  void checkAccess(HttpServletRequest request)
  {
    if (!hasAccess(request)) {
      throw new SecurityException();
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
      throw new NotFoundException();
    }
    return oi;
  }

  @GET
  @Path(PATH_OPERATORS + "/{operatorId}/ports")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public JSONObject getPortsInfo(@PathParam("operatorId") String operatorId) throws Exception
  {
    init();
    Map<String, Object> map = new HashMap<String, Object>();
    OperatorInfo oi = dagManager.getOperatorInfo(operatorId);
    if (oi == null) {
      throw new NotFoundException();
    }
    ObjectMapper mapper = new ObjectMapper();
    map.put("inputPorts", oi.inputPorts);
    map.put("outputPorts", oi.outputPorts);
    return new JSONObject(mapper.writeValueAsString(map));
  }

  @GET
  @Path(PATH_OPERATORS + "/{operatorId}/ports/{portName}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public PortInfo getPortsInfo(@PathParam("operatorId") String operatorId, @PathParam("portName") String portName) throws Exception
  {
    init();
    OperatorInfo oi = dagManager.getOperatorInfo(operatorId);
    if (oi == null) {
      throw new NotFoundException();
    }
    for (PortInfo pi : oi.inputPorts) {
      if (pi.name.equals(portName)) {
        return pi;
      }
    }
    for (PortInfo pi : oi.outputPorts) {
      if (pi.name.equals(portName)) {
        return pi;
      }
    }
    throw new NotFoundException();
  }

  @GET
  @Path(PATH_OPERATOR_CLASSES)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  @SuppressWarnings({"rawtypes", "unchecked"})
  public JSONObject getOperatorClasses(@QueryParam("parent") String parent)
  {
    JSONObject result = new JSONObject();
    JSONArray classNames = new JSONArray();

    if (parent != null) {
      if (parent.equals("chart")) {
        parent = "com.datatorrent.lib.chart.ChartOperator";
      }
      else if (parent.equals("filter")) {
        parent = "com.datatorrent.lib.util.SimpleFilterOperator";
      }
    }

    try {
      List<Class<? extends Operator>> operatorClasses = operatorDiscoverer.getOperatorClasses(parent);

      for (Class clazz : operatorClasses) {
        classNames.put(clazz.getName());
      }

      result.put("classes", classNames);
    }
    catch (Exception ex) {
      throw new NotFoundException();
    }
    return result;
  }

  @GET
  @Path(PATH_DESCRIBE_OPERATOR)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public JSONObject describeOperator(@QueryParam("class") String className)
  {
    if (className == null) {
      throw new UnsupportedOperationException();
    }
    try {
      Class<?> clazz = Class.forName(className);
      if (OperatorDiscoverer.isInstantiableOperatorClass(clazz)) {
        JSONObject response = new JSONObject();
        JSONArray properties = new JSONArray();
        JSONArray inputPorts = new JSONArray();
        JSONArray outputPorts = new JSONArray();
        BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
        PropertyDescriptor[] pds = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor pd : pds) {
          if (pd.getWriteMethod() != null
                  && !pd.getWriteMethod().getName().equals("setup")
                  && !pd.getName().equals("name")) {
            JSONObject property = new JSONObject();
            property.put("name", pd.getName());
            property.put("class", pd.getPropertyType().getName());
            property.put("description", pd.getShortDescription());
            properties.put(property);
          }
        }
        Field[] fields = clazz.getFields();
        for (Field field : fields) {
          InputPortFieldAnnotation inputAnnotation = field.getAnnotation(InputPortFieldAnnotation.class);
          if (inputAnnotation != null) {
            JSONObject inputPort = new JSONObject();
            inputPort.put("name", inputAnnotation.name());
            inputPort.put("optional", inputAnnotation.optional());
            inputPorts.put(inputPort);
            continue;
          }
          else if (InputPort.class.isAssignableFrom(field.getType())) {
            JSONObject inputPort = new JSONObject();
            inputPort.put("name", field.getName());
            inputPort.put("optional", false); // input port that is not annotated is default to be non-optional
            inputPorts.put(inputPort);
            continue;
          }
          OutputPortFieldAnnotation outputAnnotation = field.getAnnotation(OutputPortFieldAnnotation.class);
          if (outputAnnotation != null) {
            JSONObject outputPort = new JSONObject();
            outputPort.put("name", outputAnnotation.name());
            outputPort.put("optional", outputAnnotation.optional());
            outputPorts.put(outputPort);
            continue;
          }
          else if (OutputPort.class.isAssignableFrom(field.getType())) {
            JSONObject outputPort = new JSONObject();
            outputPort.put("name", field.getName());
            outputPort.put("optional", true); // output port that is not annotated is default to be optional
            outputPorts.put(outputPort);
            continue;
          }
        }
        response.put("properties", properties);
        response.put("inputPorts", inputPorts);
        response.put("outputPorts", outputPorts);
        return response;
      }
      else {
        throw new UnsupportedOperationException();
      }
    }
    catch (ClassNotFoundException ex) {
      throw new NotFoundException();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
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
      LOG.warn("Got JSON Exception: ", ex);
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
      LOG.warn("Got JSON Exception: ", ex);
    }
    return response;
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path(PATH_SYNCSTATS)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public JSONObject syncStats(JSONObject request)
  {
    JSONObject response = new JSONObject();
    dagManager.syncStats();
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
    for (StramChildAgent sca : containerAgents) {
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
      throw new NotFoundException();
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
      LOG.warn("Got JSON Exception: ", ex);
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
    }
    else {
      JSONObject json = new JSONObject();
      try {
        json.put(attributeName, m.get(attributeName));
      }
      catch (JSONException ex) {
        LOG.warn("Got JSON Exception: ", ex);
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
    }
    else {
      JSONObject json = new JSONObject();
      try {
        json.put(attributeName, m.get(attributeName));
      }
      catch (JSONException ex) {
        LOG.warn("Got JSON Exception: ", ex);
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
    }
    else {
      JSONObject json = new JSONObject();
      try {
        json.put(attributeName, m.get(attributeName));
      }
      catch (JSONException ex) {
        LOG.warn("Got JSON Exception: ", ex);
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
    }
    else {
      JSONObject json = new JSONObject();
      try {
        json.put(propertyName, m.get(propertyName));
      }
      catch (JSONException ex) {
        LOG.warn("Got JSON Exception: ", ex);
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
        LogicalPlanRequest requestObj = (LogicalPlanRequest)Class.forName(LogicalPlanRequest.class.getPackage().getName() + "." + jsonObj.getString("requestType")).newInstance();
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
        }
        else {
          response.put("error", ex.toString());
        }
      }
      catch (Exception e) {
        // ignore
      }
    }

    return response;
  }

}
