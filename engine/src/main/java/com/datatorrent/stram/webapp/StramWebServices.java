/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.datatorrent.api.AttributeMap.Attribute;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StramChildAgent;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.codec.LogicalPlanSerializer;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import com.datatorrent.stram.plan.logical.LogicalPlanRequest;

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
  public static final String PATH = WebServices.PATH + "/" + WebServices.VERSION + "/stram";
  public static final String PATH_INFO = "info";
  public static final String PATH_PHYSICAL_PLAN = "physicalPlan";
  public static final String PATH_PHYSICAL_PLAN_OPERATORS = PATH_PHYSICAL_PLAN + "/operators";
  public static final String PATH_PHYSICAL_PLAN_STREAMS = PATH_PHYSICAL_PLAN + "/streams";
  public static final String PATH_SHUTDOWN = "shutdown";
  public static final String PATH_RECORDINGS = "recordings";
  public static final String PATH_RECORDINGS_START = PATH_RECORDINGS + "/start";
  public static final String PATH_RECORDINGS_STOP = PATH_RECORDINGS + "/stop";
  public static final String PATH_PHYSICAL_PLAN_CONTAINERS = PATH_PHYSICAL_PLAN + "/containers";
  public static final String PATH_LOGICAL_PLAN = "logicalPlan";
  public static final String PATH_LOGICAL_PLAN_OPERATORS = PATH_LOGICAL_PLAN + "/operators";
  public static final String PATH_OPERATOR_CLASSES = "operatorClasses";
  public static final String PATH_ALERTS = "alerts";
  //public static final String PATH_ACTION_OPERATOR_CLASSES = "actionOperatorClasses";
  private final StramAppContext appCtx;
  @Context
  private HttpServletResponse httpResponse;
  @Inject
  @Nullable
  private StreamingContainerManager dagManager;
  private final OperatorDiscoverer operatorDiscoverer = new OperatorDiscoverer();
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Inject
  public StramWebServices(final StramAppContext context)
  {
    this.appCtx = context;
    objectMapper.configure(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, true);
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
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject get() throws Exception
  {
    return getAppInfo();
  }

  @GET
  @Path(PATH_INFO)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getAppInfo() throws Exception
  {
    init();
    return new JSONObject(objectMapper.writeValueAsString(new AppInfo(this.appCtx)));
  }

  @GET
  @Path(PATH_PHYSICAL_PLAN)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getPhysicalPlan() throws Exception
  {
    Map<String, Object> result = new HashMap<String, Object>();
    result.put("operators", dagManager.getOperatorInfoList());
    result.put("streams", dagManager.getStreamInfoList());
    return new JSONObject(objectMapper.writeValueAsString(result));
  }

  @GET
  @Path(PATH_PHYSICAL_PLAN_OPERATORS)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getOperatorsInfo() throws Exception
  {
    init();
    OperatorsInfo nodeList = new OperatorsInfo();
    nodeList.operators = dagManager.getOperatorInfoList();
    // To get around the nasty JAXB problem for lists
    return new JSONObject(objectMapper.writeValueAsString(nodeList));
  }

  @GET
  @Path(PATH_PHYSICAL_PLAN_STREAMS)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getStreamsInfo() throws Exception
  {
    init();
    StreamsInfo streamList = new StreamsInfo();
    streamList.streams = dagManager.getStreamInfoList();
    return new JSONObject(objectMapper.writeValueAsString(streamList));
  }

  @GET
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{operatorId}")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getOperatorInfo(@PathParam("operatorId") String operatorId) throws Exception
  {
    init();
    OperatorInfo oi = dagManager.getOperatorInfo(operatorId);
    if (oi == null) {
      throw new NotFoundException();
    }
    return new JSONObject(objectMapper.writeValueAsString(oi));
  }

  @GET
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{operatorId}/ports")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getPortsInfo(@PathParam("operatorId") String operatorId) throws Exception
  {
    init();
    Map<String, Object> map = new HashMap<String, Object>();
    OperatorInfo oi = dagManager.getOperatorInfo(operatorId);
    if (oi == null) {
      throw new NotFoundException();
    }
    map.put("ports", oi.ports);
    return new JSONObject(objectMapper.writeValueAsString(map));
  }

  @GET
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{operatorId}/ports/{portName}")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getPortsInfo(@PathParam("operatorId") String operatorId, @PathParam("portName") String portName) throws Exception
  {
    init();
    OperatorInfo oi = dagManager.getOperatorInfo(operatorId);
    if (oi == null) {
      throw new NotFoundException();
    }
    for (PortInfo pi : oi.ports) {
      if (pi.name.equals(portName)) {
        return new JSONObject(objectMapper.writeValueAsString(pi));
      }
    }
    throw new NotFoundException();
  }

  @GET
  @Path(PATH_OPERATOR_CLASSES)
  @Produces(MediaType.APPLICATION_JSON)
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
      Set<Class<? extends Operator>> operatorClasses = operatorDiscoverer.getOperatorClasses(parent);

      for (Class<?> clazz : operatorClasses) {
        JSONObject j = new JSONObject();
        j.put("name", clazz.getName());
        classNames.put(j);
      }

      result.put("classes", classNames);
    }
    catch (ClassNotFoundException ex) {
      throw new NotFoundException();
    }
    catch (JSONException ex) {
    }
    return result;
  }

  @GET
  @Path(PATH_OPERATOR_CLASSES + "/{className}")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject describeOperator(@PathParam("className") String className)
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
        Arrays.sort(fields, new Comparator<Field>()
        {
          @Override
          public int compare(Field a, Field b)
          {
            return a.getName().compareTo(b.getName());
          }

        });
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
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject shutdown()
  {
    LOG.debug("Shutdown requested");
    dagManager.shutdownAllContainers("Shutdown requested externally.");
    return new JSONObject();
  }

  @POST
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{opId}/" + PATH_RECORDINGS_START)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject startRecording(@PathParam("opId") String opId)
  {
    LOG.debug("Start recording on {} requested", opId);
    JSONObject response = new JSONObject();
    dagManager.startRecording(Integer.valueOf(opId), null);
    return response;
  }

  @POST
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{opId}/ports/{portName}" + PATH_RECORDINGS_START)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject startRecording(@PathParam("opId") String opId, @PathParam("portName") String portName)
  {
    LOG.debug("Start recording on {}.{} requested", opId, portName);
    JSONObject response = new JSONObject();
    dagManager.startRecording(Integer.valueOf(opId), portName);
    return response;
  }

  @POST
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{opId}/" + PATH_RECORDINGS_STOP)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject stopRecording(@PathParam("opId") String opId)
  {
    LOG.debug("Start recording on {} requested", opId);
    JSONObject response = new JSONObject();
    dagManager.stopRecording(Integer.valueOf(opId), null);
    return response;
  }

  @POST
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{opId}/ports/{portName}" + PATH_RECORDINGS_STOP)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject stopRecording(@PathParam("opId") String opId, @PathParam("portName") String portName)
  {
    LOG.debug("Stop recording on {}.{} requested", opId, portName);
    JSONObject response = new JSONObject();
    dagManager.stopRecording(Integer.valueOf(opId), portName);
    return response;
  }

  @GET
  @Path(PATH_PHYSICAL_PLAN_CONTAINERS)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject listContainers() throws Exception
  {
    init();
    Collection<StramChildAgent> containerAgents = dagManager.getContainerAgents();
    ContainersInfo ci = new ContainersInfo();
    for (StramChildAgent sca : containerAgents) {
      ci.add(sca.getContainerInfo());
    }
    // To get around the nasty JAXB problem for lists
    return new JSONObject(objectMapper.writeValueAsString(ci));
  }

  @GET
  @Path(PATH_PHYSICAL_PLAN_CONTAINERS + "/{containerId}")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getContainer(@PathParam("containerId") String containerId) throws Exception
  {
    init();
    StramChildAgent sca = dagManager.getContainerAgent(containerId);
    if (sca == null) {
      throw new NotFoundException();
    }
    return new JSONObject(objectMapper.writeValueAsString(sca.getContainerInfo()));
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path(PATH_PHYSICAL_PLAN_CONTAINERS + "/{containerId}/kill")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject killContainer(@PathParam("containerId") String containerId)
  {
    JSONObject response = new JSONObject();
    dagManager.stopContainer(containerId);
    return response;
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path(PATH_LOGICAL_PLAN_OPERATORS + "/{operatorId}/properties")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject setOperatorProperties(JSONObject request, @PathParam("operatorId") String operatorId)
  {
    JSONObject response = new JSONObject();
    try {
      @SuppressWarnings("unchecked")
      Iterator<String> keys = request.keys();
      while (keys.hasNext()) {
        String key = keys.next();
        String val = request.getString(key);
        dagManager.setOperatorProperty(operatorId, key, val);
      }
    }
    catch (JSONException ex) {
      LOG.warn("Got JSON Exception: ", ex);
    }
    return response;
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{operatorId}/properties")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject setPhysicalOperatorProperties(JSONObject request, @PathParam("operatorId") String operatorId)
  {
    JSONObject response = new JSONObject();
    try {
      @SuppressWarnings("unchecked")
      Iterator<String> keys = request.keys();
      while (keys.hasNext()) {
        String key = keys.next();
        String val = request.getString(key);
        dagManager.setPhysicalOperatorProperty(operatorId, key, val);
      }
    }
    catch (JSONException ex) {
      LOG.warn("Got JSON Exception: ", ex);
    }
    return response;
  }
  
  @GET
  @Path(PATH_LOGICAL_PLAN_OPERATORS + "/{operatorId}/attributes")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getOperatorAttributes(@PathParam("operatorId") String operatorId, @QueryParam("attributeName") String attributeName)
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    for (Entry<Attribute<?>, Object> entry : dagManager.getOperatorAttributes(operatorId).entrySet()) {
      if (attributeName == null || entry.getKey().equals(attributeName)) {
        map.put(entry.getKey().name, entry.getValue());
      }
    }
    return new JSONObject(map);
  }

  @GET
  @Path(PATH_LOGICAL_PLAN + "/attributes")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getApplicationAttributes(@QueryParam("attributeName") String attributeName)
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    for (Entry<Attribute<?>, Object> entry : dagManager.getApplicationAttributes().entrySet()) {
      if (attributeName == null || entry.getKey().equals(attributeName)) {
        map.put(entry.getKey().name, entry.getValue());
      }
    }
    return new JSONObject(map);
  }

  @GET
  @Path(PATH_LOGICAL_PLAN_OPERATORS + "/{operatorId}/{portName}/attributes")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getPortAttributes(@PathParam("operatorId") String operatorId, @PathParam("portName") String portName, @QueryParam("attributeName") String attributeName)
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    for (Entry<Attribute<?>, Object> entry : dagManager.getPortAttributes(operatorId, portName).entrySet()) {
      if (attributeName == null || entry.getKey().equals(attributeName)) {
        map.put(entry.getKey().name, entry.getValue());
      }
    }
    return new JSONObject(map);
  }

  @GET
  @Path(PATH_LOGICAL_PLAN_OPERATORS + "/{operatorId}/properties")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getOperatorProperties(@PathParam("operatorId") String operatorId, @QueryParam("propertyName") String propertyName)
  {
    OperatorMeta logicalOperator = dagManager.getLogicalPlan().getOperatorMeta(operatorId);
    if (logicalOperator == null) {
      throw new IllegalArgumentException("Invalid operatorId " + operatorId);
    }
    Map<String, Object> m = LogicalPlanConfiguration.getOperatorProperties(logicalOperator.getOperator());

    try {
      if (propertyName == null) {
        return new JSONObject(new ObjectMapper().writeValueAsString(m));
      }
      else {
        Map<String, Object> m1 = new HashMap<String, Object>();
        m1.put(propertyName, m.get(propertyName));
        return new JSONObject(new ObjectMapper().writeValueAsString(m1));
      }
    }
    catch (Exception ex) {
      LOG.warn("Caught exception", ex);
      throw new RuntimeException(ex);
    }
  }

  @GET
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{operatorId}/properties")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getPhysicalOperatorProperties(@PathParam("operatorId") String operatorId, @QueryParam("propertyName") String propertyName)
  {    
    Map<String, Object> m = dagManager.getPhysicalOperatorProperty(operatorId);

    try {
      if (propertyName == null) {
        return new JSONObject(new ObjectMapper().writeValueAsString(m));
      }
      else {
        Map<String, Object> m1 = new HashMap<String, Object>();
        m1.put(propertyName, m.get(propertyName));
        return new JSONObject(new ObjectMapper().writeValueAsString(m1));
      }
    }
    catch (Exception ex) {
      LOG.warn("Caught exception", ex);
      throw new RuntimeException(ex);
    }
  }
  
  @GET
  @Path(PATH_LOGICAL_PLAN)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getLogicalPlan() throws JSONException, IOException
  {
    LogicalPlan lp = dagManager.getLogicalPlan();
    return new JSONObject(objectMapper.writeValueAsString(LogicalPlanSerializer.convertToMap(lp)));
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path(PATH_LOGICAL_PLAN)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
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

  @PUT
  @Path(PATH_ALERTS + "/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Object createAlert(String content, @PathParam("name") String name) throws JSONException, IOException
  {
    return dagManager.getAlertsManager().createAlert(name, content);
  }

  @DELETE
  @Path(PATH_ALERTS + "/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Object deleteAlert(@PathParam("name") String name) throws JSONException, IOException
  {
    return dagManager.getAlertsManager().deleteAlert(name);
  }

  @GET
  @Path(PATH_ALERTS + "/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Object getAlert(@PathParam("name") String name) throws JSONException, IOException
  {
    JSONObject alert = dagManager.getAlertsManager().getAlert(name);
    if (alert == null) {
      throw new NotFoundException();
    }
    return alert;
  }

  @GET
  @Path(PATH_ALERTS)
  @Produces(MediaType.APPLICATION_JSON)
  public Object listAlerts() throws JSONException, IOException
  {
    return dagManager.getAlertsManager().listAlerts();
  }

  /*
   @GET
   @Path(PATH_ACTION_OPERATOR_CLASSES)
   @Produces(MediaType.APPLICATION_JSON)
   public Object listActionOperatorClasses(@PathParam("appId") String appId) throws JSONException
   {
   JSONObject response = new JSONObject();
   JSONArray jsonArray = new JSONArray();
   List<Class<? extends Operator>> operatorClasses = operatorDiscoverer.getActionOperatorClasses();

   for (Class<? extends Operator> clazz : operatorClasses) {
   jsonArray.put(clazz.getName());
   }
   response.put("classes", jsonArray);
   return response;
   }
   */
}
