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
package com.datatorrent.stram.webapp;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.map.ser.std.SerializerBase;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.beanutils.BeanMap;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StringCodec;
import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StramUtils;
import com.datatorrent.stram.StreamingContainerAgent;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.StringCodecs;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol;
import com.datatorrent.stram.codec.LogicalPlanSerializer;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.ModuleMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import com.datatorrent.stram.plan.logical.requests.LogicalPlanRequest;
import com.datatorrent.stram.util.ConfigValidator;
import com.datatorrent.stram.util.JSONSerializationProvider;
import com.datatorrent.stram.util.LoggerUtil;

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
@Singleton
@Path(StramWebServices.PATH)
public class StramWebServices
{
  private static final Logger LOG = LoggerFactory.getLogger(StramWebServices.class);
  public static final String PATH = WebServices.PATH + "/" + WebServices.VERSION + "/stram";
  public static final String PATH_INFO = "info";
  public static final String PATH_PHYSICAL_PLAN = "physicalPlan";
  public static final String PATH_PHYSICAL_PLAN_OPERATORS = PATH_PHYSICAL_PLAN + "/operators";
  public static final String PATH_PHYSICAL_PLAN_STREAMS = PATH_PHYSICAL_PLAN + "/streams";
  public static final String PATH_PHYSICAL_PLAN_CONTAINERS = PATH_PHYSICAL_PLAN + "/containers";
  public static final String PATH_SHUTDOWN = "shutdown";
  public static final String PATH_RECORDINGS = "recordings";
  public static final String PATH_RECORDINGS_START = PATH_RECORDINGS + "/start";
  public static final String PATH_RECORDINGS_STOP = PATH_RECORDINGS + "/stop";
  public static final String PATH_LOGICAL_PLAN = "logicalPlan";
  public static final String PATH_LOGICAL_PLAN_OPERATORS = PATH_LOGICAL_PLAN + "/operators";
  public static final String PATH_OPERATOR_CLASSES = "operatorClasses";
  public static final String PATH_ALERTS = "alerts";
  public static final String PATH_LOGGERS = "loggers";
  public static final String PATH_STACKTRACE = "stackTrace";
  public static final long WAIT_TIME = 5000;
  public static final long STACK_TRACE_WAIT_TIME = 1000;
  public static final long STACK_TRACE_ATTEMPTS = 10;


  //public static final String PATH_ACTION_OPERATOR_CLASSES = "actionOperatorClasses";
  private StramAppContext appCtx;
  @Context
  private HttpServletResponse httpResponse;
  @Inject
  @Nullable
  private StreamingContainerManager dagManager;
  private ObjectMapper objectMapper = new JSONSerializationProvider().getContext(null);
  private boolean initialized = false;

  private OperatorDiscoverer operatorDiscoverer = new OperatorDiscoverer();

  @Inject
  public StramWebServices(StramAppContext context)
  {
    this.appCtx = context;
  }

  Boolean hasAccess(HttpServletRequest request)
  {
    String remoteUser = request.getRemoteUser();
    if (remoteUser != null) {
      UserGroupInformation callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
      if (callerUGI != null) {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void init()
  {
    //clear content type
    httpResponse.setContentType(null);
    if (!initialized) {
      Map<Class<?>, Class<? extends StringCodec<?>>> codecs = dagManager.getApplicationAttributes().get(DAGContext.STRING_CODECS);
      StringCodecs.loadConverters(codecs);
      if (codecs != null) {
        SimpleModule sm = new SimpleModule("DTSerializationModule", new Version(1, 0, 0, null));
        for (Map.Entry<Class<?>, Class<? extends StringCodec<?>>> entry : codecs.entrySet()) {
          try {
            final StringCodec<Object> codec = (StringCodec<Object>)entry.getValue().newInstance();
            sm.addSerializer(new SerializerBase(entry.getKey())
            {
              @Override
              public void serialize(Object value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException
              {
                jgen.writeString(codec.toString(value));
              }

            });
          } catch (Exception ex) {
            LOG.error("Caught exception when instantiating codec for class {}", entry.getKey().getName(), ex);
          }
        }

        objectMapper.registerModule(sm);
      }
      initialized = true;
    }
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
    init();
    Map<String, Object> result = new HashMap<>();
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
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{operatorId:\\d+}")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getOperatorInfo(@PathParam("operatorId") int operatorId) throws Exception
  {
    init();
    OperatorInfo oi = dagManager.getOperatorInfo(operatorId);
    if (oi == null) {
      throw new NotFoundException();
    }
    return new JSONObject(objectMapper.writeValueAsString(oi));
  }

  @GET
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{operatorId:\\d+}/ports")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getPortsInfo(@PathParam("operatorId") int operatorId) throws Exception
  {
    init();
    Map<String, Object> map = new HashMap<>();
    OperatorInfo oi = dagManager.getOperatorInfo(operatorId);
    if (oi == null) {
      throw new NotFoundException();
    }
    map.put("ports", oi.ports);
    return new JSONObject(objectMapper.writeValueAsString(map));
  }

  @GET
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{operatorId:\\d+}/ports/{portName}")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getPortsInfo(@PathParam("operatorId") int operatorId, @PathParam("portName") String portName) throws Exception
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
  public JSONObject getOperatorClasses(@QueryParam("q") String searchTerm, @QueryParam("parent") String parent)
  {
    init();
    JSONObject result = new JSONObject();
    JSONArray classNames = new JSONArray();

    if (parent != null) {
      if (parent.equals("chart")) {
        parent = "com.datatorrent.lib.chart.ChartOperator";
      } else if (parent.equals("filter")) {
        parent = "com.datatorrent.common.util.SimpleFilterOperator";
      }
    }

    try {
      Set<String> operatorClasses = operatorDiscoverer.getOperatorClasses(parent, searchTerm);

      for (String clazz : operatorClasses) {
        JSONObject j = new JSONObject();
        j.put("name", clazz);
        classNames.put(j);
      }

      result.put("operatorClasses", classNames);
    } catch (ClassNotFoundException ex) {
      throw new NotFoundException();
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
    return result;
  }

  @GET
  @Path(PATH_OPERATOR_CLASSES + "/{className}")
  @Produces(MediaType.APPLICATION_JSON)
  @SuppressWarnings("unchecked")
  public JSONObject describeOperator(@PathParam("className") String className)
  {
    init();
    if (className == null) {
      throw new UnsupportedOperationException();
    }
    try {
      Class<?> clazz = Class.forName(className);
      if (Operator.class.isAssignableFrom(clazz)) {
        return operatorDiscoverer.describeOperator(className);
      } else {
        throw new NotFoundException();
      }
    } catch (Exception ex) {
      throw new NotFoundException();
    }
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path(PATH_SHUTDOWN)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject shutdown()
  {
    init();
    LOG.debug("Shutdown requested");
    dagManager.shutdownAllContainers(StreamingContainerUmbilicalProtocol.ShutdownType.WAIT_TERMINATE, "Shutdown requested externally.");
    return new JSONObject();
  }

  private static String getTupleRecordingId()
  {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
    String val = sdf.format(new Date());
    val += "-";
    byte[] r = new byte[4];
    new Random().nextBytes(r);
    val += Base64.encodeBase64URLSafeString(r);
    return val;
  }

  @POST
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{opId:\\d+}/" + PATH_RECORDINGS_START)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject startRecording(@PathParam("opId") int opId, String content) throws JSONException
  {
    init();
    LOG.debug("Start recording on {} requested", opId);
    JSONObject response = new JSONObject();
    long numWindows = 0;
    if (StringUtils.isNotBlank(content)) {
      JSONObject r = new JSONObject(content);
      numWindows = r.optLong("numWindows", 0);
    }
    String id = getTupleRecordingId();
    dagManager.startRecording(id, opId, null, numWindows);
    response.put("id", id);
    return response;
  }

  @POST
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{opId:\\d+}/ports/{portName}/" + PATH_RECORDINGS_START)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject startRecording(@PathParam("opId") int opId, @PathParam("portName") String portName, String content) throws JSONException
  {
    init();
    LOG.debug("Start recording on {}.{} requested", opId, portName);
    JSONObject response = new JSONObject();
    long numWindows = 0;
    if (StringUtils.isNotBlank(content)) {
      JSONObject r = new JSONObject(content);
      numWindows = r.optLong("numWindows", 0);
    }
    String id = getTupleRecordingId();
    dagManager.startRecording(id, opId, portName, numWindows);
    response.put("id", id);
    return response;
  }

  @POST
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{opId:\\d+}/" + PATH_RECORDINGS_STOP)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject stopRecording(@PathParam("opId") int opId)
  {
    init();
    LOG.debug("Start recording on {} requested", opId);
    JSONObject response = new JSONObject();
    dagManager.stopRecording(opId, null);
    return response;
  }

  @POST
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{opId:\\d+}/ports/{portName}/" + PATH_RECORDINGS_STOP)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject stopRecording(@PathParam("opId") int opId, @PathParam("portName") String portName)
  {
    init();
    LOG.debug("Stop recording on {}.{} requested", opId, portName);
    JSONObject response = new JSONObject();
    dagManager.stopRecording(opId, portName);
    return response;
  }

  @GET
  @Path(PATH_PHYSICAL_PLAN_CONTAINERS)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject listContainers(@QueryParam("states") String states) throws Exception
  {
    init();
    Set<String> stateSet = null;
    if (states != null) {
      stateSet = new HashSet<>();
      stateSet.addAll(Arrays.asList(StringUtils.split(states, ',')));
    }
    ContainersInfo ci = new ContainersInfo();
    for (ContainerInfo containerInfo : dagManager.getCompletedContainerInfo()) {
      if (stateSet == null || stateSet.contains(containerInfo.state)) {
        ci.add(containerInfo);
      }
    }

    Collection<StreamingContainerAgent> containerAgents = dagManager.getContainerAgents();
    // add itself (app master container)
    ContainerInfo appMasterContainerInfo = dagManager.getAppMasterContainerInfo();
    if (stateSet == null || stateSet.contains(appMasterContainerInfo.state)) {
      ci.add(appMasterContainerInfo);
    }
    for (StreamingContainerAgent sca : containerAgents) {
      ContainerInfo containerInfo = sca.getContainerInfo();
      if (stateSet == null || stateSet.contains(containerInfo.state)) {
        ci.add(containerInfo);
      }
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
    ContainerInfo ci = null;
    if (containerId.equals(System.getenv(ApplicationConstants.Environment.CONTAINER_ID.toString()))) {
      ci = dagManager.getAppMasterContainerInfo();
    } else {
      for (ContainerInfo containerInfo : dagManager.getCompletedContainerInfo()) {
        if (containerInfo.id.equals(containerId)) {
          ci = containerInfo;
        }
      }
      if (ci == null) {
        StreamingContainerAgent sca = dagManager.getContainerAgent(containerId);
        if (sca == null) {
          throw new NotFoundException();
        }
        ci = sca.getContainerInfo();
      }
    }
    return new JSONObject(objectMapper.writeValueAsString(ci));
  }

  @GET
  @Path(PATH_PHYSICAL_PLAN_CONTAINERS + "/{containerId}/" + PATH_STACKTRACE)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getContainerStackTrace(@PathParam("containerId") String containerId) throws Exception
  {
    init();

    if (containerId.equals(System.getenv(ApplicationConstants.Environment.CONTAINER_ID.toString()))) {
      return StramUtils.getStackTrace();
    }

    StreamingContainerAgent sca = dagManager.getContainerAgent(containerId);

    if (sca == null) {
      throw new NotFoundException("Container not found.");
    }

    if (!sca.getContainerInfo().state.equals("ACTIVE")) {
      throw new NotFoundException("Container is not active.");
    }

    for (int i = 0; i < STACK_TRACE_ATTEMPTS; ++i) {
      String result = sca.getStackTrace();

      if (result != null) {
        return new JSONObject(result);
      }

      Thread.sleep(STACK_TRACE_WAIT_TIME);
    }

    throw new TimeoutException("Not able to get the stack trace");
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path(PATH_PHYSICAL_PLAN_CONTAINERS + "/{containerId}/kill")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject killContainer(@PathParam("containerId") String containerId)
  {
    init();
    JSONObject response = new JSONObject();
    if (containerId.equals(System.getenv(ApplicationConstants.Environment.CONTAINER_ID.toString()))) {
      LOG.info("Received a kill request on application master container. Exiting.");
      new Thread()
      {
        @Override
        public void run()
        {
          try {
            Thread.sleep(3000);
            System.exit(1);
          } catch (InterruptedException ex) {
            LOG.info("Received interrupt, aborting exit.");
          }
        }

      }.start();
    } else {
      dagManager.stopContainer(containerId);
    }
    return response;
  }

  @GET
  @Path(PATH_LOGICAL_PLAN_OPERATORS)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getLogicalOperators() throws Exception
  {
    init();
    LogicalOperatorsInfo nodeList = new LogicalOperatorsInfo();
    nodeList.operators = dagManager.getLogicalOperatorInfoList();
    return new JSONObject(objectMapper.writeValueAsString(nodeList));
  }

  @GET
  @Path(PATH_LOGICAL_PLAN_OPERATORS + "/{operatorName}")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getLogicalOperator(@PathParam("operatorName") String operatorName) throws Exception
  {
    init();
    OperatorMeta logicalOperator = dagManager.getLogicalPlan().getOperatorMeta(operatorName);
    if (logicalOperator == null) {
      throw new NotFoundException();
    }

    LogicalOperatorInfo logicalOperatorInfo = dagManager.getLogicalOperatorInfo(operatorName);
    return new JSONObject(objectMapper.writeValueAsString(logicalOperatorInfo));
  }

  @GET
  @Path(PATH_LOGICAL_PLAN_OPERATORS + "/{operatorName}/aggregation")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getOperatorAggregation(@PathParam("operatorName") String operatorName) throws Exception
  {
    init();
    OperatorMeta logicalOperator = dagManager.getLogicalPlan().getOperatorMeta(operatorName);
    if (logicalOperator == null) {
      throw new NotFoundException();
    }

    OperatorAggregationInfo operatorAggregationInfo = dagManager.getOperatorAggregationInfo(operatorName);
    return new JSONObject(objectMapper.writeValueAsString(operatorAggregationInfo));
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path(PATH_LOGICAL_PLAN_OPERATORS + "/{operatorName}/properties")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject setOperatorProperties(JSONObject request, @PathParam("operatorName") String operatorName)
  {
    init();
    OperatorMeta logicalOperator = dagManager.getLogicalPlan().getOperatorMeta(operatorName);
    if (logicalOperator == null) {
      throw new NotFoundException();
    }
    JSONObject response = new JSONObject();
    try {
      @SuppressWarnings("unchecked")
      Iterator<String> keys = request.keys();
      while (keys.hasNext()) {
        String key = keys.next();
        String val = request.isNull(key) ? null : request.getString(key);
        LOG.debug("Setting property for {}: {}={}", operatorName, key, val);
        dagManager.setOperatorProperty(operatorName, key, val);
      }
    } catch (JSONException ex) {
      LOG.warn("Got JSON Exception: ", ex);
    } catch (Exception ex) {
      LOG.error("Caught exception: ", ex);
      throw new RuntimeException(ex);
    }
    return response;
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{operatorId:\\d+}/properties")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject setPhysicalOperatorProperties(JSONObject request, @PathParam("operatorId") int operatorId)
  {
    init();
    JSONObject response = new JSONObject();
    try {
      @SuppressWarnings("unchecked")
      Iterator<String> keys = request.keys();
      while (keys.hasNext()) {
        String key = keys.next();
        String val = request.isNull(key) ? null : request.getString(key);
        dagManager.setPhysicalOperatorProperty(operatorId, key, val);
      }
    } catch (JSONException ex) {
      LOG.warn("Got JSON Exception: ", ex);
    }
    return response;
  }

  @GET
  @Path(PATH_LOGICAL_PLAN_OPERATORS + "/{operatorName}/attributes")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getOperatorAttributes(@PathParam("operatorName") String operatorName, @QueryParam("attributeName") String attributeName)
  {
    init();
    OperatorMeta logicalOperator = dagManager.getLogicalPlan().getOperatorMeta(operatorName);
    if (logicalOperator == null) {
      throw new NotFoundException();
    }
    HashMap<String, String> map = new HashMap<>();
    for (Map.Entry<Attribute<?>, Object> entry : dagManager.getOperatorAttributes(operatorName).entrySet()) {
      if (attributeName == null || entry.getKey().getSimpleName().equals(attributeName)) {
        Map.Entry<Attribute<Object>, Object> entry1 = (Map.Entry<Attribute<Object>, Object>)(Map.Entry)entry;
        map.put(entry1.getKey().getSimpleName(), entry1.getKey().codec.toString(entry1.getValue()));
      }
    }
    return new JSONObject(map);
  }

  @GET
  @Path(PATH_LOGICAL_PLAN + "/attributes")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getApplicationAttributes(@QueryParam("attributeName") String attributeName)
  {
    init();
    HashMap<String, String> map = new HashMap<>();
    for (Map.Entry<Attribute<?>, Object> entry : dagManager.getApplicationAttributes().entrySet()) {
      if (attributeName == null || entry.getKey().getSimpleName().equals(attributeName)) {
        Map.Entry<Attribute<Object>, Object> entry1 = (Map.Entry<Attribute<Object>, Object>)(Map.Entry)entry;
        map.put(entry1.getKey().getSimpleName(), entry1.getKey().codec.toString(entry1.getValue()));
      }
    }
    return new JSONObject(map);
  }

  @GET
  @Path(PATH_LOGICAL_PLAN_OPERATORS + "/{operatorName}/ports")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getPorts(@PathParam("operatorName") String operatorName)
  {
    init();
    OperatorMeta logicalOperator = dagManager.getLogicalPlan().getOperatorMeta(operatorName);
    Set<LogicalPlan.InputPortMeta> inputPorts;
    Set<LogicalPlan.OutputPortMeta> outputPorts;
    if (logicalOperator == null) {
      ModuleMeta logicalModule = dagManager.getModuleMeta(operatorName);
      if (logicalModule == null) {
        throw new NotFoundException();
      }
      inputPorts = logicalModule.getInputStreams().keySet();
      outputPorts = logicalModule.getOutputStreams().keySet();
    } else {
      inputPorts = logicalOperator.getInputStreams().keySet();
      outputPorts = logicalOperator.getOutputStreams().keySet();
    }

    JSONObject result = getPortsObjects(inputPorts, outputPorts);
    return result;
  }

  private JSONObject getPortsObjects(Collection<LogicalPlan.InputPortMeta> inputs, Collection<LogicalPlan.OutputPortMeta> outputs)
  {
    JSONObject result = new JSONObject();
    JSONArray ports = new JSONArray();
    try {
      for (LogicalPlan.InputPortMeta inputPort : inputs) {
        JSONObject port = new JSONObject();
        port.put("name", inputPort.getPortName());
        port.put("type", "input");
        ports.put(port);
      }
      for (LogicalPlan.OutputPortMeta outputPort : outputs) {
        JSONObject port = new JSONObject();
        port.put("name", outputPort.getPortName());
        port.put("type", "output");
        ports.put(port);
      }
      result.put("ports", ports);
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
    return result;
  }

  private JSONObject getPortObject(Collection<LogicalPlan.InputPortMeta> inputs,
      Collection<LogicalPlan.OutputPortMeta> outputs,
      String portName) throws JSONException
  {
    for (LogicalPlan.InputPortMeta inputPort : inputs) {
      if (inputPort.getPortName().equals(portName)) {
        JSONObject port = new JSONObject();
        port.put("name", inputPort.getPortName());
        port.put("type", "input");
        return port;
      }
    }
    for (LogicalPlan.OutputPortMeta outputPort : outputs) {
      if (outputPort.getPortName().equals(portName)) {
        JSONObject port = new JSONObject();
        port.put("name", outputPort.getPortName());
        port.put("type", "output");
        return port;
      }
    }
    return null;
  }


  @GET
  @Path(PATH_LOGICAL_PLAN_OPERATORS + "/{operatorName}/ports/{portName}")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getPort(@PathParam("operatorName") String operatorName, @PathParam("portName") String portName)
  {
    init();
    OperatorMeta logicalOperator = dagManager.getLogicalPlan().getOperatorMeta(operatorName);
    Set<LogicalPlan.InputPortMeta> inputPorts;
    Set<LogicalPlan.OutputPortMeta> outputPorts;
    if (logicalOperator == null) {
      ModuleMeta logicalModule = dagManager.getModuleMeta(operatorName);
      if (logicalModule == null) {
        throw new NotFoundException();
      }
      inputPorts = logicalModule.getInputStreams().keySet();
      outputPorts = logicalModule.getOutputStreams().keySet();
    } else {
      inputPorts = logicalOperator.getInputStreams().keySet();
      outputPorts = logicalOperator.getOutputStreams().keySet();
    }

    try {
      JSONObject resp = getPortObject(inputPorts, outputPorts, portName);
      if (resp != null) {
        return resp;
      }
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
    throw new NotFoundException();
  }

  @GET
  @Path(PATH_LOGICAL_PLAN_OPERATORS + "/{operatorName}/ports/{portName}/attributes")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getPortAttributes(@PathParam("operatorName") String operatorName, @PathParam("portName") String portName, @QueryParam("attributeName") String attributeName)
  {
    init();
    OperatorMeta logicalOperator = dagManager.getLogicalPlan().getOperatorMeta(operatorName);
    if (logicalOperator == null) {
      throw new NotFoundException();
    }
    HashMap<String, String> map = new HashMap<>();
    for (Map.Entry<Attribute<?>, Object> entry : dagManager.getPortAttributes(operatorName, portName).entrySet()) {
      if (attributeName == null || entry.getKey().getSimpleName().equals(attributeName)) {
        Map.Entry<Attribute<Object>, Object> entry1 = (Map.Entry<Attribute<Object>, Object>)(Map.Entry)entry;
        map.put(entry1.getKey().getSimpleName(), entry1.getKey().codec.toString(entry1.getValue()));
      }
    }
    return new JSONObject(map);
  }

  @GET
  @Path(PATH_LOGICAL_PLAN_OPERATORS + "/{operatorName}/properties")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getOperatorProperties(@PathParam("operatorName") String operatorName, @QueryParam("propertyName") String propertyName) throws IOException, JSONException
  {
    init();
    OperatorMeta logicalOperator = dagManager.getLogicalPlan().getOperatorMeta(operatorName);
    BeanMap operatorProperties = null;
    if (logicalOperator == null) {
      ModuleMeta logicalModule = dagManager.getModuleMeta(operatorName);
      if (logicalModule == null) {
        throw new NotFoundException();
      }
      operatorProperties = LogicalPlanConfiguration.getObjectProperties(logicalModule.getOperator());
    } else {
      operatorProperties = LogicalPlanConfiguration.getObjectProperties(logicalOperator.getOperator());
    }

    Map<String, Object> m = getPropertiesAsMap(propertyName, operatorProperties);
    return new JSONObject(objectMapper.writeValueAsString(m));
  }

  private Map<String, Object> getPropertiesAsMap(@QueryParam("propertyName") String propertyName, BeanMap operatorProperties)
  {
    Map<String, Object> m = new HashMap<>();
    @SuppressWarnings("rawtypes")
    Iterator entryIterator = operatorProperties.entryIterator();
    while (entryIterator.hasNext()) {
      try {
        @SuppressWarnings("unchecked")
        Map.Entry<String, Object> entry = (Map.Entry<String, Object>)entryIterator.next();
        if (propertyName == null) {
          m.put(entry.getKey(), entry.getValue());
        } else if (propertyName.equals(entry.getKey())) {
          m.put(entry.getKey(), entry.getValue());
          break;
        }
      } catch (Exception ex) {
        LOG.warn("Caught exception", ex);
      }
    }
    return m;
  }

  @GET
  @Path(PATH_PHYSICAL_PLAN_OPERATORS + "/{operatorId:\\d+}/properties")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getPhysicalOperatorProperties(@PathParam("operatorId") int operatorId, @QueryParam("propertyName") String propertyName, @QueryParam("waitTime") long waitTime)
  {
    init();
    if (waitTime == 0) {
      waitTime = WAIT_TIME;
    }

    Future<?> future = dagManager.getPhysicalOperatorProperty(operatorId, propertyName, waitTime);

    try {
      Object object = future.get(waitTime, TimeUnit.MILLISECONDS);
      if (object != null) {
        return new JSONObject(new ObjectMapper().writeValueAsString(object));
      }
    } catch (Exception ex) {
      LOG.warn("Caught exception", ex);
      throw new RuntimeException(ex);
    }
    return new JSONObject();
  }

  @GET
  @Path(PATH_LOGICAL_PLAN)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getLogicalPlan(@QueryParam("includeModules") String includeModules) throws JSONException, IOException
  {
    init();
    return new JSONObject(objectMapper.writeValueAsString(LogicalPlanSerializer.convertToMap(
        dagManager.getLogicalPlan(), includeModules != null)));
  }

  @POST // not supported by WebAppProxyServlet, can only be called directly
  @Path(PATH_LOGICAL_PLAN)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject logicalPlanModification(JSONObject request)
  {
    init();
    JSONObject response = new JSONObject();
    try {
      JSONArray jsonArray = request.getJSONArray("requests");
      List<LogicalPlanRequest> requests = new ArrayList<>();
      for (int i = 0; i < jsonArray.length(); i++) {
        JSONObject jsonObj = (JSONObject)jsonArray.get(i);
        LogicalPlanRequest requestObj = (LogicalPlanRequest)Class.forName(LogicalPlanRequest.class.getPackage().getName() + "." + jsonObj.getString("requestType")).newInstance();
        @SuppressWarnings("unchecked")
        Map<String, String> properties = BeanUtils.describe(requestObj);
        @SuppressWarnings("unchecked")
        Iterator<String> keys = jsonObj.keys();

        while (keys.hasNext()) {
          String key = keys.next();
          if (!key.equals("requestType")) {
            properties.put(key, jsonObj.get(key).toString());
          }
        }
        BeanUtils.populate(requestObj, properties);
        requests.add(requestObj);
      }
      Future<?> fr = dagManager.logicalPlanModification(requests);
      fr.get(3000, TimeUnit.MILLISECONDS);
    } catch (Exception ex) {
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

  @POST
  @Path(PATH_LOGGERS)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject setLoggersLevel(JSONObject request)
  {
    init();
    JSONObject response = new JSONObject();
    Map<String, String> targetChanges = Maps.newHashMap();
    try {
      @SuppressWarnings("unchecked")
      JSONArray loggerArray = request.getJSONArray("loggers");
      for (int i = 0; i < loggerArray.length(); i++) {
        JSONObject loggerNode = loggerArray.getJSONObject(i);
        String target = loggerNode.getString("target");
        String level = loggerNode.getString("logLevel");
        if (ConfigValidator.validateLoggersLevel(target, level)) {
          LOG.info("changing logger level for {} to {}", target, level);
          targetChanges.put(target, level);
        } else {
          LOG.warn("incorrect logger settings {}:{}", target, level);
        }
      }

      if (!targetChanges.isEmpty()) {
        dagManager.setLoggersLevel(Collections.unmodifiableMap(targetChanges));
        //Changing the levels on Stram after sending the message to all containers.
        LoggerUtil.changeLoggersLevel(targetChanges);
      }
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
    return response;
  }

  @GET
  @Path(PATH_LOGGERS + "/search")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject searchLoggersLevel(@QueryParam("pattern") String pattern)
  {
    init();
    JSONObject response = new JSONObject();
    JSONArray loggersArray = new JSONArray();
    try {
      if (pattern != null) {
        Map<String, String> matches = LoggerUtil.getClassesMatching(pattern);
        for (Map.Entry<String, String> match : matches.entrySet()) {
          JSONObject node = new JSONObject();
          node.put("name", match.getKey());
          node.put("level", match.getValue());
          loggersArray.put(node);
        }
      }
      response.put("loggers", loggersArray);
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
    return response;
  }

  @GET
  @Path(PATH_LOGGERS)
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getLoggerLevels() throws JSONException
  {
    init();
    JSONObject response = new JSONObject();
    JSONArray levelsArray = new JSONArray();
    Map<String, String> currentLevels = LoggerUtil.getPatternLevels();
    for (Map.Entry<String, String> lvl : currentLevels.entrySet()) {
      JSONObject node = new JSONObject();
      node.put("target", lvl.getKey());
      node.put("logLevel", lvl.getValue());
      levelsArray.put(node);
    }
    response.put("loggers", levelsArray);
    return response;
  }
}
