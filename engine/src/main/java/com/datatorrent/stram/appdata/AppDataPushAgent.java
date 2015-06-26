/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.stram.appdata;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

import org.apache.commons.lang3.ClassUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context.DAGContext;

import com.datatorrent.common.util.Pair;
import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.WebsocketAppDataPusher;
import com.datatorrent.stram.api.AppDataPusher;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.webapp.LogicalOperatorInfo;

/**
 *
 */
public class AppDataPushAgent extends AbstractService
{
  /**
   * The annotation to denote fields included for metric push. They are for now only valid on the fields of AppInfo and
   * OperatorInfo.
   */
  @Target({ElementType.FIELD, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Metric
  {
  }

  private static final String CUSTOM_METRICS_SCHEMA = "customMetricsSchema";
  private static final String CUSTOM_METRICS_SCHEMA_VERSION = "1.0";
  private static final String DATA = "data";
  private static final Logger LOG = LoggerFactory.getLogger(AppDataPushAgent.class);
  private static final String APP_DATA_PUSH_TRANSPORT_BUILTIN_VALUE = "builtin";
  private final StreamingContainerManager dnmgr;
  private final StramAppContext appContext;
  private final AppDataPushThread appDataPushThread = new AppDataPushThread();
  private AppDataPusher appDataPusher;
  private final Map<Class<?>, List<Field>> cacheFields = new HashMap<Class<?>, List<Field>>();
  private final Map<Class<?>, Map<String, Method>> cacheGetMethods = new HashMap<Class<?>, Map<String, Method>>();

  private final Map<String, Long> operatorsSchemaLastSentTime = Maps.newHashMap();
  private final Map<String, JSONObject> operatorSchemas = Maps.newHashMap();

  public AppDataPushAgent(StreamingContainerManager dnmgr, StramAppContext appContext)
  {
    super(AppDataPushAgent.class.getName());
    this.dnmgr = dnmgr;
    this.appContext = appContext;
  }

  @Override
  protected void serviceStop() throws Exception
  {
    if (appDataPusher != null) {
      appDataPushThread.interrupt();
      try {
        appDataPushThread.join();
      } catch (InterruptedException ex) {
        LOG.error("Error joining with {}", appDataPushThread.getName(), ex);
      }
    }
    super.serviceStop(); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  protected void serviceStart() throws Exception
  {
    if (appDataPusher != null) {
      appDataPushThread.start();
    }
    super.serviceStart();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception
  {
    init();
    super.serviceInit(conf);
  }

  public void init()
  {
    String appDataPushTransport = dnmgr.getLogicalPlan().getValue(DAGContext.METRICS_TRANSPORT);
    if (appDataPushTransport.startsWith(APP_DATA_PUSH_TRANSPORT_BUILTIN_VALUE + ":")) {
      String topic = appDataPushTransport.substring(APP_DATA_PUSH_TRANSPORT_BUILTIN_VALUE.length() + 1);
      appDataPusher = new WebsocketAppDataPusher(dnmgr.getWsClient(), topic);
      LOG.info("App Data Push Transport set up for {}", appDataPushTransport);
    } else {
      // TBD add kakfa
      LOG.error("App Data Push Transport not recognized: {}", appDataPushTransport);
    }
  }

  private JSONObject getPushData()
  {
    // assemble the json that contains the app stats and logical operator stats and counters
    JSONObject json = new JSONObject();
    try {
      json.put("type", DATA);
      json.put("appId", dnmgr.getLogicalPlan().getValue(DAGContext.APPLICATION_ID));
      json.put("appName", dnmgr.getLogicalPlan().getValue(DAGContext.APPLICATION_NAME));
      json.put("appUser", appContext.getUser());
      List<LogicalOperatorInfo> logicalOperatorInfoList = dnmgr.getLogicalOperatorInfoList();
      JSONObject logicalOperators = new JSONObject();
      long resendSchemaInterval = appDataPusher.getResendSchemaInterval();
      for (LogicalOperatorInfo logicalOperator : logicalOperatorInfoList) {
        JSONObject logicalOperatorJson = extractFields(logicalOperator);
        JSONArray customMetricsList = new JSONArray();
        Queue<Pair<Long, Map<String, Object>>> windowMetrics = dnmgr.getWindowMetrics(logicalOperator.name);
        if (windowMetrics != null) {
          while (!windowMetrics.isEmpty()) {
            Pair<Long, Map<String, Object>> metrics = windowMetrics.remove();
            long windowId = metrics.first;
            // metric name, aggregated value
            Map<String, Object> aggregates = metrics.second;
            long now = System.currentTimeMillis();
            if (!operatorsSchemaLastSentTime.containsKey(logicalOperator.name)
                    || operatorsSchemaLastSentTime.get(logicalOperator.name) < now - resendSchemaInterval) {
              try {
                pushCustomMetricSchema(dnmgr.getLogicalPlan().getOperatorMeta(logicalOperator.name), aggregates);
                operatorsSchemaLastSentTime.put(logicalOperator.name, now);
              } catch (IOException ex) {
                LOG.error("Cannot push custom metric schema", ex);
              }
            }
            JSONObject customMetricsItem = new JSONObject();
            customMetricsItem.put("_windowId", windowId);
            long windowToMillis = dnmgr.windowIdToMillis(windowId);
            LOG.debug("metric window {} time {}", windowId, windowToMillis);
            customMetricsItem.put("_time", windowToMillis);
            for (Map.Entry<String, Object> entry : aggregates.entrySet()) {
              String metricName = entry.getKey();
              Object aggregateValue = entry.getValue();
              customMetricsItem.put(metricName, aggregateValue);
            }
            customMetricsList.put(customMetricsItem);
          }
        }
        logicalOperatorJson.put("customMetrics", customMetricsList);
        logicalOperators.put(logicalOperator.name, logicalOperatorJson);
      }
      json.put("time", System.currentTimeMillis());
      json.put("logicalOperators", logicalOperators);
      json.put("stats", extractFields(appContext.getStats()));
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
    return json;
  }

  private JSONObject extractFields(Object o)
  {
    List<Field> fields;
    Map<String, Method> methods;

    if (cacheFields.containsKey(o.getClass())) {
      fields = cacheFields.get(o.getClass());
    } else {
      fields = new ArrayList<Field>();

      for (Class<?> c = o.getClass(); c != Object.class; c = c.getSuperclass()) {
        Field[] declaredFields = c.getDeclaredFields();
        for (Field field : declaredFields) {
          field.setAccessible(true);
          Metric rfa = field.getAnnotation(Metric.class);
          if (rfa != null) {
            field.setAccessible(true);
            try {
              fields.add(field);
            } catch (Exception ex) {
              LOG.debug("Error extracting fields for app data: {}. Ignoring.", ex.getMessage());
            }
          }
        }
      }
      cacheFields.put(o.getClass(), fields);
    }
    JSONObject result = new JSONObject();
    for (Field field : fields) {
      try {
        result.put(field.getName(), field.get(o));
      } catch (Exception ex) {
        // ignore
      }
    }
    if (cacheGetMethods.containsKey(o.getClass())) {
      methods = cacheGetMethods.get(o.getClass());
    } else {
      methods = new HashMap<String, Method>();    
      try {
        BeanInfo info = Introspector.getBeanInfo(o.getClass());
        for (PropertyDescriptor pd : info.getPropertyDescriptors()) {
          Method method = pd.getReadMethod();
          if (pd.getReadMethod() != null) {
            Metric rfa = method.getAnnotation(Metric.class);
            if (rfa != null) {
              methods.put(pd.getName(), method);
            }
          }
        }
      } catch (IntrospectionException ex) {
        // ignore
      }
      cacheGetMethods.put(o.getClass(), methods);
    }
    for (Map.Entry<String, Method> entry : methods.entrySet()) {
      try {
        result.put(entry.getKey(), entry.getValue().invoke(o));
      } catch (Exception ex) {
        // ignore
      }
    }
    return result;
  }

  private JSONObject getCustomMetricsSchemaData(LogicalPlan.OperatorMeta operatorMeta, Map<String, Object> aggregates)
  {
    JSONObject result = new JSONObject();
    try {
      result.put("type", CUSTOM_METRICS_SCHEMA);
      result.put("version", CUSTOM_METRICS_SCHEMA_VERSION);
      result.put("appUser", appContext.getUser());
      result.put("appName", dnmgr.getApplicationAttributes().get(DAGContext.APPLICATION_NAME));
      result.put("logicalOperatorName", operatorMeta.getName());

      LogicalPlan.CustomMetricAggregatorMeta customMetricAggregatorMeta = operatorMeta.getCustomMetricAggregatorMeta();
      JSONArray valueSchemas = new JSONArray();
      for (Map.Entry<String, Object> entry : aggregates.entrySet()) {
        String metricName = entry.getKey();
        Object metricValue = entry.getValue();
        JSONObject valueSchema = new JSONObject();
        valueSchema.put("name", metricName);
        Class<?> type = ClassUtils.wrapperToPrimitive(metricValue.getClass());
        valueSchema.put("type", type == null ? metricValue.getClass().getCanonicalName() : type);
        String[] dimensionAggregators = customMetricAggregatorMeta.getDimensionAggregatorsFor(metricName);
        if (dimensionAggregators != null) {
          valueSchema.put("dimensionAggregators", dimensionAggregators);
        }
        valueSchemas.put(valueSchema);
      }
      result.put("values", valueSchemas);
      String[] timeBuckets = customMetricAggregatorMeta.getTimeBuckets();
      if (timeBuckets != null) {
        result.put("timeBuckets", timeBuckets);
      }

    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
    return result;
  }

  public void pushCustomMetricSchema(LogicalPlan.OperatorMeta operatorMeta, Map<String, Object> aggregates) throws IOException
  {
    JSONObject schema = operatorSchemas.get(operatorMeta.getName());
    if (schema == null) {
      schema = getCustomMetricsSchemaData(operatorMeta, aggregates);
      operatorSchemas.put(operatorMeta.getName(), schema);
    }
    appDataPusher.push(schema);
  }

  public void pushData() throws IOException
  {
    appDataPusher.push(getPushData());
  }

  public class AppDataPushThread extends Thread
  {

    @Override
    public void run()
    {
      while (true) {
        try {
          pushData();
        } catch (IOException ex) {
          LOG.warn("Error during pushing app data", ex);
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ex) {
          LOG.warn("Received interrupt, exiting app data push thread!");
          return;
        }
      }
    }

  }

}
