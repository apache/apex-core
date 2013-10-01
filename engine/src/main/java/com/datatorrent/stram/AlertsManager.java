/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.stram.plan.logical.*;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>AlertsManager class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.5
 */
public class AlertsManager
{
  private static final Logger LOG = LoggerFactory.getLogger(AlertsManager.class);
  private final Map<String, AlertInfo> alerts = new HashMap<String, AlertInfo>();
  private StreamingContainerManager dagManager;

  private static class AlertInfo
  {
    String streamName;
    String filterOperatorName;
    String escalationOperatorName;
    List<String> actionOperatorNames = new ArrayList<String>();
    String filterStreamName;
    String escalationStreamName;
    List<String> actionStreamNames = new ArrayList<String>();
    JSONObject createFrom;
  }

  public AlertsManager(StreamingContainerManager dagManager)
  {
    this.dagManager = dagManager;
  }

  public JSONObject createAlert(String name, String content)
  {
    LOG.debug("Creating Alert: {}", content);
    JSONObject response = new JSONObject();
    try {
      JSONObject json = new JSONObject(content);
      String streamName = json.getString("streamName");
      JSONObject filter = json.getJSONObject("filter");
      JSONObject escalation = json.getJSONObject("escalation");
      JSONArray actions = json.getJSONArray("actions");
      JSONObject createFrom = json.optJSONObject("createFrom");
      List<LogicalPlanRequest> requests = new ArrayList<LogicalPlanRequest>();
      AlertInfo alertInfo = new AlertInfo();
      alertInfo.streamName = streamName;

      synchronized (alerts) {
        if (alerts.containsKey(name)) {
          throw new Exception("alert " + name + " already exists");
        }

        alertInfo.createFrom = createFrom;

        // create filter operator
        String filterOperatorName = "_alert_filter_" + name;
        alertInfo.filterOperatorName = filterOperatorName;
        {
          CreateOperatorRequest request = new CreateOperatorRequest();
          request.setOperatorName(filterOperatorName);
          request.setOperatorFQCN(filter.getString("class"));
          requests.add(request);
        }

        // set filter operator properties
        Iterator<String> keys;
        JSONObject properties = filter.optJSONObject("properties");
        if (properties != null) {
          keys = properties.keys();
          while (keys.hasNext()) {
            String key = keys.next();
            Object val = properties.get(key);
            SetOperatorPropertyRequest request = new SetOperatorPropertyRequest();
            request.setOperatorName(filterOperatorName);
            request.setPropertyName(key);
            request.setPropertyValue(val.toString());
            requests.add(request);
          }
        }
        // create escalation operator
        String escalationOperatorName = "_alert_escalation_" + name;
        alertInfo.escalationOperatorName = escalationOperatorName;

        {
          CreateOperatorRequest request = new CreateOperatorRequest();
          request.setOperatorName(escalationOperatorName);
          request.setOperatorFQCN(escalation.getString("class"));
          requests.add(request);
        }

        // set escalation operator properties
        properties = escalation.optJSONObject("properties");
        if (properties != null) {
          keys = properties.keys();
          while (keys.hasNext()) {
            String key = keys.next();
            Object val = properties.get(key);
            SetOperatorPropertyRequest request = new SetOperatorPropertyRequest();
            request.setOperatorName(escalationOperatorName);
            request.setPropertyName(key);
            request.setPropertyValue(val.toString());
            requests.add(request);
          }
        }
        // create action operators and set properties
        for (int i = 0; i < actions.length(); i++) {
          String actionOperatorName = "_alert_action_" + name + "_" + i;
          alertInfo.actionOperatorNames.add(actionOperatorName);

          JSONObject action = actions.getJSONObject(i);
          {
            CreateOperatorRequest request = new CreateOperatorRequest();
            request.setOperatorName(actionOperatorName);
            request.setOperatorFQCN(action.getString("class"));
            requests.add(request);
          }
          properties = action.optJSONObject("properties");
          if (properties != null) {
            keys = properties.keys();
            while (keys.hasNext()) {
              String key = keys.next();
              Object val = properties.get(key);
              SetOperatorPropertyRequest request = new SetOperatorPropertyRequest();
              request.setOperatorName(actionOperatorName);
              request.setPropertyName(key);
              request.setPropertyValue(val.toString());
              requests.add(request);
            }
          }
          // create stream from escalation to actions
          CreateStreamRequest request = new CreateStreamRequest();
          alertInfo.actionStreamNames.add("_alert_stream_action_" + name);
          request.setStreamName("_alert_stream_action_" + name);
          request.setSourceOperatorName(escalationOperatorName);
          request.setSourceOperatorPortName(action.getString("outputPort"));
          request.setSinkOperatorName(actionOperatorName);
          request.setSinkOperatorPortName(action.getString("inputPort"));
          requests.add(request);
        }

        // create stream from existing operator to filter
        {
          StreamMeta stream = dagManager.getLogicalPlan().getStream(streamName);
          if (stream == null) {
            int index = streamName.indexOf('.');
            if (index > 0 && index < streamName.length() - 1) {
              String operatorName = streamName.substring(0, index);
              String portName = streamName.substring(index + 1);
              CreateStreamRequest request = new CreateStreamRequest();
              alertInfo.filterStreamName = streamName;
              request.setStreamName(streamName);
              request.setSourceOperatorName(operatorName);
              request.setSourceOperatorPortName(portName);
              request.setSinkOperatorName(filterOperatorName);
              request.setSinkOperatorPortName(filter.optString("inputPort", "in"));
              requests.add(request);
            }
            else {
              throw new Exception("stream " + streamName + " does not exist.");
            }

          }
          else {
            AddStreamSinkRequest request = new AddStreamSinkRequest();
            alertInfo.filterStreamName = streamName;
            request.setStreamName(streamName);
            request.setSinkOperatorName(filterOperatorName);
            request.setSinkOperatorPortName(filter.optString("inputPort", "in"));
            requests.add(request);
          }
        }

        // create stream from filter to escalation
        {
          CreateStreamRequest request = new CreateStreamRequest();
          alertInfo.escalationStreamName = "_alert_stream_escalation_" + name;
          request.setStreamName("_alert_stream_escalation_" + name);
          request.setSourceOperatorName(filterOperatorName);
          request.setSourceOperatorPortName(filter.optString("outputPort", "out"));
          request.setSinkOperatorName(escalationOperatorName);
          request.setSinkOperatorPortName(filter.optString("inputPort", "in"));
          requests.add(request);
        }
        LOG.info("Alert {} submitted.", name);
        Future<?> fr = dagManager.logicalPlanModification(requests);
        fr.get(3000, TimeUnit.MILLISECONDS);
        alerts.put(name, alertInfo);
        LOG.info("Alert {} added. There are now {} alerts.", name, alerts.size());
      }
    }
    catch (Exception ex) {
      LOG.error("Error adding alert", ex);
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

  public JSONObject deleteAlert(String name) throws JSONException
  {
    JSONObject response = new JSONObject();
    List<LogicalPlanRequest> requests = new ArrayList<LogicalPlanRequest>();

    try {
      synchronized (alerts) {
        if (!alerts.containsKey(name)) {
          throw new NotFoundException();
        }
        AlertInfo alertInfo = alerts.get(name);
        {
          StreamMeta stream = dagManager.getLogicalPlan().getStream(alertInfo.filterStreamName);
          if (stream == null) {
            LOG.warn("Stream to the filter operator does not exist! Ignoring...");
          }
          else {
            List<InputPortMeta> sinks = stream.getSinks();
            if (sinks.size() == 1 && sinks.get(0).getOperatorWrapper().getName().equals(alertInfo.filterOperatorName)) {
              // The only sink is the filter operator, so it's safe to remove
              RemoveStreamRequest request = new RemoveStreamRequest();
              request.setStreamName(alertInfo.filterStreamName);
              requests.add(request);
            }
          }
        }
        {
          RemoveStreamRequest request = new RemoveStreamRequest();
          request.setStreamName(alertInfo.escalationStreamName);
          requests.add(request);
        }
        for (String streamName : alertInfo.actionStreamNames) {
          RemoveStreamRequest request = new RemoveStreamRequest();
          request.setStreamName(streamName);
          requests.add(request);
        }
        {
          RemoveOperatorRequest request = new RemoveOperatorRequest();
          request.setOperatorName(alertInfo.filterOperatorName);
          requests.add(request);
          request = new RemoveOperatorRequest();
          request.setOperatorName(alertInfo.escalationOperatorName);
          requests.add(request);
        }
        for (String operatorName : alertInfo.actionOperatorNames) {
          RemoveOperatorRequest request = new RemoveOperatorRequest();
          request.setOperatorName(operatorName);
          requests.add(request);
        }
        Future<?> fr = dagManager.logicalPlanModification(requests);
        fr.get(3000, TimeUnit.MILLISECONDS);
        alerts.remove(name);
        LOG.info("Alert {} removed. There are now {} alerts.", name, alerts.size());
      }
    }
    catch (Exception ex) {
      LOG.error("Error deleting alert", ex);
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

  public JSONObject listAlerts() throws JSONException
  {
    JSONObject response = new JSONObject();
    JSONArray alertsArray = new JSONArray();
    LOG.info("There are {} alerts", alerts.size());
    synchronized (alerts) {
      for (Map.Entry<String, AlertInfo> entry : alerts.entrySet()) {
        JSONObject alert = new JSONObject();
        alert.put("name", entry.getKey());
        alert.put("streamName", entry.getValue().streamName);
        alert.put("createFrom", entry.getValue().createFrom);
        alertsArray.put(alert);
      }
    }
    response.put("alerts", alertsArray);
    return response;
  }

  public JSONObject getAlert(String name) throws JSONException
  {
    JSONObject response = new JSONObject();
    AlertInfo alertInfo;
    synchronized (alerts) {
      alertInfo = alerts.get(name);
    }
    if (alertInfo == null) {
      return null;
    }
    response.put("name", name);
    response.put("streamName", alertInfo.streamName);
    response.put("createFrom", alertInfo.createFrom);
    return response;
  }
}
