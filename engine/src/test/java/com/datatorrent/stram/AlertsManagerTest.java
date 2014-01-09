/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import java.net.InetSocketAddress;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.DAGContext;

import com.datatorrent.stram.StreamingContainerManager.ContainerResource;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class AlertsManagerTest
{
  @Test
  public void testAlertManager() throws JSONException
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(DAGContext.APPLICATION_PATH, "target/" + this.getClass().getName());
    dag.addOperator("o", GenericTestOperator.class);
    final StreamingContainerManager dnm = new StreamingContainerManager(dag);
    Assert.assertNotNull(dnm.assignContainer(new ContainerResource(0, "container1", "localhost", 0, null), InetSocketAddress.createUnresolved("localhost", 0)));

    new Thread()
    {
      @Override
      public void run()
      {
        while (true) {
          dnm.processEvents();
          try {
            Thread.sleep(500);
          }
          catch (InterruptedException ex) {
            return;
          }
        }
      }

    }.start();

    AlertsManager alertsManager = dnm.getAlertsManager();
    JSONObject json = new JSONObject();
    JSONObject filter = new JSONObject();
    JSONObject escalation = new JSONObject();
    JSONArray actions = new JSONArray();
    JSONObject action = new JSONObject();
    filter.put("class", GenericTestOperator.class.getName());
    filter.put("inputPort", "input1");
    filter.put("outputPort", "output1");
    escalation.put("class", GenericTestOperator.class.getName());
    escalation.put("inputPort", "input1");
    action.put("class", GenericTestOperator.class.getName());
    action.put("outputPort", "output1");
    action.put("inputPort", "input1");
    actions.put(action);
    json.put("streamName", "o.output1");
    json.put("filter", filter);
    json.put("escalation", escalation);
    json.put("actions", actions);

    // create the alert
    alertsManager.createAlert("alertName", json.toString());
    JSONObject alerts = alertsManager.listAlerts();
    Assert.assertEquals("alert name should match", alerts.getJSONArray("alerts").getJSONObject(0).getString("name"), "alertName");
    Assert.assertNotNull(alertsManager.getAlert("alertName"));

    // make sure we have the operators
    Assert.assertEquals("there should be 4 operators", dag.getAllOperators().size(), 4);
    Assert.assertEquals("there should be 3 streams", dag.getAllStreams().size(), 3);

    // delete the alert
    alertsManager.deleteAlert("alertName");
    alerts = alertsManager.listAlerts();
    Assert.assertEquals("there should be no more alerts after delete", alerts.getJSONArray("alerts").length(), 0);

    // make sure the operators are removed
    Assert.assertEquals("there should be 1 operator", dag.getAllOperators().size(), 1);
    Assert.assertEquals("there should be 0 stream", dag.getAllStreams().size(), 0);
  }

}
