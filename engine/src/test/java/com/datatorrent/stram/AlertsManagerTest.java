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
import org.junit.Rule;
import org.junit.Test;

import com.datatorrent.api.DAGContext;
import com.datatorrent.stram.StreamingContainerManager.ContainerResource;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.support.StramTestSupport.TestMeta;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class AlertsManagerTest
{
  @Rule public TestMeta testMeta = new TestMeta();

  @Test
  public void testAlertManager() throws JSONException
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(DAGContext.APPLICATION_PATH, "target/" + this.getClass().getName());
    dag.addOperator("o", GenericTestOperator.class);
    final StreamingContainerManager dnm = new StreamingContainerManager(dag);
    Assert.assertNotNull(dnm.assignContainer(new ContainerResource(0, "container1", "localhost", 0, null), InetSocketAddress.createUnresolved("localhost", 0)));

    Thread t = new Thread()
    {
      @Override
      public void run()
      {
        while (true) {
          for (PTOperator o : dnm.getPhysicalPlan().getAllOperators().values()) {
            o.setState(PTOperator.State.ACTIVE);
          }
          dnm.processEvents();
          try {
            Thread.sleep(500);
          }
          catch (InterruptedException ex) {
            return;
          }
        }
      }

    };

    t.start();

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
    Assert.assertEquals("alert name should match", "alertName", alerts.getJSONArray("alerts").getJSONObject(0).getString("name"));
    Assert.assertNotNull(alertsManager.getAlert("alertName"));

    // make sure we have the operators
    Assert.assertEquals("there should be 4 operators", 4, dag.getAllOperators().size());
    Assert.assertEquals("there should be 3 streams", 3, dag.getAllStreams().size());

    // delete the alert
    alertsManager.deleteAlert("alertName");
    alerts = alertsManager.listAlerts();
    Assert.assertEquals("there should be no more alerts after delete", 0, alerts.getJSONArray("alerts").length());

    // make sure the operators are removed
    Assert.assertEquals("there should be 1 operator", 1, dag.getAllOperators().size());
    Assert.assertEquals("there should be 0 stream", 0, dag.getAllStreams().size());

    t.interrupt();
    try {
      t.join();
    }
    catch (InterruptedException ex) {
      // ignore
    }
  }

}
