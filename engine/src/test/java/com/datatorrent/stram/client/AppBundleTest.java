/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */

package com.datatorrent.stram.client;

import com.datatorrent.lib.util.JacksonObjectMapperProvider;
import com.datatorrent.stram.codec.LogicalPlanSerializer;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import java.io.File;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class AppBundleTest
{
  @Test
  public void testAppBundle() throws Exception
  {
    AppBundle ab = new AppBundle(new File("src/test/resources/testAppBundle.zip"), true);
    JacksonObjectMapperProvider jomp = new JacksonObjectMapperProvider();
    jomp.addSerializer(LogicalPlan.class, new LogicalPlanSerializer());
    JSONObject json = new JSONObject(jomp.getContext(null).writeValueAsString(ab));

    Assert.assertEquals("pi-demo", json.getString("appBundleName"));
    Assert.assertEquals("1.0-SNAPSHOT", json.getString("appBundleVersion"));
    Assert.assertEquals("1.0.4-SNAPSHOT", json.getString("dtEngineVersion"));
    Assert.assertEquals("lib/*.jar", json.getJSONArray("classPath").getString(0));
    
    JSONObject application = json.getJSONArray("applications").getJSONObject(0);
    Assert.assertEquals("PiCalculator", application.getString("name"));
    Assert.assertEquals("pi-demo-1.0-SNAPSHOT.jar", application.getString("jarName"));

    JSONObject dag = application.getJSONObject("dag");
    Assert.assertTrue("There is at least one stream", dag.getJSONArray("streams").length() >= 1);
    Assert.assertTrue("There is at least one operator", dag.getJSONArray("operators").length() >= 1);
  }
}
