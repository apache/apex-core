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
public class AppPackageTest
{
  @Test
  public void testAppPackage() throws Exception
  {
    AppPackage ap = new AppPackage(new File("src/test/resources/testAppPackage/testAppPackage.jar"), true);
    JacksonObjectMapperProvider jomp = new JacksonObjectMapperProvider();
    jomp.addSerializer(LogicalPlan.class, new LogicalPlanSerializer());
    JSONObject json = new JSONObject(jomp.getContext(null).writeValueAsString(ap));

    Assert.assertEquals("pi-demo", json.getString("appPackageName"));
    Assert.assertEquals("1.0-SNAPSHOT", json.getString("appPackageVersion"));
    Assert.assertEquals("1.0.0", json.getString("dtEngineVersion"));
    Assert.assertEquals("lib/*.jar", json.getJSONArray("classPath").getString(0));

    JSONObject application = json.getJSONArray("applications").getJSONObject(0);
    Assert.assertEquals("PiCalculator", application.getString("name"));
    Assert.assertEquals("pi-demo-1.0-SNAPSHOT.jar", application.getString("file"));

    JSONObject dag = application.getJSONObject("dag");
    Assert.assertTrue("There is at least one stream", dag.getJSONArray("streams").length() >= 1);
    Assert.assertTrue("There is at least one operator", dag.getJSONArray("operators").length() >= 1);
  }
}
