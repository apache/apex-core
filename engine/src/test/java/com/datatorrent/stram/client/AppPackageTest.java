/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */

package com.datatorrent.stram.client;

import com.datatorrent.lib.util.JacksonObjectMapperProvider;
import com.datatorrent.stram.codec.LogicalPlanSerializer;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.support.StramTestSupport;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.io.FileUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class AppPackageTest
{

  // file basename for the created jar
  private static final String jarPath = "testAppPackage.jar";

  // The jar file to use for the AppPackage constructor
  private File file;

  private AppPackage ap;
  private JacksonObjectMapperProvider jomp;
  private JSONObject json;

  public class TestMeta extends TestWatcher
  {

    TemporaryFolder testFolder = new TemporaryFolder();

    @Override
    protected void starting(Description description) {
      try {

        // Set up jar file to use with constructor
        testFolder.create();
        file = StramTestSupport.createAppPackageFile(new File(testFolder.getRoot(), jarPath));

        // Set up test instance
        ap = new AppPackage(file, true);
        jomp = new JacksonObjectMapperProvider();
        jomp.addSerializer(LogicalPlan.class, new LogicalPlanSerializer());
        json = new JSONObject(jomp.getContext(null).writeValueAsString(ap));

      } catch (ZipException e) {
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (JSONException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void finished(Description description) {
      super.finished(description);
      try {
        FileUtils.forceDelete(file);
        testFolder.delete();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testAppPackage() throws Exception
  {
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

  @Test
  public void testRequiredProperties()
  {
    Set<String> requiredProperties = ap.getRequiredProperties();
    Assert.assertEquals(2, requiredProperties.size());
    String[] rp = requiredProperties.toArray(new String[]{});
    Assert.assertEquals("dt.test.required.1", rp[0]);
    Assert.assertEquals("dt.test.required.2", rp[1]);
  }

  @Test
  public void testAppLevelRequiredProperties()
  {
    List<AppPackage.AppInfo> applications = ap.getApplications();
    for (AppPackage.AppInfo app : applications) {
      if (app.name.equals("PiCalculator")) {
        String[] rp = app.requiredProperties.toArray(new String[]{});
        Assert.assertEquals("dt.test.required.2", rp[0]);
        Assert.assertEquals("dt.test.required.3", rp[1]);
        Assert.assertEquals("app-default-for-required-1", app.defaultProperties.get("dt.test.required.1"));
        return;
      }
    }
    Assert.fail("Should consist of an app called PiCalculator");
  }
}
