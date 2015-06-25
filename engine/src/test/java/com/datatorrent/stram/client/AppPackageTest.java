/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */

package com.datatorrent.stram.client;

import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.util.JSONSerializationProvider;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.io.IOUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class AppPackageTest
{
  private static AppPackage ap;
  private static JSONSerializationProvider jomp;
  private static JSONObject json;

  String appPackageDir = "src/test/resources/testAppPackage/mydtapp";

  @BeforeClass
  public static void starting()
  {
    try {
      File file = StramTestSupport.createAppPackageFile();
      // Set up test instance
      ap = new AppPackage(file, true);
      jomp = new JSONSerializationProvider();
      json = new JSONObject(jomp.getContext(null).writeValueAsString(ap));

    } catch (ZipException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void finished()
  {
    StramTestSupport.removeAppPackageFile();
  }

  @Test
  public void testAppPackage() throws Exception
  {
    Assert.assertEquals("mydtapp", json.getString("appPackageName"));
    Assert.assertEquals("1.0-SNAPSHOT", json.getString("appPackageVersion"));
    Assert.assertEquals("2.2.0-SNAPSHOT", json.getString("dtEngineVersion"));
    Assert.assertEquals("lib/*.jar", json.getJSONArray("classPath").getString(0));

    JSONObject application = json.getJSONArray("applications").getJSONObject(0);
    Assert.assertEquals("MyFirstApplication", application.getString("name"));
    Assert.assertEquals("mydtapp-1.0-SNAPSHOT.jar", application.getString("file"));

    JSONObject dag = application.getJSONObject("dag");
    Assert.assertTrue("There is at least one stream", dag.getJSONArray("streams").length() >= 1);
    Assert.assertEquals("There are two operator", 2, dag.getJSONArray("operators").length());
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
      if (app.name.equals("MyFirstApplication")) {
        String[] rp = app.requiredProperties.toArray(new String[]{});
        Assert.assertEquals("dt.test.required.2", rp[0]);
        Assert.assertEquals("dt.test.required.3", rp[1]);
        Assert.assertEquals("app-default-for-required-1", app.defaultProperties.get("dt.test.required.1"));
        return;
      }
    }
    Assert.fail("Should consist of an app called MyFirstApplication");
  }
}
