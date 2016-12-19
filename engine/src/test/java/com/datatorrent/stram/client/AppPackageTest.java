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
package com.datatorrent.stram.client;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.commons.io.IOUtils;

import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.util.JSONSerializationProvider;

import net.lingala.zip4j.exception.ZipException;

/**
 *
 */
public class AppPackageTest
{
  private static AppPackage ap;
  //yet another app package which retains the files
  private static AppPackage yap;
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
      // set up another instance
      File testfolder = new File("target/testapp");
      yap = new AppPackage(file, testfolder, false);
      jomp = new JSONSerializationProvider();
      json = new JSONObject(jomp.getContext(null).writeValueAsString(ap));

    } catch (ZipException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    } finally {
      IOUtils.closeQuietly(ap);
      IOUtils.closeQuietly(yap);
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
    Assert.assertEquals("com.example", json.getString("appPackageGroupId"));
    Assert.assertEquals("mydtapp", json.getString("appPackageName"));
    Assert.assertEquals("1.0-SNAPSHOT", json.getString("appPackageVersion"));
    Assert.assertEquals(System.getProperty("apex.version", "3.4.0"), json.getString("dtEngineVersion"));
    Assert.assertEquals("lib/*.jar", json.getJSONArray("classPath").getString(0));

    JSONObject application1 = json.getJSONArray("applications").getJSONObject(0);
    JSONObject application2 = json.getJSONArray("applications").getJSONObject(1);

    Map<String, JSONObject> apps = new HashMap<>();

    apps.put(application1.getString("name"), application1);
    apps.put(application2.getString("name"), application2);

    Assert.assertEquals(true, apps.containsKey("MyFirstApplication"));
    Assert.assertEquals(true, apps.containsKey("MySecondApplication"));

    Assert.assertEquals("mydtapp-1.0-SNAPSHOT.jar", apps.get("MyFirstApplication").getString("file"));

    JSONObject dag = apps.get("MyFirstApplication").getJSONObject("dag");
    Assert.assertTrue("There is at least one stream", dag.getJSONArray("streams").length() >= 1);
    Assert.assertEquals("There are two operator", 2, dag.getJSONArray("operators").length());

    Assert.assertTrue("app package extraction folder should be retained", new File("target/testapp").exists());
    yap.cleanContent();
    Assert.assertTrue("app package extraction folder should be removed", !new File("target/testapp").exists());
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
  public void testAppLevelRequiredAndDefaultProperties()
  {
    List<AppPackage.AppInfo> applications = ap.getApplications();
    for (AppPackage.AppInfo app : applications) {
      if (app.name.equals("MyFirstApplication")) {
        String[] rp = app.requiredProperties.toArray(new String[]{});
        Assert.assertEquals("dt.test.required.2", rp[0]);
        Assert.assertEquals("dt.test.required.3", rp[1]);
        Assert.assertEquals("app-default-for-required-1", app.defaultProperties.get("dt.test.required.1").getValue());
        Assert.assertEquals("app-default-for-required-1-description", app.defaultProperties.get("dt.test.required.1").getDescription());
        return;
      }
    }
    Assert.fail("Should consist of an app called MyFirstApplication");
  }
}
