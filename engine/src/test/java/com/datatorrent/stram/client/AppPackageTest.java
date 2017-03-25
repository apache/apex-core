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

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.commons.io.IOUtils;

import com.datatorrent.stram.client.AppPackage.PropertyInfo;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.util.JSONSerializationProvider;

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
  public static void starting() throws IOException, JSONException
  {
    File file = StramTestSupport.createAppPackageFile();
    // Set up test instance
    ap = new AppPackage(file, true);
    try {
      // set up another instance
      File testfolder = new File("target/testapp");
      yap = new AppPackage(file, testfolder, false);
      jomp = new JSONSerializationProvider();
      json = new JSONObject(jomp.getContext(null).writeValueAsString(ap));
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

    // Test if there are fixed number of applications
    Assert.assertEquals("Number of applications", 2, json.getJSONArray("applications").length());

    JSONArray applications = json.getJSONArray("applications");
    Map<String, JSONObject> apps = new HashMap<>();

    for (int i = 0; i < applications.length(); i++) {
      JSONObject application = applications.getJSONObject(i);
      apps.put(application.getString("name"), application);
    }

    // Retrieve applications found
    JSONObject myFirstApplication = apps.get("MyFirstApplication");
    JSONObject mySecondApplication = apps.get("MySecondApplication");

    // Testing if expected applications are found
    Assert.assertNotNull("MyFirstApplication not found", myFirstApplication);
    Assert.assertNotNull("MySecondApplication not found", mySecondApplication);

    // Tests related to MyFirstApplication start
    Assert.assertEquals("mydtapp-1.0-SNAPSHOT.jar", myFirstApplication.getString("file"));
    String errorStackTrace = myFirstApplication.getString("errorStackTrace");
    Assert.assertEquals("ErrorStackTrace", "null", errorStackTrace);

    JSONObject dag = myFirstApplication.getJSONObject("dag");
    Assert.assertNotNull("MyFirstApplication DAG does not exist", dag);
    Assert.assertEquals("Number of streams", 1, dag.getJSONArray("streams").length());
    Assert.assertEquals("Number of operators", 3, dag.getJSONArray("operators").length());

    JSONArray operatorsJSONArray = dag.getJSONArray("operators");
    Map<String, JSONObject> operatorMap = new HashMap<>();

    for (int i = 0; i < operatorsJSONArray.length(); i++) {
      JSONObject operator = operatorsJSONArray.getJSONObject(i);
      operatorMap.put(operator.getString("name"), operator);
    }

    JSONObject operator = operatorMap.get("rand");
    Assert.assertNotNull("Input Operator not found", operator);
    Assert.assertEquals("Input operator class", "com.example.mydtapp.RandomNumberGenerator", operator.getJSONObject("properties").get("@class"));

    operator = operatorMap.get("stdout");
    Assert.assertNotNull("Output Operator not found", operator);
    Assert.assertEquals("Value for Output Operator's testInput:", "default-value-1", operator.getJSONObject("properties").get("testInput"));
    Assert.assertEquals("Output Operator class", "com.example.mydtapp.StdoutOperator", operator.getJSONObject("properties").get("@class"));

    operator = operatorMap.get("testModule$testModuleOperator");
    Assert.assertNotNull("Test Module Operator not found", operator);
    Assert.assertEquals("Test Module Operator class", "com.example.mydtapp.TestModuleOperator", operator.getJSONObject("properties").get("@class"));
    Assert.assertEquals("Value for Test Module Operator's testInput:", "default-value-1", operator.getJSONObject("properties").get("testInput"));
    // Tests related to MyFirstApplication end

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

  @Test
  public void testPropertyInfoSerializer() throws JsonGenerationException, JsonMappingException, IOException
  {
    AppPackage.PropertyInfo propertyInfo = new AppPackage.PropertyInfo("test-value", "test-description");
    JSONSerializationProvider jomp = new JSONSerializationProvider();
    jomp.addSerializer(PropertyInfo.class, new AppPackage.PropertyInfoSerializer(false));
    String result = jomp.getContext(null).writeValueAsString(propertyInfo);
    Assert.assertEquals("\"test-value\"", result);

    jomp = new JSONSerializationProvider();
    jomp.addSerializer(PropertyInfo.class, new AppPackage.PropertyInfoSerializer(true));
    result = jomp.getContext(null).writeValueAsString(propertyInfo);
    Assert.assertEquals("{\"value\":\"test-value\",\"description\":\"test-description\"}", result);
  }

  public void testDefaultProperties()
  {

    Map<String, AppPackage.PropertyInfo> defaultProperties = ap.getDefaultProperties();
    Assert.assertEquals(8, defaultProperties.size());
    Assert.assertEquals("package-default", defaultProperties.get("dt.test.1").getValue());
    Assert.assertEquals("package-default", defaultProperties.get("dt.test.2").getValue());
    Assert.assertEquals("package-default", defaultProperties.get("dt.test.3").getValue());
    Assert.assertEquals("package-default", defaultProperties.get("dt.test.4").getValue());
    Assert.assertEquals("package-default", defaultProperties.get("dt.test.5").getValue());
    Assert.assertEquals("package-default", defaultProperties.get("dt.test.6").getValue());
    Assert.assertEquals("default-value-1", defaultProperties.get("dt.operator.testModule.prop.testInput").getValue());
    Assert.assertEquals("default-value-1", defaultProperties.get("dt.operator.stdout.prop.testInput").getValue());
  }
}
