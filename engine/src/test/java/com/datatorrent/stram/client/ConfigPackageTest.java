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
import java.util.Map;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;

import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.util.JSONSerializationProvider;

import net.lingala.zip4j.exception.ZipException;

/**
 *
 */
public class ConfigPackageTest
{

  // file basename for the created jar
  private static final String jarPath = "testConfigPackage.jar";

  // The jar file to use for the AppPackage constructor
  private File file;

  private ConfigPackage cp;
  private JSONSerializationProvider jomp;
  private JSONObject json;

  public class TestMeta extends TestWatcher
  {

    TemporaryFolder testFolder = new TemporaryFolder();

    @Override
    protected void starting(Description description)
    {
      try {

        // Set up jar file to use with constructor
        testFolder.create();
        file = StramTestSupport.createConfigPackageFile(new File(testFolder.getRoot(), jarPath));

        // Set up test instance
        cp = new ConfigPackage(file);
        jomp = new JSONSerializationProvider();
        json = new JSONObject(jomp.getContext(null).writeValueAsString(cp));

      } catch (ZipException e) {
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (JSONException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void finished(Description description)
    {
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
  public void testConfigPackage() throws Exception
  {
    Assert.assertEquals("com.example", json.getString("appPackageGroupId"));
    Assert.assertEquals("myapexapp", json.getString("appPackageName"));
    Assert.assertEquals("1.0.0", json.getString("appPackageMinVersion"));
    Assert.assertEquals("1.9999.9999", json.getString("appPackageMaxVersion"));
    Assert.assertEquals("classpath/*", json.getJSONArray("classPath").getString(0));
    Assert.assertEquals("files/*", json.getJSONArray("files").getString(0));
  }

  @Test
  public void testGlobalProperties()
  {
    Map<String, String> properties = cp.getProperties(null);
    Assert.assertEquals("000", properties.get("aaa"));
    Assert.assertEquals("123", properties.get("foo"));
    Assert.assertEquals("456", properties.get("bar"));
  }

  @Test
  public void testAppProperties()
  {
    Map<String, String> properties = cp.getProperties("app1");
    Assert.assertEquals("111", properties.get("aaa"));
    Assert.assertEquals("789", properties.get("foo"));
    Assert.assertEquals("456", properties.get("bar"));
    Assert.assertEquals("888", properties.get("baz"));
  }
}
