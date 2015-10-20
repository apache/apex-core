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
package com.datatorrent.stram.cli;

import java.io.File;
import com.datatorrent.stram.client.AppPackage;
import com.datatorrent.stram.client.ConfigPackage;
import com.datatorrent.stram.client.DTConfiguration;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.TestHomeDirectory;
import static com.datatorrent.stram.support.StramTestSupport.setEnv;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 */
public class DTCliTest
{

  // file basename for the created jar
  private static final String appJarPath = "testAppPackage.jar";
  // file basename for the created jar
  private static final String configJarPath = "testConfigPackage.jar";

  // The jar file to use for the AppPackage constructor
  private static File appFile;
  private static File configFile;


  private static AppPackage ap;
  private static ConfigPackage cp;
  static TemporaryFolder testFolder = new TemporaryFolder();
  static DTCli cli = new DTCli();

  static Map<String, String> env = new HashMap<String, String>();
  static String userHome;

  @BeforeClass
  public static void starting()
  {
    try {
      userHome = System.getProperty("user.home");
      String newHome = System.getProperty("user.dir") + "/target";
      FileUtils.forceMkdir(new File(newHome + "/.dt"));
      FileUtils.copyFile(new File(System.getProperty("user.dir") + "/src/test/resources/testAppPackage/dt-site.xml"), new File(newHome + "/.dt/dt-site.xml"));
      env.put("HOME", newHome);
      setEnv(env);

      cli.init();
      // Set up jar file to use with constructor
      testFolder.create();
      appFile = StramTestSupport.createAppPackageFile();
      configFile = StramTestSupport.createConfigPackageFile(new File(testFolder.getRoot(), configJarPath));
      ap = new AppPackage(appFile, true);
      cp = new ConfigPackage(configFile);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void finished()
  {
    try {
      env.put("HOME", userHome);
      setEnv(env);

      StramTestSupport.removeAppPackageFile();
      FileUtils.forceDelete(configFile);
      testFolder.delete();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testLaunchAppPackagePropertyPrecedence() throws Exception
  {
    // set launch command options
    DTCli.LaunchCommandLineInfo commandLineInfo = DTCli.getLaunchCommandLineInfo(new String[]{"-D", "dt.test.1=launch-define", "-apconf", "my-app-conf1.xml", "-conf", "src/test/resources/testAppPackage/local-conf.xml"});
    // process and look at launch config

    DTConfiguration props = cli.getLaunchAppPackageProperties(ap, null, commandLineInfo, null);
    Assert.assertEquals("launch-define", props.get("dt.test.1"));
    Assert.assertEquals("local-fs-config", props.get("dt.test.2"));
    Assert.assertEquals("app-package-config", props.get("dt.test.3"));
    Assert.assertEquals("user-home-config", props.get("dt.test.4"));
    Assert.assertEquals("package-default", props.get("dt.test.5"));

    props = cli.getLaunchAppPackageProperties(ap, null, commandLineInfo, "MyFirstApplication");
    Assert.assertEquals("launch-define", props.get("dt.test.1"));
    Assert.assertEquals("local-fs-config", props.get("dt.test.2"));
    Assert.assertEquals("app-package-config", props.get("dt.test.3"));
    Assert.assertEquals("user-home-config", props.get("dt.test.4"));
    Assert.assertEquals("app-default", props.get("dt.test.5"));
    Assert.assertEquals("package-default", props.get("dt.test.6"));
  }

  @Test
  public void testLaunchAppPackageParametersWithConfigPackage() throws Exception
  {
    DTCli.LaunchCommandLineInfo commandLineInfo = DTCli.getLaunchCommandLineInfo(new String[]{"-exactMatch", "-conf", configFile.getAbsolutePath(), appFile.getAbsolutePath(), "MyFirstApplication"});
    String[] args = cli.getLaunchAppPackageArgs(ap, cp, commandLineInfo, null);
    commandLineInfo = DTCli.getLaunchCommandLineInfo(args);
    StringBuilder sb = new StringBuilder();
    for (String f : ap.getClassPath()) {
      if (sb.length() != 0) {
        sb.append(",");
      }
      sb.append(ap.tempDirectory()).append(File.separatorChar).append(f);
    }
    for (String f : cp.getClassPath()) {
      if (sb.length() != 0) {
        sb.append(",");
      }
      sb.append(cp.tempDirectory()).append(File.separatorChar).append(f);
    }

    Assert.assertEquals(sb.toString(), commandLineInfo.libjars);

    sb.setLength(0);
    for (String f : cp.getFiles()) {
      if (sb.length() != 0) {
        sb.append(",");
      }
      sb.append(cp.tempDirectory()).append(File.separatorChar).append(f);
    }

    Assert.assertEquals(sb.toString(), commandLineInfo.files);
  }

  @Test
  public void testLaunchAppPackagePrecedenceWithConfigPackage() throws Exception
  {
    // set launch command options
    DTCli.LaunchCommandLineInfo commandLineInfo = DTCli.getLaunchCommandLineInfo(new String[]{"-D", "dt.test.1=launch-define", "-apconf", "my-app-conf1.xml", "-conf", configFile.getAbsolutePath()});
    // process and look at launch config

    DTConfiguration props = cli.getLaunchAppPackageProperties(ap, cp, commandLineInfo, null);
    Assert.assertEquals("launch-define", props.get("dt.test.1"));
    Assert.assertEquals("config-package", props.get("dt.test.2"));
    Assert.assertEquals("app-package-config", props.get("dt.test.3"));
    Assert.assertEquals("user-home-config", props.get("dt.test.4"));
    Assert.assertEquals("package-default", props.get("dt.test.5"));

    props = cli.getLaunchAppPackageProperties(ap, cp, commandLineInfo, "MyFirstApplication");
    Assert.assertEquals("launch-define", props.get("dt.test.1"));
    Assert.assertEquals("config-package-appname", props.get("dt.test.2"));
    Assert.assertEquals("app-package-config", props.get("dt.test.3"));
    Assert.assertEquals("user-home-config", props.get("dt.test.4"));
    Assert.assertEquals("app-default", props.get("dt.test.5"));
    Assert.assertEquals("package-default", props.get("dt.test.6"));
  }
}
