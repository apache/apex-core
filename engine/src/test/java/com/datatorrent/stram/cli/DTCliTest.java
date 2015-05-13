/**
 * Copyright (c) 2014 DataTorrent, Inc. All rights reserved.
 */
package com.datatorrent.stram.cli;

import com.datatorrent.stram.client.AppPackage;
import com.datatorrent.stram.client.ConfigPackage;
import com.datatorrent.stram.client.DTConfiguration;
import java.io.File;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.util.JSONSerializationProvider;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class DTCliTest
{

  // file basename for the created jar
  private static final String appJarPath = "testAppPackage.jar";
  // file basename for the created jar
  private static final String configJarPath = "testConfigPackage.jar";

  // The jar file to use for the AppPackage constructor
  private File appFile;
  private File configFile;


  private AppPackage ap;
  private ConfigPackage cp;
  private JSONSerializationProvider jomp;
  private JSONObject json;
  DTCli cli = new DTCli();

  public class TestMeta extends TestWatcher
  {

    TemporaryFolder testFolder = new TemporaryFolder();
    Map<String, String> env = new HashMap<String, String>();
    String userHome;

    @Override
    protected void starting(Description description)
    {
      try {
        cli.init();
        // Set up jar file to use with constructor
        testFolder.create();
        appFile = StramTestSupport.createAppPackageFile(new File(testFolder.getRoot(), appJarPath));
        configFile = StramTestSupport.createConfigPackageFile(new File(testFolder.getRoot(), configJarPath));

        // Set up test instance
        ap = new AppPackage(appFile, true);
        cp = new ConfigPackage(configFile);
        jomp = new JSONSerializationProvider();
        json = new JSONObject(jomp.getContext(null).writeValueAsString(ap));

        userHome = System.getProperty("user.home");
        env.put("HOME", System.getProperty("user.dir") + "/src/test/resources/testAppPackage");
        setEnv(env);

      } catch (Exception e) {
        throw new RuntimeException(e);
      } 
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);

      try {
        env.put("HOME", userHome);
        setEnv(env);
        FileUtils.forceDelete(appFile);
        FileUtils.forceDelete(configFile);
        testFolder.delete();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  protected static void setEnv(Map<String, String> newenv) throws Exception
  {
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>)theEnvironmentField.get(null);
      env.putAll(newenv);
      Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>)theCaseInsensitiveEnvironmentField.get(null);
      cienv.putAll(newenv);
    } catch (NoSuchFieldException e) {
      Class[] classes = Collections.class.getDeclaredClasses();
      Map<String, String> env = System.getenv();
      for (Class cl : classes) {
        if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
          Field field = cl.getDeclaredField("m");
          field.setAccessible(true);
          Object obj = field.get(env);
          Map<String, String> map = (Map<String, String>)obj;
          map.clear();
          map.putAll(newenv);
        }
      }
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

    props = cli.getLaunchAppPackageProperties(ap, null, commandLineInfo, "PiCalculator");
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
    DTCli.LaunchCommandLineInfo commandLineInfo = DTCli.getLaunchCommandLineInfo(new String[]{"-exactMatch", "-conf", configFile.getAbsolutePath(), appFile.getAbsolutePath(), "PiCalculator"});
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

    Assert.assertEquals(sb.toString(), commandLineInfo.resources);

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

    props = cli.getLaunchAppPackageProperties(ap, cp, commandLineInfo, "PiCalculator");
    Assert.assertEquals("launch-define", props.get("dt.test.1"));
    Assert.assertEquals("config-package-appname", props.get("dt.test.2"));
    Assert.assertEquals("app-package-config", props.get("dt.test.3"));
    Assert.assertEquals("user-home-config", props.get("dt.test.4"));
    Assert.assertEquals("app-default", props.get("dt.test.5"));
    Assert.assertEquals("package-default", props.get("dt.test.6"));
  }
}
