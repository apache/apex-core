/**
 * Copyright (c) 2014 DataTorrent, Inc. All rights reserved.
 */
package com.datatorrent.stram.cli;

import com.datatorrent.stram.client.AppPackage;
import com.datatorrent.stram.client.DTConfiguration;
import java.io.File;
import com.datatorrent.stram.support.StramTestSupport;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class DTCliTest
{

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
    Map<String, String> env = new HashMap<String, String>();
    String userHome = System.getProperty("user.home");
    env.put("HOME", System.getProperty("user.dir") + "/src/test/resources/testAppPackage");
    setEnv(env);
    File appPackageFile = StramTestSupport.createAppPackageFile(new File("src/test/resources/testAppPackage/testAppPackage.jar"));
    try {
      DTCli cli = new DTCli();
      cli.init();
      // set launch command options
      DTCli.LaunchCommandLineInfo commandLineInfo = DTCli.getLaunchCommandLineInfo(new String[]{"-D", "dt.test.1=launch-define", "-apconf", "my-app-conf1.xml", "-conf", "src/test/resources/testAppPackage/local-conf.xml"});
      // process and look at launch config

      AppPackage ap = new AppPackage(appPackageFile, true);
      DTConfiguration props = cli.getLaunchAppPackageProperties(ap, commandLineInfo, null);
      Assert.assertEquals("launch-define", props.get("dt.test.1"));
      Assert.assertEquals("local-fs-config", props.get("dt.test.2"));
      Assert.assertEquals("app-package-config", props.get("dt.test.3"));
      Assert.assertEquals("user-home-config", props.get("dt.test.4"));
      Assert.assertEquals("package-default", props.get("dt.test.5"));

      props = cli.getLaunchAppPackageProperties(ap, commandLineInfo, "PiCalculator");
      Assert.assertEquals("launch-define", props.get("dt.test.1"));
      Assert.assertEquals("local-fs-config", props.get("dt.test.2"));
      Assert.assertEquals("app-package-config", props.get("dt.test.3"));
      Assert.assertEquals("user-home-config", props.get("dt.test.4"));
      Assert.assertEquals("app-default", props.get("dt.test.5"));
      Assert.assertEquals("package-default", props.get("dt.test.6"));

    } finally {
      env.put("HOME", userHome);
      setEnv(env);
      FileUtils.forceDelete(appPackageFile);
    }
  }
}
