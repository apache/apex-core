/**
 * Copyright (c) 2014 DataTorrent, Inc. All rights reserved.
 */
package com.datatorrent.stram.cli;

import java.io.File;
import com.datatorrent.stram.client.AppPackage;
import com.datatorrent.stram.client.ConfigPackage;
import com.datatorrent.stram.client.DTConfiguration;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.TestHomeDirectory;
import com.datatorrent.stram.util.JSONSerializationProvider;
import org.apache.commons.io.FileUtils;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
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

  public class TestMeta extends TestHomeDirectory
  {

    TemporaryFolder testFolder = new TemporaryFolder();

    @Override
    protected void starting(Description description)
    {
      super.starting(description);
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

      } catch (Exception e) {
        throw new RuntimeException(e);
      } 
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);

      try {
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

    props = cli.getLaunchAppPackageProperties(ap, cp, commandLineInfo, "PiCalculator");
    Assert.assertEquals("launch-define", props.get("dt.test.1"));
    Assert.assertEquals("config-package-appname", props.get("dt.test.2"));
    Assert.assertEquals("app-package-config", props.get("dt.test.3"));
    Assert.assertEquals("user-home-config", props.get("dt.test.4"));
    Assert.assertEquals("app-default", props.get("dt.test.5"));
    Assert.assertEquals("package-default", props.get("dt.test.6"));
  }
}
