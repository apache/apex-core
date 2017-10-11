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
import java.net.URL;
import java.util.LinkedHashSet;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.rule.PowerMockRule;
import org.powermock.reflect.Whitebox;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.security.StramUserLogin;

import static org.powermock.api.mockito.PowerMockito.method;
import static org.powermock.api.mockito.PowerMockito.suppress;

/**
 * StramAppLauncher Test
 */
@RunWith(Enclosed.class)
public class StramAppLauncherTest
{

  private static final String SET_TOKEN_REFRESH_CREDENTIALS_METHOD = "setTokenRefreshCredentials";

  @PrepareForTest({StramAppLauncher.class})
  @PowerMockIgnore({"javax.xml.*", "org.w3c.*", "org.apache.hadoop.*", "org.apache.log4j.*"})
  public static class LoadDependenciesTest
  {

    @Rule
    public PowerMockRule rule = new PowerMockRule();

    @Rule
    public TestWatcher setup = new TestWatcher()
    {
      @Override
      protected void starting(Description description)
      {
        super.starting(description);
        suppress(method(StramAppLauncher.class, "init"));
      }
    };

    @Test
    public void testLoadDependenciesSetsParentClassLoader() throws Exception
    {
      // Setup
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.newInstance(conf);
      StramAppLauncher appLauncher = new StramAppLauncher(fs, conf);

      Whitebox.setInternalState(appLauncher, "launchDependencies", new LinkedHashSet<URL>());

      // Get initial contextClassLoader
      ClassLoader initialClassLoader = Thread.currentThread().getContextClassLoader();

      appLauncher.loadDependencies();

      // Make sure that new contextClassLoader has initialClassLoader as parent
      ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();

      Assert.assertSame(initialClassLoader, currentClassLoader.getParent());
    }

    @Test
    public void testResetContextClassLoaderResetsToInitialClassLoader() throws Exception
    {
      // Setup
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.newInstance(conf);
      StramAppLauncher appLauncher = new StramAppLauncher(fs, conf);

      Whitebox.setInternalState(appLauncher, "launchDependencies", new LinkedHashSet<URL>());

      // Get initial contextClassLoader
      ClassLoader initialClassLoader = Thread.currentThread().getContextClassLoader();

      appLauncher.loadDependencies();
      appLauncher.resetContextClassLoader();

      ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
      Assert.assertSame(initialClassLoader, currentClassLoader);
    }

    @Test
    public void testResetContextClassloaderOnlyOnInitialThread() throws Exception
    {
      // Setup
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.newInstance(conf);
      final StramAppLauncher appLauncher = new StramAppLauncher(fs, conf);
      Whitebox.setInternalState(appLauncher, "launchDependencies", new LinkedHashSet<URL>());

      new AsyncTester(new Runnable()
      {
        @Override
        public void run()
        {
          try {
            appLauncher.loadDependencies();
          } catch (Exception e) {
            Assert.fail(e.getMessage());
          }
        }
      }).start().test();

      new AsyncTester(new Runnable()
      {
        @Override
        public void run()
        {
          try {
            appLauncher.resetContextClassLoader();
            Assert.fail("An exception should be thrown");
          } catch (RuntimeException e) {
            // catch as expected
          }
        }
      }).start().test();
    }

    @Test
    public void testLoadDependenciesOnlyOnInitialThread() throws Exception
    {
      // Setup
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.newInstance(conf);
      final StramAppLauncher appLauncher = new StramAppLauncher(fs, conf);
      Whitebox.setInternalState(appLauncher, "launchDependencies", new LinkedHashSet<URL>());

      new AsyncTester(new Runnable()
      {
        @Override
        public void run()
        {
          try {
            appLauncher.loadDependencies();
          } catch (Exception e) {
            Assert.fail(e.getMessage());
          }
        }
      }).start().test();

      new AsyncTester(new Runnable()
      {
        @Override
        public void run()
        {
          try {
            appLauncher.loadDependencies();
            Assert.fail("An exception should be thrown");
          } catch (RuntimeException e) {
            // catch as expected
          }
        }
      }).start().test();
    }
  }

  @PrepareForTest({StramAppLauncher.class})
  @PowerMockIgnore({"javax.xml.*", "org.w3c.*", "org.apache.hadoop.*", "org.apache.log4j.*"})
  public static class RefreshTokenTests
  {
    File workspace;
    File sourceKeytab;
    File dfsDir;
    File apexDfsDir;

    static final String principal = "username/group@domain";

    @Rule
    public PowerMockRule rule = new PowerMockRule();

    @Rule
    public TestWatcher setup = new TestWatcher()
    {
      @Override
      protected void starting(Description description)
      {
        super.starting(description);
        workspace = new File("target/" + description.getClassName() + "/" + description.getMethodName());
        try {
          FileUtils.forceMkdir(workspace);
          sourceKeytab = new File(workspace, "src/keytab");
          FileUtils.touch(sourceKeytab);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        dfsDir = new File(workspace, "dst");
        apexDfsDir = new File(workspace, "adst");
        suppress(method(StramAppLauncher.class, "init"));
      }

      @Override
      protected void finished(Description description)
      {
        FileUtils.deleteQuietly(workspace);
        super.finished(description);
      }
    };

    @Test
    public void testGetTokenRefreshKeytab() throws Exception
    {
      Configuration conf = new Configuration(false);
      File storeKeytab = new File(dfsDir, "keytab2");
      conf.set(StramClientUtils.TOKEN_REFRESH_KEYTAB, storeKeytab.getPath());
      StramUserLogin.authenticate(principal, sourceKeytab.getPath());
      LogicalPlan dag = applyTokenRefreshKeytab(FileSystem.newInstance(conf), conf);
      Assert.assertEquals("Token refresh principal", principal, dag.getValue(LogicalPlan.PRINCIPAL));
      Assert.assertEquals("Token refresh keytab path", storeKeytab.getPath(), dag.getValue(LogicalPlan.KEY_TAB_FILE));
    }

    @Test
    public void testUserLoginTokenRefreshKeytab() throws Exception
    {
      Configuration conf = new Configuration(false);
      /*
      spy(StramUserLogin.class);
      when(StramUserLogin.getPrincipal()).thenReturn(principal);
      when(StramUserLogin.getKeytab()).thenReturn(sourceKeytab.getPath());
      */
      StramUserLogin.authenticate(principal, sourceKeytab.getPath());
      testDFSTokenPath(conf);
    }

    @Test
    public void testAuthPropTokenRefreshKeytab() throws Exception
    {
      Configuration conf = new Configuration(false);
      conf.set(StramUserLogin.DT_AUTH_PRINCIPAL, principal);
      conf.set(StramUserLogin.DT_AUTH_KEYTAB, sourceKeytab.getPath());
      StramUserLogin.authenticate(conf);
      testDFSTokenPath(conf);
    }

    private void testDFSTokenPath(Configuration conf) throws Exception
    {
      FileSystem fs = FileSystem.newInstance(conf);
      conf.set(StramClientUtils.DT_DFS_ROOT_DIR, dfsDir.getAbsolutePath());
      LogicalPlan dag = applyTokenRefreshKeytab(fs, conf);
      Assert.assertEquals("Token refresh principal", principal, dag.getValue(LogicalPlan.PRINCIPAL));
      Assert.assertEquals("Token refresh keytab path", new Path(fs.getUri().getScheme(), fs.getUri().getAuthority(),
          new File(dfsDir, sourceKeytab.getName()).getAbsolutePath()).toString(), dag.getValue(LogicalPlan.KEY_TAB_FILE));
    }

    @Test
    public void testUserLoginTokenRefreshKeytabWithApexDFS() throws Exception
    {
      Configuration conf = new Configuration(false);
      /*
      spy(StramUserLogin.class);
      when(StramUserLogin.getPrincipal()).thenReturn(principal);
      when(StramUserLogin.getKeytab()).thenReturn(sourceKeytab.getPath());
      */
      StramUserLogin.authenticate(principal, sourceKeytab.getPath());
      testDFSTokenPathWithApexDFS(conf);
    }

    @Test
    public void testAuthPropTokenRefreshKeytabWithApexDFS() throws Exception
    {
      Configuration conf = new Configuration(false);
      conf.set(StramUserLogin.DT_AUTH_PRINCIPAL, principal);
      conf.set(StramUserLogin.DT_AUTH_KEYTAB, sourceKeytab.getPath());
      StramUserLogin.authenticate(conf);
      testDFSTokenPathWithApexDFS(conf);
    }

    private void testDFSTokenPathWithApexDFS(Configuration conf) throws Exception
    {
      FileSystem fs = FileSystem.newInstance(conf);
      conf.set(StramClientUtils.DT_DFS_ROOT_DIR, dfsDir.getAbsolutePath());
      conf.set(StramClientUtils.APEX_APP_DFS_ROOT_DIR, apexDfsDir.getAbsolutePath());
      conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, true); // needs to be true for APEX_APP_DFS_ROOT_DIR to be honored
      LogicalPlan dag = applyTokenRefreshKeytab(fs, conf);
      Assert.assertEquals("Token refresh principal", principal, dag.getValue(LogicalPlan.PRINCIPAL));
      Assert.assertEquals("Token refresh keytab path", new Path(fs.getUri().getScheme(), fs.getUri().getAuthority(),
          new File(apexDfsDir, sourceKeytab.getName()).getAbsolutePath()).toString(), dag.getValue(LogicalPlan.KEY_TAB_FILE));
    }

    private LogicalPlan applyTokenRefreshKeytab(FileSystem fs, Configuration conf) throws Exception
    {
      LogicalPlan dag = new LogicalPlan();
      StramAppLauncher appLauncher = new StramAppLauncher(fs, conf);
      Whitebox.invokeMethod(appLauncher, SET_TOKEN_REFRESH_CREDENTIALS_METHOD, dag, conf);
      return dag;
    }
  }

}
