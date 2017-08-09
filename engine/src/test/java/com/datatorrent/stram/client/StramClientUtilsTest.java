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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.datatorrent.stram.security.StramUserLogin;
import com.datatorrent.stram.util.ConfigUtils;

import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Unit tests for StramClientUtils
 */
@PrepareForTest(UserGroupInformation.class)
@RunWith(PowerMockRunner.class)
public class StramClientUtilsTest
{

  @Test
  public void testEvalExpression() throws Exception
  {
    Configuration conf = new Configuration();
    conf.set("a.b.c", "123");
    conf.set("d.e.f", "456");
    conf.set("x.y.z", "foobar");

    Properties prop = new Properties();
    prop.put("product.result", "Product result is {% (_prop[\"a.b.c\"] * _prop[\"d.e.f\"]).toFixed(0) %}...");
    prop.put("concat.result", "Concat result is {% _prop[\"x.y.z\"] %} ... {% _prop[\"a.b.c\"] %} blah");

    StramClientUtils.evalProperties(prop, conf);

    Assert.assertEquals("Product result is " + (123 * 456) + "...", prop.get("product.result"));
    Assert.assertEquals("Concat result is foobar ... 123 blah", prop.get("concat.result"));
    Assert.assertEquals("123", conf.get("a.b.c"));
    Assert.assertEquals("456", conf.get("d.e.f"));
    Assert.assertEquals("foobar", conf.get("x.y.z"));
  }

  @Test
  public void testVariableSubstitution() throws Exception
  {
    Configuration conf = new Configuration();
    conf.set("a.b.c", "123");
    conf.set("x.y.z", "foobar");

    Properties prop = new Properties();
    prop.put("var.result", "1111 ${a.b.c} xxx ${x.y.z} yyy");

    StramClientUtils.evalProperties(prop, conf);

    Assert.assertEquals("1111 123 xxx foobar yyy", prop.get("var.result"));
  }

  @Test
  public void testEvalConfiguration() throws Exception
  {
    Configuration conf = new Configuration();
    conf.set("a.b.c", "123");
    conf.set("x.y.z", "foobar");
    conf.set("sub.result", "1111 ${a.b.c} xxx ${x.y.z} yyy");
    conf.set("script.result", "1111 {% (_prop[\"a.b.c\"] * _prop[\"a.b.c\"]).toFixed(0) %} xxx");

    StramClientUtils.evalConfiguration(conf);

    Assert.assertEquals("1111 123 xxx foobar yyy", conf.get("sub.result"));
    Assert.assertEquals("1111 15129 xxx", conf.get("script.result"));
  }

  private String getHostString(String host) throws UnknownHostException
  {
    InetAddress address = InetAddress.getByName(host);
    if (address.isAnyLocalAddress() || address.isLoopbackAddress()) {
      return address.getCanonicalHostName();
    } else {
      return address.getHostName();
    }
  }

  @Test
  public void testRMWebAddress() throws UnknownHostException
  {
    Configuration conf = new YarnConfiguration(new Configuration(false))
    {
      @Override
      public InetSocketAddress getSocketAddr(String name, String defaultAddress, int defaultPort)
      {
        String rmId = get(ConfigUtils.RM_HA_ID);
        if (rmId != null) {
          name = name + "." + rmId;
        }
        return super.getSocketAddr(name, defaultAddress, defaultPort);
      }
    };

    // basic test
    conf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_KEY, false);
    conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "192.168.1.1:8032");
    conf.set(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS, "192.168.1.2:8032");
    Assert.assertEquals(getHostString("192.168.1.1") + ":8032", StramClientUtils.getSocketConnectString(StramClientUtils.getRMWebAddress(conf, null)));
    List<InetSocketAddress> addresses = StramClientUtils.getRMAddresses(conf);
    Assert.assertEquals(1, addresses.size());
    Assert.assertEquals(getHostString("192.168.1.1") + ":8032", StramClientUtils.getSocketConnectString(addresses.get(0)));

    conf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_KEY, true);
    Assert.assertEquals(getHostString("192.168.1.2") + ":8032", StramClientUtils.getSocketConnectString(StramClientUtils.getRMWebAddress(conf, null)));
    addresses = StramClientUtils.getRMAddresses(conf);
    Assert.assertEquals(1, addresses.size());
    Assert.assertEquals(getHostString("192.168.1.2") + ":8032", StramClientUtils.getSocketConnectString(addresses.get(0)));

    // set localhost if host is unknown
    conf.set(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS, "someunknownhost.:8032");

    Assert.assertEquals(InetAddress.getLocalHost().getCanonicalHostName() + ":8032", StramClientUtils.getSocketConnectString(StramClientUtils.getRMWebAddress(conf, null)));

    // set localhost
    conf.set(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS, "127.0.0.1:8032");
    Assert.assertEquals(InetAddress.getLocalHost().getCanonicalHostName() + ":8032", StramClientUtils.getSocketConnectString(StramClientUtils.getRMWebAddress(conf, null)));

    // test when HA is enabled
    conf.setBoolean(ConfigUtils.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS + ".rm1", "192.168.1.1:8032");
    conf.set(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS + ".rm2", "192.168.1.2:8032");
    conf.set("yarn.resourcemanager.ha.rm-ids", "rm1,rm2");
    Assert.assertEquals(getHostString("192.168.1.1") + ":8032", StramClientUtils.getSocketConnectString(StramClientUtils.getRMWebAddress(conf, "rm1")));
    Assert.assertEquals(getHostString("192.168.1.2") + ":8032", StramClientUtils.getSocketConnectString(StramClientUtils.getRMWebAddress(conf, "rm2")));
    addresses = StramClientUtils.getRMAddresses(conf);
    Assert.assertEquals(2, addresses.size());
    Assert.assertEquals(getHostString("192.168.1.1") + ":8032", StramClientUtils.getSocketConnectString(addresses.get(0)));
    Assert.assertEquals(getHostString("192.168.1.2") + ":8032", StramClientUtils.getSocketConnectString(addresses.get(1)));
  }

  /**
   * With static spying for UserGroupInformation, we need to set up a test login user
   * for FileSystem.newInstance() etc to work.
   *
   * @throws Exception
   */
  private void setupLoginTestUser() throws Exception
  {
    UserGroupInformation testUser = UserGroupInformation.createUserForTesting("testUser1", new String[]{""});
    spy(UserGroupInformation.class);
    doReturn(testUser).when(UserGroupInformation.class, "getLoginUser");
  }

  /**
   * apex.dfsRootDirectory not set: legacy behavior of getDTDFSRootDir()
   * @throws Exception
   *
   */
  @Test
  public void getApexDFSRootDirLegacy() throws Exception
  {
    Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.DT_DFS_ROOT_DIR, "/a/b/c");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, false);

    setupLoginTestUser();

    FileSystem fs = FileSystem.newInstance(conf);
    Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
    Assert.assertEquals("file:/a/b/c", path.toString());
    // verify conf properties for DT_DFS_ROOT_DIR and APEX_APP_DFS_ROOT_DIR
    Assert.assertEquals("/a/b/c", conf.get(StramClientUtils.DT_DFS_ROOT_DIR));
    Assert.assertNull(conf.get(StramClientUtils.APEX_APP_DFS_ROOT_DIR));
  }

  /**
   * apex.dfsRootDirectory set: absolute path e.g. /x/y/z
   * @throws Exception
   *
   */
  @Test
  public void getApexDFSRootDirAbsPath() throws Exception
  {
    Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.APEX_APP_DFS_ROOT_DIR, "/x/y/z");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, false);

    setupLoginTestUser();

    FileSystem fs = FileSystem.newInstance(conf);
    Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
    Assert.assertEquals(fs.getHomeDirectory() + "/datatorrent", path.toString());
    // verify conf properties for DT_DFS_ROOT_DIR and APEX_APP_DFS_ROOT_DIR
    Assert.assertNull(conf.get(StramClientUtils.DT_DFS_ROOT_DIR));
    Assert.assertEquals("/x/y/z", conf.get(StramClientUtils.APEX_APP_DFS_ROOT_DIR));
  }

  /**
   * apex.dfsRootDirectory set: absolute path with scheme e.g. file:/p/q/r
   * @throws Exception
   *
   */
  @Test
  public void getApexDFSRootDirScheme() throws Exception
  {
    Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.APEX_APP_DFS_ROOT_DIR, "file:/p/q/r");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, false);

    setupLoginTestUser();

    FileSystem fs = FileSystem.newInstance(conf);
    Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
    Assert.assertEquals(fs.getHomeDirectory() + "/datatorrent", path.toString());
    // verify conf properties for DT_DFS_ROOT_DIR and APEX_APP_DFS_ROOT_DIR
    Assert.assertNull(conf.get(StramClientUtils.DT_DFS_ROOT_DIR));
    Assert.assertEquals("file:/p/q/r", conf.get(StramClientUtils.APEX_APP_DFS_ROOT_DIR));
  }

  /**
   * apex.dfsRootDirectory set: absolute path with variable %USER_NAME%
   * @throws Exception
   *
   */
  @Test
  public void getApexDFSRootDirWithVar() throws Exception
  {
    final Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.APEX_APP_DFS_ROOT_DIR, "/x/%USER_NAME%/z");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, false);

    setupLoginTestUser();

    final FileSystem fs = FileSystem.newInstance(conf);

    UserGroupInformation doAsUser = UserGroupInformation.createUserForTesting("impersonated", new String[]{""});

    doAsUser.doAs(new PrivilegedExceptionAction<Void>()
    {
      @Override
      public Void run() throws Exception
      {
        Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
        Assert.assertEquals(fs.getHomeDirectory() + "/datatorrent", path.toString());
        return null;
      }
    });
    // verify conf properties for DT_DFS_ROOT_DIR and APEX_APP_DFS_ROOT_DIR
    Assert.assertNull(conf.get(StramClientUtils.DT_DFS_ROOT_DIR));
    Assert.assertEquals("/x/%USER_NAME%/z", conf.get(StramClientUtils.APEX_APP_DFS_ROOT_DIR));
  }

  /**
   * apex.dfsRootDirectory set: absolute path with %USER_NAME% and scheme e.g. file:/x/%USER_NAME%/z
   * @throws Exception
   *
   */
  @Test
  public void getApexDFSRootDirWithSchemeAndVar() throws Exception
  {
    final Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.APEX_APP_DFS_ROOT_DIR, "file:/x/%USER_NAME%/z");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, true);

    setupLoginTestUser();

    final FileSystem fs = FileSystem.newInstance(conf);

    UserGroupInformation doAsUser = UserGroupInformation.createUserForTesting("impersonated", new String[]{""});

    doAsUser.doAs(new PrivilegedExceptionAction<Void>()
    {
      @Override
      public Void run() throws Exception
      {
        Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
        Assert.assertEquals("file:/x/impersonated/z", path.toString());
        return null;
      }
    });
    // verify conf properties for DT_DFS_ROOT_DIR and APEX_APP_DFS_ROOT_DIR
    Assert.assertNull(conf.get(StramClientUtils.DT_DFS_ROOT_DIR));
    Assert.assertEquals("file:/x/%USER_NAME%/z", conf.get(StramClientUtils.APEX_APP_DFS_ROOT_DIR));
  }

  /**
   * apex.dfsRootDirectory set: relative path
   * @throws Exception
   *
   */
  @Test
  public void getApexDFSRootDirRelPath() throws Exception
  {
    final Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.APEX_APP_DFS_ROOT_DIR, "apex");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, false);

    setupLoginTestUser();

    final FileSystem fs = FileSystem.newInstance(conf);

    UserGroupInformation doAsUser = UserGroupInformation.createUserForTesting("impersonated", new String[]{""});

    doAsUser.doAs(new PrivilegedExceptionAction<Void>()
    {
      @Override
      public Void run() throws Exception
      {
        Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
        Assert.assertEquals(fs.getHomeDirectory() + "/datatorrent", path.toString());
        return null;
      }
    });
    // verify conf properties for DT_DFS_ROOT_DIR and APEX_APP_DFS_ROOT_DIR
    Assert.assertNull(conf.get(StramClientUtils.DT_DFS_ROOT_DIR));
    Assert.assertEquals("apex", conf.get(StramClientUtils.APEX_APP_DFS_ROOT_DIR));
  }

  /**
   * apex.dfsRootDirectory set: absolute path with %USER_NAME% and impersonation enabled
   * @throws Exception
   *
   */
  @Test
  public void getApexDFSRootDirAbsPathAndVar() throws Exception
  {
    final Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.APEX_APP_DFS_ROOT_DIR, "/x/%USER_NAME%/z");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, true);

    setupLoginTestUser();

    final FileSystem fs = FileSystem.newInstance(conf);

    UserGroupInformation doAsUser = UserGroupInformation.createUserForTesting("impersonated", new String[]{""});

    doAsUser.doAs(new PrivilegedExceptionAction<Void>()
    {
      @Override
      public Void run() throws Exception
      {
        Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
        Assert.assertEquals("file:/x/impersonated/z", path.toString());
        return null;
      }
    });
    // verify conf properties for DT_DFS_ROOT_DIR and APEX_APP_DFS_ROOT_DIR
    Assert.assertNull(conf.get(StramClientUtils.DT_DFS_ROOT_DIR));
    Assert.assertEquals("/x/%USER_NAME%/z", conf.get(StramClientUtils.APEX_APP_DFS_ROOT_DIR));
  }

  /**
   * apex.dfsRootDirectory set: relative path and impersonation enabled and doAS
   * @throws Exception
   *
   */
  @Test
  public void getApexDFSRootDirRelPathAndImpersonation() throws Exception
  {
    final Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.APEX_APP_DFS_ROOT_DIR, "apex");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, true);

    setupLoginTestUser();

    final FileSystem fs = FileSystem.newInstance(conf);

    UserGroupInformation doAsUser = UserGroupInformation.createUserForTesting("testUser2", new String[]{""});

    doAsUser.doAs(new PrivilegedExceptionAction<Void>()
    {
      @Override
      public Void run() throws Exception
      {
        Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
        Assert.assertEquals("file:/user/testUser2/apex", path.toString());
        return null;
      }
    });
    // verify conf properties for DT_DFS_ROOT_DIR and APEX_APP_DFS_ROOT_DIR
    Assert.assertNull(conf.get(StramClientUtils.DT_DFS_ROOT_DIR));
    Assert.assertEquals("apex", conf.get(StramClientUtils.APEX_APP_DFS_ROOT_DIR));
  }

  /**
   * apex.dfsRootDirectory set: relative path blank and impersonation enabled and doAS
   * @throws Exception
   *
   */
  @Test
  public void getApexDFSRootDirBlankPathAndImpersonation() throws Exception
  {
    final Configuration conf = new Configuration(false);
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, true);

    setupLoginTestUser();

    final FileSystem fs = FileSystem.newInstance(conf);

    UserGroupInformation doAsUser = UserGroupInformation.createUserForTesting("testUser2", new String[]{""});

    doAsUser.doAs(new PrivilegedExceptionAction<Void>()
    {
      @Override
      public Void run() throws Exception
      {
        Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
        Assert.assertEquals("file:/user/testUser2/datatorrent", path.toString());
        return null;
      }
    });
    // verify conf properties for DT_DFS_ROOT_DIR and APEX_APP_DFS_ROOT_DIR
    Assert.assertNull(conf.get(StramClientUtils.DT_DFS_ROOT_DIR));
    Assert.assertNull(conf.get(StramClientUtils.APEX_APP_DFS_ROOT_DIR));
  }

  /**
   * apex.dfsRootDirectory set: relative path having %USER_NAME% and impersonation enabled and doAS
   * Make sure currentUser appears twice
   * @throws Exception
   *
   */
  @Test
  public void getApexDFSRootDirRelPathVarAndImpersonation() throws Exception
  {
    final Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.APEX_APP_DFS_ROOT_DIR, "apex/%USER_NAME%/xyz");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, true);

    setupLoginTestUser();

    final FileSystem fs = FileSystem.newInstance(conf);

    UserGroupInformation doAsUser = UserGroupInformation.createUserForTesting("testUser2", new String[]{""});

    doAsUser.doAs(new PrivilegedExceptionAction<Void>()
    {
      @Override
      public Void run() throws Exception
      {
        Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
        Assert.assertEquals("file:/user/testUser2/apex/testUser2/xyz", path.toString());
        return null;
      }
    });
    // verify conf properties for DT_DFS_ROOT_DIR and APEX_APP_DFS_ROOT_DIR
    Assert.assertNull(conf.get(StramClientUtils.DT_DFS_ROOT_DIR));
    Assert.assertEquals("apex/%USER_NAME%/xyz", conf.get(StramClientUtils.APEX_APP_DFS_ROOT_DIR));
  }

  /**
   * dt.dfsRootDirectory is blank: return homeDir/datatorrent
   * @throws Exception
   *
   */
  @Test
  public void getDTDFSRootDirBlankPath() throws Exception
  {
    final Configuration conf = new Configuration(false);

    setupLoginTestUser();
    FileSystem fs = spy(FileSystem.newInstance(conf));

    Path testPath = new Path("/foo");
    doReturn(testPath).when(fs).getHomeDirectory();

    Path path = StramClientUtils.getDTDFSRootDir(fs, conf);
    Assert.assertEquals("/foo/datatorrent", path.toString());
    // verify conf properties for DT_DFS_ROOT_DIR and APEX_APP_DFS_ROOT_DIR
    Assert.assertNull(conf.get(StramClientUtils.DT_DFS_ROOT_DIR));
    Assert.assertNull(conf.get(StramClientUtils.APEX_APP_DFS_ROOT_DIR));
  }

  /**
   * dt.dfsRootDirectory is non-blank: return full path (URI + value of dt.dfsRootDirectory after replacing %USER_NAME%)
   * @throws Exception
   *
   */
  @Test
  public void getDTDFSRootDirNonBlankPath() throws Exception
  {
    final Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.DT_DFS_ROOT_DIR, "/dt/%USER_NAME%/xyz");
    setupLoginTestUser();
    final FileSystem fs = spy(FileSystem.newInstance(conf));

    URI uri = new URI("hdfs://host1:1020");
    doReturn(uri).when(fs).getUri();

    UserGroupInformation doAsUser = UserGroupInformation.createUserForTesting("testUser2", new String[]{""});

    doAsUser.doAs(new PrivilegedExceptionAction<Void>()
    {
      @Override
      public Void run() throws Exception
      {
        Path path = StramClientUtils.getDTDFSRootDir(fs, conf);
        Assert.assertEquals("hdfs://host1:1020/dt/testUser1/xyz", path.toString());
        return null;
      }
    });
    // verify conf properties for DT_DFS_ROOT_DIR and APEX_APP_DFS_ROOT_DIR
    Assert.assertEquals("/dt/testUser1/xyz", conf.get(StramClientUtils.DT_DFS_ROOT_DIR));
    Assert.assertNull(conf.get(StramClientUtils.APEX_APP_DFS_ROOT_DIR));
  }

  /**
   * dt.dfsRootDirectory is blank: FileSystem is "file:///"
   * @throws Exception
   *
   *
   */
  @Test
  public void newFileSystemInstanceBlankPath() throws Exception
  {
    final Configuration conf = new Configuration(false);

    setupLoginTestUser();
    FileSystem fs = StramClientUtils.newFileSystemInstance(conf);
    Assert.assertEquals("file:///", fs.getUri().toString());
    // verify conf properties for DT_DFS_ROOT_DIR
    Assert.assertNull(conf.get(StramClientUtils.DT_DFS_ROOT_DIR));
  }

  /**
   * dt.dfsRootDirectory is non-blank: use value to get FS
   * @throws Exception
   *
   *
   */
  @Test
  public void newFileSystemInstanceNonBlankPath() throws Exception
  {
    final Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.DT_DFS_ROOT_DIR, "/dt/testUser1/xyz");

    setupLoginTestUser();
    FileSystem fs = StramClientUtils.newFileSystemInstance(conf);
    Assert.assertEquals("file:///", fs.getUri().toString());
    // verify conf properties for DT_DFS_ROOT_DIR
    Assert.assertEquals("/dt/testUser1/xyz", conf.get(StramClientUtils.DT_DFS_ROOT_DIR));
  }

  /**
   * dt.dfsRootDirectory is non-blank and has %USER_NAME%: use value to get FS
   * @throws Exception
   *
   *
   */
  @Test
  public void newFileSystemInstanceNonBlankPathAndVar() throws Exception
  {
    final Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.DT_DFS_ROOT_DIR, "/dt/%USER_NAME%/xyz");

    setupLoginTestUser();

    UserGroupInformation doAsUser = UserGroupInformation.createUserForTesting("testUser2", new String[]{""});

    doAsUser.doAs(new PrivilegedExceptionAction<Void>()
    {
      @Override
      public Void run() throws Exception
      {
        FileSystem fs = StramClientUtils.newFileSystemInstance(conf);
        Assert.assertEquals("file:///", fs.getUri().toString());
        return null;
      }
    });

    // verify conf properties for DT_DFS_ROOT_DIR
    Assert.assertEquals("/dt/testUser1/xyz", conf.get(StramClientUtils.DT_DFS_ROOT_DIR));
  }

}
