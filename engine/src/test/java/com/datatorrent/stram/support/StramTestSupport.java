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
package com.datatorrent.stram.support;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.codehaus.plexus.DefaultPlexusContainer;
import org.codehaus.plexus.PlexusContainer;
import org.codehaus.plexus.logging.BaseLoggerManager;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketServlet;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.maven.cli.MavenCli;
import org.apache.maven.cli.logging.Slf4jLogger;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.StramLocalCluster.LocalStreamingContainer;
import com.datatorrent.stram.api.AppDataSource;
import com.datatorrent.stram.api.BaseContext;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.tuple.EndWindowTuple;
import com.datatorrent.stram.tuple.Tuple;
import com.datatorrent.stram.webapp.AppInfo;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;

/**
 * Bunch of utilities shared between tests.
 */
public abstract class StramTestSupport
{
  private static final Logger LOG = LoggerFactory.getLogger(StramTestSupport.class);
  private static MavenCli mavenCli = new MavenCli()
  {
    @Override
    protected void customizeContainer(PlexusContainer container)
    {
      ((DefaultPlexusContainer)container).setLoggerManager(
          new BaseLoggerManager()
          {
            @Override
            protected org.codehaus.plexus.logging.Logger createLogger(String s)
            {
              return new Slf4jLogger(LOG);
            }
          }
      );
    }
  };
  private static final String workingDirectory = "src/test/resources/testAppPackage/mydtapp/";
  public static final long DEFAULT_TIMEOUT_MILLIS = 30000;

  public static Object generateTuple(Object payload, int windowId)
  {
    return payload;
  }

  public static Tuple generateBeginWindowTuple(String nodeid, int windowId)
  {
    Tuple bwt = new Tuple(MessageType.BEGIN_WINDOW, windowId);
    return bwt;
  }

  public static Tuple generateEndWindowTuple(String nodeid, int windowId)
  {
    EndWindowTuple t = new EndWindowTuple(windowId);
    return t;
  }

  public static void checkStringMatch(String print, String expected, String got)
  {
    Assert.assertTrue(print + " doesn't match, got: " + got + " expected: " + expected,
        got.matches(expected));
  }

  public static WindowGenerator setupWindowGenerator(ManualScheduledExecutorService mses)
  {
    WindowGenerator gen = new WindowGenerator(mses, 1024);
    gen.setResetWindow(0);
    gen.setFirstWindow(0);
    gen.setWindowWidth(1);
    return gen;
  }

  @SuppressWarnings("SleepWhileInLoop")
  public static void waitForWindowComplete(OperatorContext nodeCtx, long windowId) throws InterruptedException
  {
    LOG.debug("Waiting for end of window {} at node {} when lastProcessedWindowId is {}", new Object[] {windowId, nodeCtx.getId(), nodeCtx.getLastProcessedWindowId()});
    long startMillis = System.currentTimeMillis();
    while (nodeCtx.getLastProcessedWindowId() < windowId) {
      if (System.currentTimeMillis() > (startMillis + DEFAULT_TIMEOUT_MILLIS)) {
        long timeout = System.currentTimeMillis() - startMillis;
        throw new AssertionError(String.format("Timeout %s ms waiting for window %s operator %s", timeout, windowId, nodeCtx.getId()));
      }
      Thread.sleep(20);
    }
  }

  /**
   * Create an appPackage zip using the sample appPackage located in
   * src/test/resources/testAppPackage/testAppPackageSrc.
   * @return      The File object that can be used in the AppPackage constructor.
   */
  public static File createAppPackageFile()
  {
    final String version = System.getProperty("apex.version");
    final List<String> params = new LinkedList<>();
    params.add("clean");
    params.add("package");
    params.add("-DskipTests");
    if (version != null && version.length() > 0) {
      params.add("-Dapex.version=" + version);
    }
    Assert.assertEquals(0, mavenCli.doMain(params.toArray(new String[params.size()]), workingDirectory, System.out, System.err));
    return new File(workingDirectory, "target/mydtapp-1.0-SNAPSHOT.apa");
  }

  public static void removeAppPackageFile()
  {
    Assert.assertEquals(0, mavenCli.doMain(new String[]{"clean"}, workingDirectory, System.out, System.err));
  }

  /**
   * Create an confPackage zip using the sample confPackage located in
   * src/test/resources/testConfPackage/testConfPackageSrc.
   *
   * @param file The file whose path will be used to create the confPackage zip
   * @return The File object that can be used in the ConfigPackage constructor.
   * @throws net.lingala.zip4j.exception.ZipException
   */
  public static File createConfigPackageFile(File file) throws net.lingala.zip4j.exception.ZipException
  {
    ZipFile zipFile = new ZipFile(file);
    ZipParameters zipParameters = new ZipParameters();
    zipParameters.setIncludeRootFolder(false);
    zipFile.createZipFileFromFolder("src/test/resources/testConfigPackage/testConfigPackageSrc", zipParameters, false, Long.MAX_VALUE);
    return file;
  }

  public interface WaitCondition
  {
    boolean isComplete();

  }

  @SuppressWarnings("SleepWhileInLoop")
  public static boolean awaitCompletion(WaitCondition c, long timeoutMillis) throws InterruptedException
  {
    long startMillis = System.currentTimeMillis();
    while (System.currentTimeMillis() < (startMillis + timeoutMillis)) {
      if (c.isComplete()) {
        return true;
      }
      Thread.sleep(50);
    }
    return c.isComplete();
  }

  /**
   * Wait until instance of operator is deployed into a container and return the container reference.
   * Asserts non null return value.
   *
   * @param localCluster
   * @param operator
   * @return
   * @throws InterruptedException
   */
  @SuppressWarnings("SleepWhileInLoop")
  public static LocalStreamingContainer waitForActivation(StramLocalCluster localCluster, PTOperator operator) throws InterruptedException
  {
    LocalStreamingContainer container;
    long startMillis = System.currentTimeMillis();
    while (System.currentTimeMillis() < (startMillis + DEFAULT_TIMEOUT_MILLIS)) {
      if (operator.getState() == PTOperator.State.ACTIVE) {
        if ((container = localCluster.getContainer(operator)) != null) {
          return container;
        }
      }
      LOG.debug("Waiting for {}({}) in container {}", new Object[] {operator, operator.getState(), operator.getContainer()});
      Thread.sleep(500);
    }

    Assert.fail("timeout waiting for operator deployment " + operator);
    return null;
  }

  public static class RegexMatcher extends BaseMatcher<String>
  {
    private final String regex;

    public RegexMatcher(String regex)
    {
      this.regex = regex;
    }

    @Override
    public boolean matches(Object o)
    {
      return ((String)o).matches(regex);

    }

    @Override
    public void describeTo(Description description)
    {
      description.appendText("matches regex=" + regex);
    }

    public static RegexMatcher matches(String regex)
    {
      return new RegexMatcher(regex);
    }

  }

  public static class TestMeta extends TestWatcher
  {
    private File dir;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      final String methodName = description.getMethodName();
      final String className = description.getClassName();
      dir = new File("target/" + className + "/" + methodName);
      try {
        Files.createDirectories(dir.toPath());
      } catch (FileAlreadyExistsException e) {
        try {
          Files.delete(dir.toPath());
          Files.createDirectories(dir.toPath());
        } catch (IOException ioe) {
          throw new RuntimeException("Fail to create test working directory " + dir.getAbsolutePath(), e);
        }
      } catch (IOException e) {
        throw new RuntimeException("Fail to create test working directory " + dir.getAbsolutePath(), e);
      }
    }

    @Override
    protected void finished(org.junit.runner.Description description)
    {
      FileUtils.deleteQuietly(dir);
    }

    public String getPath()
    {
      return dir.getPath();
    }

    public String getAbsolutePath()
    {
      return dir.getAbsolutePath();
    }

    public Path toPath()
    {
      return dir.toPath();
    }

    public URI toURI()
    {
      return dir.toURI();
    }

  }

  public static class TestHomeDirectory extends TestWatcher
  {

    Map<String, String> env = new HashMap<>();
    String userHome;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      super.starting(description);
      try {
        userHome = System.getProperty("user.home");
        env.put("HOME", System.getProperty("user.dir") + "/src/test/resources/testAppPackage");
        setEnv(env);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void finished(org.junit.runner.Description description)
    {
      super.finished(description);

      try {
        env.put("HOME", userHome);
        setEnv(env);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static void setEnv(Map<String, String> newenv) throws Exception
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

  public static LogicalPlan createDAG(final TestMeta testMeta, final String suffix)
  {
    if (suffix == null) {
      throw new NullPointerException();
    }
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, testMeta.getPath() + suffix);
    return dag;
  }

  public static LogicalPlan createDAG(final TestMeta testMeta)
  {
    return createDAG(testMeta, "");
  }

  public static boolean isInTravis()
  {
    return "true".equals(System.getProperty("travis"));
  }

  public static class MemoryStorageAgent implements StorageAgent, Serializable
  {
    static class OperatorWindowIdPair implements Serializable
    {
      final int operatorId;
      final long windowId;

      OperatorWindowIdPair(int operatorId, long windowId)
      {
        this.operatorId = operatorId;
        this.windowId = windowId;
      }

      @Override
      public int hashCode()
      {
        int hash = 7;
        hash = 97 * hash + this.operatorId;
        hash = 97 * hash + (int)(this.windowId ^ (this.windowId >>> 32));
        return hash;
      }

      @Override
      public boolean equals(Object obj)
      {
        if (obj == null) {
          return false;
        }
        if (getClass() != obj.getClass()) {
          return false;
        }
        final OperatorWindowIdPair other = (OperatorWindowIdPair)obj;
        if (this.operatorId != other.operatorId) {
          return false;
        }
        if (this.windowId != other.windowId) {
          return false;
        }
        return true;
      }

      private static final long serialVersionUID = 201404091805L;
    }

    transient HashMap<OperatorWindowIdPair, Object> store = new HashMap<>();

    @Override
    public synchronized void save(Object object, int operatorId, long windowId) throws IOException
    {
      store.put(new OperatorWindowIdPair(operatorId, windowId), object);
    }

    @Override
    public synchronized Object load(int operatorId, long windowId) throws IOException
    {
      return store.get(new OperatorWindowIdPair(operatorId, windowId));
    }

    @Override
    public synchronized void delete(int operatorId, long windowId) throws IOException
    {
      store.remove(new OperatorWindowIdPair(operatorId, windowId));
    }

    @Override
    public synchronized long[] getWindowIds(int operatorId) throws IOException
    {
      ArrayList<Long> windowIds = new ArrayList<>();
      for (OperatorWindowIdPair key : store.keySet()) {
        if (key.operatorId == operatorId) {
          windowIds.add(key.windowId);
        }
      }

      long[] ret = new long[windowIds.size()];
      for (int i = ret.length; i-- > 0;) {
        ret[i] = windowIds.get(i).longValue();
      }

      return ret;
    }

    private static final long serialVersionUID = 201404091747L;
  }

  public static class TestAppContext extends BaseContext implements StramAppContext
  {

    final ApplicationAttemptId appAttemptID;
    final ApplicationId appID;
    final String appPath = "/testPath";
    final String userId = "testUser";
    final long startTime = System.currentTimeMillis();
    final String gatewayAddress = "localhost:9090";

    public TestAppContext(Attribute.AttributeMap attributeMap, int appid, int numJobs, int numTasks, int numAttempts)
    {
      super(attributeMap, null); // this needs to be done in a proper way - may cause application errors.
      this.appID = ApplicationId.newInstance(0, appid);
      this.appAttemptID = ApplicationAttemptId.newInstance(this.appID, numAttempts);
    }

    public TestAppContext(Attribute.AttributeMap attributeMap)
    {
      this(attributeMap, 0, 1, 1, 1);
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId()
    {
      return appAttemptID;
    }

    @Override
    public ApplicationId getApplicationID()
    {
      return appID;
    }

    @Override
    public String getApplicationPath()
    {
      return appPath;
    }

    @Override
    public String getAppMasterTrackingUrl()
    {
      return "unknown";
    }

    @Override
    public CharSequence getUser()
    {
      return userId;
    }

    @Override
    public Clock getClock()
    {
      return null;
    }

    @Override
    public String getApplicationName()
    {
      return "TestApp";
    }

    @Override
    public String getApplicationDocLink()
    {
      return "TestAppDocLink";
    }

    @Override
    public long getStartTime()
    {
      return startTime;
    }

    @Override
    public AppInfo.AppStats getStats()
    {
      return new AppInfo.AppStats()
      {
      };
    }

    @Override
    public String getGatewayAddress()
    {
      return gatewayAddress;
    }

    @Override
    public boolean isGatewayConnected()
    {
      return false;
    }

    @Override
    public List<AppDataSource> getAppDataSources()
    {
      return null;
    }

    @Override
    public Map<String, Object> getMetrics()
    {
      return null;
    }

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 201309121323L;
  }

  public static class EmbeddedWebSocketServer
  {

    private final Logger LOG = LoggerFactory.getLogger(EmbeddedWebSocketServer.class);

    private int port;
    private Server server;
    private WebSocket websocket;

    public EmbeddedWebSocketServer(int port)
    {
      this.port = port;
    }

    public void setWebSocket(WebSocket websocket)
    {
      this.websocket = websocket;
    }

    public void start() throws Exception
    {
      server = new Server();
      Connector connector = new SelectChannelConnector();
      connector.setPort(port);
      server.addConnector(connector);

        // Setup the basic application "context" for this application at "/"
      // This is also known as the handler tree (in jetty speak)
      ServletContextHandler contextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
      contextHandler.setContextPath("/");
      server.setHandler(contextHandler);
      WebSocketServlet webSocketServlet = new WebSocketServlet()
      {
        @Override
        public WebSocket doWebSocketConnect(HttpServletRequest request, String protocol)
        {
          return websocket;
        }
      };

      contextHandler.addServlet(new ServletHolder(webSocketServlet), "/pubsub");
      server.start();
      if (port == 0) {
        port = server.getConnectors()[0].getLocalPort();
      }
    }

    public int getPort()
    {
      return port;
    }

    public void stop() throws Exception
    {
      server.stop();
    }
  }

}
