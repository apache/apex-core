/**
 * Copyright (c) 2012-2013 DataTorrent, Inc. All rights reserved.
 */
package com.datatorrent.stram;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import static java.lang.Thread.sleep;
import java.net.URL;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import junit.framework.Assert;

import javax.ws.rs.core.MediaType;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.util.Records;
import org.codehaus.jettison.json.JSONObject;
import org.junit.*;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.StramAppMaster;
import com.datatorrent.stram.StramChild;
import com.datatorrent.stram.StramClient;
import com.datatorrent.stram.client.StramClientUtils.YarnClientHelper;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import com.datatorrent.stram.webapp.StramWebServices;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.ShipContainingJars;
import com.google.common.collect.Lists;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * The purpose of this test is to verify basic streaming application deployment
 * on a distributed yarn cluster. Specifically this exercises the application master,
 * which is not used in other tests that rely on local mode.
 */
public class StramMiniClusterTest
{
  private static final Logger LOG = LoggerFactory.getLogger(StramMiniClusterTest.class);
  protected static MiniYARNCluster yarnCluster = null;
  protected static Configuration conf = new Configuration();

  @Before
  public void setupEachTime() throws IOException
  {
    StramChild.eventloop.start();
  }

  @After
  public void teardown()
  {
    StramChild.eventloop.stop();
  }

  @BeforeClass
  public static void setup() throws InterruptedException, IOException
  {
    LOG.info("Starting up YARN cluster");
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
    conf.setInt("yarn.nodemanager.vmem-pmem-ratio", 20); // workaround to avoid containers being killed because java allocated too much vmem
    conf.setStrings("yarn.scheduler.capacity.root.queues", "default");
    conf.setStrings("yarn.scheduler.capacity.root.default.capacity", "100");

    StringBuilder adminEnv = new StringBuilder();
    if (System.getenv("JAVA_HOME") == null) {
      adminEnv.append("JAVA_HOME=").append(System.getProperty(System.getProperty("java.home")));
      adminEnv.append(",");
    }
    adminEnv.append("MALLOC_ARENA_MAX=4"); // see MAPREDUCE-3068, MAPREDUCE-3065
    adminEnv.append(",");
    adminEnv.append("CLASSPATH=").append(getTestRuntimeClasspath());

    conf.set(YarnConfiguration.NM_ADMIN_USER_ENV, adminEnv.toString());

    if (yarnCluster == null) {
      yarnCluster = new MiniYARNCluster(StramMiniClusterTest.class.getName(),
                                        1, 1, 1);
      yarnCluster.init(conf);
      yarnCluster.start();
      URL url = Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml");
      if (url == null) {
        LOG.error("Could not find 'yarn-site.xml' dummy file in classpath");
        throw new RuntimeException("Could not find 'yarn-site.xml' dummy file in classpath");
      }
      yarnCluster.getConfig().set("yarn.application.classpath", new File(url.getPath()).getParent());
      OutputStream os = new FileOutputStream(new File(url.getPath()));
      yarnCluster.getConfig().writeXml(os);
      os.close();
    }
    try {
      Thread.sleep(2000);
    }
    catch (InterruptedException e) {
      LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws IOException
  {
    if (yarnCluster != null) {
      yarnCluster.stop();
      yarnCluster = null;
    }
  }

  private File createTmpPropFile(Properties props) throws IOException
  {
    File tmpFile = File.createTempFile("stram-junit", ".properties");
    tmpFile.deleteOnExit();
    props.store(new FileOutputStream(tmpFile), "StramMiniClusterTest.test1");
    LOG.info("topology: " + tmpFile);
    return tmpFile;
  }

  @Test
  public void testSetupShutdown() throws Exception
  {
    GetClusterNodesRequest request =
            Records.newRecord(GetClusterNodesRequest.class);
    ClientRMService clientRMService = yarnCluster.getResourceManager().getClientRMService();
    GetClusterNodesResponse response = clientRMService.getClusterNodes(request);
    List<NodeReport> nodeReports = response.getNodeReports();
    LOG.info("{}", nodeReports);

    for (NodeReport nr: nodeReports) {
      LOG.info("Node: {}", nr.getNodeId());
      LOG.info("Total memory: {}", nr.getCapability());
      LOG.info("Used memory: {}", nr.getUsed());
      LOG.info("Number containers: {}", nr.getNumContainers());
    }

    String appMasterJar = JarFinder.getJar(StramAppMaster.class);
    LOG.info("appmaster jar: " + appMasterJar);
    String testJar = JarFinder.getJar(StramMiniClusterTest.class);
    LOG.info("testJar: " + testJar);

    // create test application
    LogicalPlanConfiguration tb = new LogicalPlanConfiguration();
    Properties dagProps = new Properties();

    // input module (ensure shutdown works while windows are generated)
    dagProps.put("stram.operator.numGen.classname", TestGeneratorInputOperator.class.getName());
    dagProps.put("stram.operator.numGen.maxTuples", "1");

    // fake output adapter - to be ignored when determine shutdown
    //props.put("stram.stream.output.classname", HDFSOutputStream.class.getName());
    //props.put("stram.stream.output.inputNode", "module2");
    //props.put("stram.stream.output.filepath", "miniclustertest-testSetupShutdown.out");

    dagProps.put("stram.operator.module1.classname", GenericTestOperator.class.getName());

    dagProps.put("stram.operator.module2.classname", GenericTestOperator.class.getName());

    dagProps.put("stram.stream.fromNumGen.source", "numGen.outputPort");
    dagProps.put("stram.stream.fromNumGen.sinks", "module1.input1");

    dagProps.put("stram.stream.n1n2.source", "module1.output1");
    dagProps.put("stram.stream.n1n2.sinks", "module2.input1");

    dagProps.setProperty(LogicalPlan.MASTER_MEMORY_MB.name(), "128");
    dagProps.setProperty(LogicalPlan.CONTAINER_MEMORY_MB.name(), "512");
    dagProps.setProperty(LogicalPlan.DEBUG.name(), "true");
    dagProps.setProperty(LogicalPlan.CONTAINERS_MAX_COUNT.name(), "2");
    tb.addFromProperties(dagProps);

    Properties tplgProperties = tb.getProperties();
    File tmpFile = createTmpPropFile(tplgProperties);

    String[] args = {
      "--topologyProperties",
      tmpFile.getAbsolutePath()
    };

    LOG.info("Initializing Client");
    StramClient client = new StramClient(new Configuration(yarnCluster.getConfig()));
    if (StringUtils.isBlank(System.getenv("JAVA_HOME"))) {
      client.javaCmd = "java"; // JAVA_HOME not set in the yarn mini cluster
    }
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running client");
    client.startApplication();
    boolean result = client.monitorApplication();

    LOG.info("Client run completed. Result=" + result);
    Assert.assertTrue(result);
  }

  /**
   * Verify the web service deployment and lifecycle functionality
   *
   * @throws Exception
   */
  @Ignore //disabled due to web service init delay issue
  @Test
  public void testWebService() throws Exception
  {

    // single container topology of inline input and module
    Properties props = new Properties();
    props.put("stram.stream.input.classname", TestGeneratorInputOperator.class.getName());
    props.put("stram.stream.input.outputNode", "module1");
    props.put("stram.module.module1.classname", GenericTestOperator.class.getName());

    File tmpFile = createTmpPropFile(props);

    String[] args = {
      "--topologyProperties",
      tmpFile.getAbsolutePath()
    };

    LOG.info("Initializing Client");
    StramClient client = new StramClient(new Configuration(yarnCluster.getConfig()));
    if (StringUtils.isBlank(System.getenv("JAVA_HOME"))) {
      client.javaCmd = "java"; // JAVA_HOME not set in the yarn mini cluster
    }
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);

    client.startApplication();

    // attempt web service connection
    ApplicationReport appReport = client.getApplicationReport();

    try {
      Thread.sleep(5000); // delay to give web service time to fully initialize
      Client wsClient = Client.create();
      wsClient.setFollowRedirects(true);
      WebResource r = wsClient.resource("http://" + appReport.getTrackingUrl()).path(StramWebServices.PATH).path(StramWebServices.PATH_INFO);
      LOG.info("Requesting: " + r.getURI());
      ClientResponse response = r.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      LOG.info("Got response: " + json.toString());
      assertEquals("incorrect number of elements", 1, json.length());
      assertEquals("appId", appReport.getApplicationId().toString(), json.get("id"));


      r = wsClient.resource("http://" + appReport.getTrackingUrl()).path(StramWebServices.PATH).path(StramWebServices.PATH_PHYSICAL_PLAN_OPERATORS);
      LOG.info("Requesting: " + r.getURI());
      response = r.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      json = response.getEntity(JSONObject.class);
      LOG.info("Got response: " + json.toString());


    }
    finally {
      //LOG.info("waiting...");
      //synchronized (this) {
      //  this.wait();
      //}
      //boolean result = client.monitorApplication();
      client.killApplication();
    }

  }

  private static String getTestRuntimeClasspath()
  {

    InputStream classpathFileStream = null;
    BufferedReader reader = null;
    String envClassPath = "";

    LOG.info("Trying to generate classpath for app master from current thread's classpath");
    try {

      // Create classpath from generated classpath
      // Check maven pom.xml for generated classpath info
      // Works in tests where compile time env is same as runtime.
      ClassLoader thisClassLoader =
              Thread.currentThread().getContextClassLoader();
      String generatedClasspathFile = "mvn-generated-classpath";
      classpathFileStream =
              thisClassLoader.getResourceAsStream(generatedClasspathFile);
      if (classpathFileStream == null) {
        LOG.info("Could not load classpath resource " + generatedClasspathFile);
        return envClassPath;
      }
      LOG.info("Readable bytes from stream=" + classpathFileStream.available());
      reader = new BufferedReader(new InputStreamReader(classpathFileStream));
      String cp = reader.readLine();
      if (cp != null) {
        envClassPath += cp.trim() + ":";
      }
      // Put the file itself on classpath for tasks.
      envClassPath += thisClassLoader.getResource(generatedClasspathFile).getFile();
    }
    catch (IOException e) {
      LOG.info("Could not find the necessary resource to generate class path for tests. Error=" + e.getMessage());
    }

    try {
      if (classpathFileStream != null) {
        classpathFileStream.close();
      }
      if (reader != null) {
        reader.close();
      }
    }
    catch (IOException e) {
      LOG.info("Failed to close class path file stream or reader. Error=" + e.getMessage());
    }
    return envClassPath;
  }

  public static class FailingOperator extends BaseOperator implements InputOperator
  {
    @Override
    public void emitTuples()
    {
      throw new RuntimeException("Operator failure");
    }

  }

  @Test
  public void testOperatorFailureRecovery() throws Exception
  {

    LogicalPlan dag = new LogicalPlan();
    FailingOperator badOperator = dag.addOperator("badOperator", FailingOperator.class);
    dag.getContextAttributes(badOperator).attr(OperatorContext.RECOVERY_ATTEMPTS).set(1);

    LOG.info("Initializing Client");
    StramClient client = new StramClient(dag);
    if (StringUtils.isBlank(System.getenv("JAVA_HOME"))) {
      client.javaCmd = "java"; // JAVA_HOME not set in the yarn mini cluster
    }

    client.startApplication();
    client.setClientTimeout(120000);

    boolean result = client.monitorApplication();

    LOG.info("Client run completed. Result=" + result);
    Assert.assertFalse("should fail", result);

    ApplicationReport ar = client.getApplicationReport();
    Assert.assertEquals("should fail", FinalApplicationStatus.FAILED, ar.getFinalApplicationStatus());
    // unable to get the diagnostics message set by the AM here - see YARN-208
    //Assert.assertTrue("appReport " + ar, ar.getDiagnostics().contains("badOperator"));
  }

  @ShipContainingJars(classes = {javax.jms.Message.class})
  protected class ShipJarsBaseOperator extends BaseOperator
  {
  }

  @ShipContainingJars(classes = {Logger.class})
  protected class ShipJarsOperator extends ShipJarsBaseOperator
  {
  }

  @Test
  public void testShipContainingJars()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.getClassNames();
    LinkedHashSet<String> baseJars = StramClient.findJars(dag);

    dag = new LogicalPlan();
    dag.addOperator("foo", new ShipJarsOperator());
    dag.getClassNames();
    LinkedHashSet<String> jars = StramClient.findJars(dag);

    // operator class from test + 3 annotated dependencies
    Assert.assertEquals("" + jars, baseJars.size() + 3, jars.size());

    Assert.assertTrue("", jars.contains(JarFinder.getJar(Logger.class)));
    Assert.assertTrue("", jars.contains(JarFinder.getJar(javax.jms.Message.class)));
    Assert.assertTrue("", jars.contains(JarFinder.getJar(com.esotericsoftware.kryo.Kryo.class)));
  }

  @Ignore // thomas knows why this is disabled
  @Test
  public void testUnmanagedAM() throws Exception {

    new InlineAM(conf) {
      @Override
      @SuppressWarnings("SleepWhileInLoop")
      public void runAM(ApplicationAttemptId attemptId) throws Exception {
        LOG.debug("AM running {}", attemptId);

        //AMRMClient amRmClient = new AMRMClientImpl(attemptId);
        //amRmClient.init(conf);
        //amRmClient.start();



        YarnClientHelper yarnClient = new YarnClientHelper(conf);
        ApplicationMasterProtocol resourceManager = yarnClient.connectToRM();

        // register with the RM (LAUNCHED -> RUNNING)
        RegisterApplicationMasterRequest appMasterRequest = Records.newRecord(RegisterApplicationMasterRequest.class);
        resourceManager.registerApplicationMaster(appMasterRequest);

        // AM specific logic


        /*
        int containerCount = 1;
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(1500);

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(10);

        String[] hosts = {"vm1"};
        String[] racks = {"somerack"};

        AMRMClient.ContainerRequest req = new AMRMClient.ContainerRequest(capability, hosts, racks, priority, containerCount);
        amRmClient.addContainerRequest(req);

        for (int i=0; i<100; i++) {
          AllocateResponse ar = amRmClient.allocate(0);
          Thread.sleep(1000);
          LOG.debug("allocateResponse: {}" , ar);
        }
*/

        int responseId = 0;
        AllocateRequest req = Records.newRecord(AllocateRequest.class);
        req.setResponseId(responseId++);

        List<ResourceRequest> lr = Lists.newArrayList();
        lr.add(setupContainerAskForRM("hdev-vm", 1, 128, 10));
        lr.add(setupContainerAskForRM("/default-rack", 1, 128, 10));
        lr.add(setupContainerAskForRM("*", 1, 128, 10));

        req.setAskList(lr);

        LOG.info("Requesting: " + req.getAskList());
        resourceManager.allocate(req);

        for (int i=0; i<100; i++) {
          req = Records.newRecord(AllocateRequest.class);
          req.setResponseId(responseId++);

          AllocateResponse ar = resourceManager.allocate(req);
          sleep(1000);
          LOG.debug("allocateResponse: {}" , ar);
        }

        // unregister from RM
        FinishApplicationMasterRequest finishReq = Records.newRecord(FinishApplicationMasterRequest.class);
        finishReq.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
        finishReq.setDiagnostics("testUnmanagedAM finished");
        resourceManager.finishApplicationMaster(finishReq);

      }

      private ResourceRequest setupContainerAskForRM(String resourceName, int numContainers, int containerMemory, int priority)
      {
        ResourceRequest request = Records.newRecord(ResourceRequest.class);

        // setup requirements for hosts
        // whether a particular rack/host is needed
        // Refer to apis under org.apache.hadoop.net for more
        // details on how to get figure out rack/host mapping.
        // using * as any host will do for the distributed shell app
        request.setResourceName(resourceName);

        // set no. of containers needed
        request.setNumContainers(numContainers);

        // set the priority for the request
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(priority);
        request.setPriority(pri);

        // Set up resource type requirements
        // For now, only memory is supported so we set memory requirements
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(containerMemory);
        request.setCapability(capability);

        return request;
      }

    }.run();

  }

  @Ignore
  @Test
  public void testUnmanagedAM2() throws Exception {

    new InlineAM(conf) {

      @Override
      @SuppressWarnings("SleepWhileInLoop")
      public void runAM(ApplicationAttemptId attemptId) throws Exception {
        LOG.debug("AM running {}", attemptId);

        AMRMClient amRmClient = AMRMClient.createAMRMClient();
        amRmClient.init(conf);
        amRmClient.start();


        // register with the RM (LAUNCHED -> RUNNING)
        amRmClient.registerApplicationMaster("", 0, null);


        // AM specific logic

        String[] hosts = {"vm1"};
        String[] racks = {"somerack"};

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(1000);

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(10);
        AMRMClient.ContainerRequest req = new AMRMClient.ContainerRequest(capability, hosts, racks, priority);
        amRmClient.addContainerRequest(req);
        amRmClient.addContainerRequest(req);

/*
        capability = Records.newRecord(Resource.class);
        capability.setMemory(5512);
        priority = Records.newRecord(Priority.class);
        priority.setPriority(11);
        req = new AMRMClient.ContainerRequest(capability, hosts, racks, priority, 3);
        amRmClient.addContainerRequest(req);
*/
        for (int i=0; i<100; i++) {
          AllocateResponse ar = amRmClient.allocate(0);
          sleep(1000);
          LOG.debug("allocateResponse: {}" , ar);
          for (Container c : ar.getAllocatedContainers()) {
            LOG.debug("*** allocated {}", c.getResource());
            amRmClient.removeContainerRequest(req);

          }
          /*
          GetClusterNodesRequest request =
              Records.newRecord(GetClusterNodesRequest.class);
          ClientRMService clientRMService = yarnCluster.getResourceManager().getClientRMService();
          GetClusterNodesResponse response = clientRMService.getClusterNodes(request);
          List<NodeReport> nodeReports = response.getNodeReports();
          LOG.info(nodeReports);

          for (NodeReport nr: nodeReports) {
            LOG.info("Node: " + nr.getNodeId());
            LOG.info("Total memory: " + nr.getCapability());
            LOG.info("Used memory: " + nr.getUsed());
            LOG.info("Number containers: " + nr.getNumContainers());
          }
          */
        }

        // unregister from RM
        amRmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "testUnmanagedAM finished", null);

      }

    }.run();

  }

}
