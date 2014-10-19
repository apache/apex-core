/**
 * Copyright (c) 2012-2013 DataTorrent, Inc. All rights reserved.
 */
package com.datatorrent.stram;

import java.io.*;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import static java.lang.Thread.sleep;

import javax.ws.rs.core.MediaType;

import com.google.common.collect.Lists;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import org.codehaus.jettison.json.JSONObject;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.assertEquals;

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
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.util.Records;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StreamingApplication;

import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.client.StramClientUtils.YarnClientHelper;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.StreamingContainer;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import com.datatorrent.stram.webapp.StramWebServices;

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
    StreamingContainer.eventloop.start();
  }

  @After
  public void teardown()
  {
    StreamingContainer.eventloop.stop();
  }

  @BeforeClass
  public static void setup() throws InterruptedException, IOException
  {
    LOG.info("Starting up YARN cluster");
    conf = StramClientUtils.addDTDefaultResources(conf);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
    conf.setInt("yarn.nodemanager.vmem-pmem-ratio", 20); // workaround to avoid containers being killed because java allocated too much vmem
    conf.setStrings("yarn.scheduler.capacity.root.queues", "default");
    conf.setStrings("yarn.scheduler.capacity.root.default.capacity", "100");

    StringBuilder adminEnv = new StringBuilder(1024);
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
    }

    conf = yarnCluster.getConfig();
    URL url = Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml");
    if (url == null) {
      LOG.error("Could not find 'yarn-site.xml' dummy file in classpath");
      throw new RuntimeException("Could not find 'yarn-site.xml' dummy file in classpath");
    }
    File confFile = new File(url.getPath());
    yarnCluster.getConfig().set("yarn.application.classpath", confFile.getParent());
    OutputStream os = new FileOutputStream(confFile);
    LOG.debug("Conf file: {}", confFile);
    yarnCluster.getConfig().writeXml(os);
    os.close();

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

    String appMasterJar = JarFinder.getJar(StreamingAppMaster.class);
    LOG.info("appmaster jar: " + appMasterJar);
    String testJar = JarFinder.getJar(StramMiniClusterTest.class);
    LOG.info("testJar: " + testJar);

    // create test application
    Properties dagProps = new Properties();

    // input module (ensure shutdown works while windows are generated)
    dagProps.put(StreamingApplication.DT_PREFIX + "operator.numGen.classname", TestGeneratorInputOperator.class.getName());
    dagProps.put(StreamingApplication.DT_PREFIX + "operator.numGen.maxTuples", "1");

    // fake output adapter - to be ignored when determine shutdown
    //props.put(DAGContext.DT_PREFIX + "stream.output.classname", HDFSOutputStream.class.getName());
    //props.put(DAGContext.DT_PREFIX + "stream.output.inputNode", "module2");
    //props.put(DAGContext.DT_PREFIX + "stream.output.filepath", "miniclustertest-testSetupShutdown.out");

    dagProps.put(StreamingApplication.DT_PREFIX + "operator.module1.classname", GenericTestOperator.class.getName());

    dagProps.put(StreamingApplication.DT_PREFIX + "operator.module2.classname", GenericTestOperator.class.getName());

    dagProps.put(StreamingApplication.DT_PREFIX + "stream.fromNumGen.source", "numGen.outport");
    dagProps.put(StreamingApplication.DT_PREFIX + "stream.fromNumGen.sinks", "module1.inport1");

    dagProps.put(StreamingApplication.DT_PREFIX + "stream.n1n2.source", "module1.outport1");
    dagProps.put(StreamingApplication.DT_PREFIX + "stream.n1n2.sinks", "module2.inport1");

    dagProps.setProperty(StreamingApplication.DT_PREFIX + LogicalPlan.MASTER_MEMORY_MB.getName(), "128");
    dagProps.setProperty(StreamingApplication.DT_PREFIX + "operator.*." + OperatorContext.MEMORY_MB.getName(), "512");
    dagProps.setProperty(StreamingApplication.DT_PREFIX + LogicalPlan.DEBUG.getName(), "true");
    dagProps.setProperty(StreamingApplication.DT_PREFIX + LogicalPlan.CONTAINERS_MAX_COUNT.getName(), "2");

    LOG.info("Initializing Client");
    LogicalPlanConfiguration tb = new LogicalPlanConfiguration();
    tb.addFromProperties(dagProps);
    StramClient client = new StramClient(new Configuration(yarnCluster.getConfig()), createDAG(tb));
    try {
      client.start();
      if (StringUtils.isBlank(System.getenv("JAVA_HOME"))) {
        client.javaCmd = "java"; // JAVA_HOME not set in the yarn mini cluster
      }
      LOG.info("Running client");
      client.startApplication();
      boolean result = client.monitorApplication();

      LOG.info("Client run completed. Result=" + result);
      Assert.assertTrue(result);
    }
    finally {
      client.stop();
    }
  }

  private LogicalPlan createDAG(LogicalPlanConfiguration lpc) throws Exception
  {
    LogicalPlan dag = new LogicalPlan();
    Configuration appConf = new Configuration(false);
    lpc.prepareDAG(dag, lpc, "test", appConf);
    dag.validate();
    return dag;
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
    props.put(StreamingApplication.DT_PREFIX + "stream.input.classname", TestGeneratorInputOperator.class.getName());
    props.put(StreamingApplication.DT_PREFIX + "stream.input.outputNode", "module1");
    props.put(StreamingApplication.DT_PREFIX + "module.module1.classname", GenericTestOperator.class.getName());

    LOG.info("Initializing Client");
    LogicalPlanConfiguration tb = new LogicalPlanConfiguration();
    tb.addFromProperties(props);

    StramClient client = new StramClient(new Configuration(yarnCluster.getConfig()), createDAG(tb));
    if (StringUtils.isBlank(System.getenv("JAVA_HOME"))) {
      client.javaCmd = "java"; // JAVA_HOME not set in the yarn mini cluster
    }
    try {
      client.start();
      client.startApplication();

      // attempt web service connection
      ApplicationReport appReport = client.getApplicationReport();
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
      client.stop();
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
    dag.setAttribute(com.datatorrent.api.Context.DAGContext.APPLICATION_PATH, "target/" + this.getClass().getName());
    FailingOperator badOperator = dag.addOperator("badOperator", FailingOperator.class);
    dag.getContextAttributes(badOperator).put(OperatorContext.RECOVERY_ATTEMPTS, 1);

    LOG.info("Initializing Client");
    StramClient client = new StramClient(conf, dag);
    if (StringUtils.isBlank(System.getenv("JAVA_HOME"))) {
      client.javaCmd = "java"; // JAVA_HOME not set in the yarn mini cluster
    }
    try {
      client.start();
      client.startApplication();
      client.setClientTimeout(120000);

      boolean result = client.monitorApplication();

      LOG.info("Client run completed. Result=" + result);
      Assert.assertFalse("should fail", result);

      ApplicationReport ar = client.getApplicationReport();
      Assert.assertEquals("should fail", FinalApplicationStatus.FAILED, ar.getFinalApplicationStatus());
    // unable to get the diagnostics message set by the AM here - see YARN-208
      // diagnostics message does not make it here even with Hadoop 2.2 (but works on standalone cluster)
      //Assert.assertTrue("appReport " + ar, ar.getDiagnostics().contains("badOperator"));
    }
    finally {
      client.stop();
    }
  }

  protected class ShipJarsBaseOperator extends BaseOperator
  {
  }

  protected class ShipJarsOperator extends ShipJarsBaseOperator
  {
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
        AMRMClient<ContainerRequest> amRmClient = AMRMClient.createAMRMClient();
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
