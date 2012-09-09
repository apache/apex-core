/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stram;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.util.Records;
import org.codehaus.jettison.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.HeartbeatCounters;
import com.malhartech.stram.conf.Topology;
import com.malhartech.stram.conf.TopologyBuilder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class StramMiniClusterTest
{
  private static Logger LOG = LoggerFactory.getLogger(StramMiniClusterTest.class);
  protected static MiniYARNCluster yarnCluster = null;
  protected static Configuration conf = new Configuration();

  @BeforeClass
  public static void setup() throws InterruptedException, IOException
  {
    LOG.info("Starting up YARN cluster");
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
    conf.setInt("yarn.nodemanager.vmem-pmem-ratio", 10); // workaround to avoid containers being killed because java allocated too much vmem

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
  public void testMiniClusterTestNode()
  {
    StramMiniClusterTest.TestDNode d = new StramMiniClusterTest.TestDNode();

    d.setTupleCounts("100, 100, 1000");
    Assert.assertEquals("100,100,1000", d.getTupleCounts());

    Assert.assertEquals("heartbeat1", 100, d.resetHeartbeatCounters().tuplesProcessed);
    Assert.assertEquals("heartbeat2", 100, d.resetHeartbeatCounters().tuplesProcessed);
    Assert.assertEquals("heartbeat3", 1000, d.resetHeartbeatCounters().tuplesProcessed);
    Assert.assertEquals("heartbeat4", 100, d.resetHeartbeatCounters().tuplesProcessed);

  }

  @Test
  public void testSetupShutdown() throws Exception
  {


    GetClusterNodesRequest request =
            Records.newRecord(GetClusterNodesRequest.class);
    ClientRMService clientRMService = yarnCluster.getResourceManager().getClientRMService();
    GetClusterNodesResponse response = clientRMService.getClusterNodes(request);
    List<NodeReport> nodeReports = response.getNodeReports();
    System.out.println(nodeReports);

    for (NodeReport nr: nodeReports) {
      System.out.println("Node: " + nr.getNodeId());
      System.out.println("Total memory: " + nr.getCapability());
      System.out.println("Used memory: " + nr.getUsed());
      System.out.println("Number containers: " + nr.getNumContainers());
    }

    String appMasterJar = JarFinder.getJar(StramAppMaster.class);
    LOG.info("appmaster jar: " + appMasterJar);
    String testJar = JarFinder.getJar(StramMiniClusterTest.class);
    LOG.info("testJar: " + testJar);

    // create test topology
    TopologyBuilder tb = new TopologyBuilder();
    Properties dagProps = new Properties();

    // input node (ensure shutdown works while windows are generated)
    dagProps.put("stram.node.numGen.classname", NumberGeneratorInputAdapter.class.getName());
    dagProps.put("stram.node.numGen.maxTuples", "1");

    // fake output adapter - to be ignored when determine shutdown
    //props.put("stram.stream.output.classname", HDFSOutputStream.class.getName());
    //props.put("stram.stream.output.inputNode", "node2");
    //props.put("stram.stream.output.filepath", "miniclustertest-testSetupShutdown.out");

    dagProps.put("stram.node.node1.classname", GenericTestNode.class.getName());

    dagProps.put("stram.node.node2.classname", GenericTestNode.class.getName());

    dagProps.put("stram.stream.fromNumGen.source", "numGen.outputPort");
    dagProps.put("stram.stream.fromNumGen.sinks", "node1.input1");

    dagProps.put("stram.stream.n1n2.source", "node1.output1");
    dagProps.put("stram.stream.n1n2.sinks", "node2.input1");

    dagProps.setProperty(Topology.STRAM_CONTAINER_MEMORY_MB, "256");
    dagProps.setProperty(Topology.STRAM_CONTAINER_MEMORY_MB, "64");
    dagProps.setProperty(Topology.STRAM_DEBUG, "true");
    dagProps.setProperty(Topology.STRAM_MAX_CONTAINERS, "2");
    tb.addFromProperties(dagProps);

    //StramLocalCluster lc = new StramLocalCluster(tb.getTopology());
    //lc.run();
    //assert(false);

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

    // single container topology of inline input and node
    Properties props = new Properties();
    props.put("stram.stream.input.classname", NumberGeneratorInputAdapter.class.getName());
    props.put("stram.stream.input.outputNode", "node1");
    props.put("stram.node.node1.classname", NoTimeoutTestNode.class.getName());

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
      WebResource r = wsClient.resource("http://" + appReport.getTrackingUrl()).path("ws").path("v1").path("stram").path("info");
      LOG.info("Requesting: " + r.getURI());
      ClientResponse response = r.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      LOG.info("Got response: " + json.toString());
      assertEquals("incorrect number of elements", 1, json.length());
      assertEquals("appId", appReport.getApplicationId().toString(), json.getJSONObject("info").get("appId"));


      r = wsClient.resource("http://" + appReport.getTrackingUrl()).path("ws").path("v1").path("stram").path("nodes");
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

//    LOG.info("Client run completed. Result=" + result);
//    Assert.assertTrue(result);

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
      // Works if compile time env is same as runtime. Mainly tests.
      ClassLoader thisClassLoader =
              Thread.currentThread().getContextClassLoader();
      String generatedClasspathFile = "mvn-generated-classpath";
      classpathFileStream =
              thisClassLoader.getResourceAsStream(generatedClasspathFile);
      if (classpathFileStream == null) {
        LOG.info("Could not classpath resource from class loader");
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

  @SuppressWarnings("PublicInnerClass")
  public static class TestDNode extends AbstractNode
  {
    @SuppressWarnings("PackageVisibleField")
    int getResetCount = 0;
    @SuppressWarnings("PackageVisibleField")
    Integer[] tupleCounts = new Integer[0];

    public HeartbeatCounters resetHeartbeatCounters()
    {
      HeartbeatCounters stats = new HeartbeatCounters();
      if (tupleCounts.length == 0) {
        stats.tuplesProcessed = 0;
      }
      else {
        int count = getResetCount++ % (tupleCounts.length);
        stats.tuplesProcessed = tupleCounts[count];
      }
      return stats;
    }

    public String getTupleCounts()
    {
      return StringUtils.join(tupleCounts, ",");
    }

    /**
     * used to parameterize test node for heartbeat reporting
     *
     * @param tupleCounts
     */
    public void setTupleCounts(String tupleCounts)
    {
      String[] scounts = StringUtils.splitByWholeSeparator(tupleCounts, ",");
      Integer[] counts = new Integer[scounts.length];
      for (int i = 0; i < scounts.length; i++) {
        counts[i] = new Integer(scounts[i].trim());
      }
      this.tupleCounts = counts;
    }

    @Override
    public void process(Object payload)
    {
      LOG.info("Designed to do nothing!");
    }

    /**
     * Exit processing loop immediately and report not processing in heartbeat.
     */
    @Override
    public void handleIdleTimeout()
    {
      deactivate();
    }
  }

  @SuppressWarnings("PublicInnerClass")
  public static class NoTimeoutTestNode extends TestDNode
  {
    @Override
    public void handleIdleTimeout()
    {
      // does not timeout
    }
  }
}
