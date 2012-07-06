package com.malhartech.stram;

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

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.util.Records;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.NodeContext;
import com.malhartech.dag.NodeContext.HeartbeatCounters;

public class StramMiniClusterTest {
  
  private static Logger LOG = LoggerFactory.getLogger(StramMiniClusterTest.class);
  protected static MiniYARNCluster yarnCluster = null;
  protected static Configuration conf = new Configuration();  
  
  @BeforeClass
  public static void setup() throws InterruptedException, IOException {
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
    
    conf.set(YarnConfiguration.NM_ADMIN_USER_ENV,adminEnv.toString()); 
    
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
    } catch (InterruptedException e) {
      LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
    } 
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (yarnCluster != null) {
      yarnCluster.stop();
      yarnCluster = null;
    }
  }
  
  @Test
  public void test1() throws Exception {

    /**
     * Find out about the currently available cluster resources
     */
      // some of this needs to happen in the app master? some in order to decide where to request the app master?
    // get NodeReports from RM: 
    GetClusterNodesRequest request = 
        Records.newRecord(GetClusterNodesRequest.class);
    ClientRMService clientRMService = yarnCluster.getResourceManager().getClientRMService();
    GetClusterNodesResponse response = clientRMService.getClusterNodes(request);
    List<NodeReport> nodeReports = response.getNodeReports();
    System.out.println(nodeReports);
    
    for (NodeReport nr : nodeReports) {
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
    Properties props = new Properties();
    props.put("stram.stream.n1n2.inputNode", "node1");
    props.put("stram.stream.n1n2.outputNode", "node2");
    props.put("stram.stream.n1n2.template", "defaultstream");

    props.put("stram.node.node1.classname", TopologyBuilderTest.EchoNode.class.getName());
    props.put("stram.node.node1.myStringProperty", "myStringPropertyValue");

    props.put("stram.node.node2.classname", TopologyBuilderTest.EchoNode.class.getName());
    File tmpFile = File.createTempFile("stram-junit", ".properties");
    tmpFile.deleteOnExit();
    props.store(new FileOutputStream(tmpFile), "StramMiniClusterTest.test1");
    LOG.info("topology: " + tmpFile);
    
    //URL location =  this.getClass().getResource("/testTopology.properties");
    //String topologyPath = location.getPath();    
    
    String[] args = {
        "--num_containers",
        "2",
        "--master_memory",
        "256",
        "--container_memory",
        "64",
        "--topologyProperties",
        tmpFile.getAbsolutePath()
    };

    LOG.info("Initializing DS Client");
    StramClient client = new StramClient(new Configuration(yarnCluster.getConfig()));
    if (StringUtils.isBlank(System.getenv("JAVA_HOME"))) {
      client.javaCmd = "java"; // JAVA_HOME not set in the yarn mini cluster
    }
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running client");
    boolean result = client.run();

    LOG.info("Client run completed. Result=" + result);
    Assert.assertTrue(result);
      
  }

  private static String getTestRuntimeClasspath() {

    InputStream classpathFileStream = null;
    BufferedReader reader = null;
    String envClassPath = "";

    LOG.info("Trying to generate classpath for app master from current thread's classpath");
    try {

      // Create classpath from generated classpath
      // Check maven ppom.xml for generated classpath info
      // Works if compile time env is same as runtime. Mainly tests.
      ClassLoader thisClassLoader =
          Thread.currentThread().getContextClassLoader();
      String generatedClasspathFile = "mrapp-generated-classpath";
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
    } catch (IOException e) {
      LOG.info("Could not find the necessary resource to generate class path for tests. Error=" + e.getMessage());
    } 

    try {
      if (classpathFileStream != null) {
        classpathFileStream.close();
      }
      if (reader != null) {
        reader.close();
      }
    } catch (IOException e) {
      LOG.info("Failed to close class path file stream or reader. Error=" + e.getMessage());
    } 
    return envClassPath;
  }     


  public static class TestDNode extends AbstractNode {

    public TestDNode(NodeContext ctx) {
      super(ctx);
    }

    int getResetCount = 0;
    Long[] tupleCounts = new Long[0];
    
    @Override
    public HeartbeatCounters resetHeartbeatCounters() {
      HeartbeatCounters stats = new HeartbeatCounters();
      if (tupleCounts.length == 0) {
          stats.tuplesProcessed = 0;
      } else {
        int count = getResetCount++ % (tupleCounts.length);
        stats.tuplesProcessed = tupleCounts[count];
      }
      return stats;
    }

    public String getTupleCounts() {
      return StringUtils.join(tupleCounts, ",");
    }

    /**
     * used to parameterize test node for heartbeat reporting
     * @param tupleCounts
     */
    public void setTupleCounts(String tupleCounts) {
      String[] scounts = StringUtils.splitByWholeSeparator(tupleCounts, ",");
      Long[] counts = new Long[scounts.length];
      for (int i=0; i<scounts.length; i++) {
        counts[i] = new Long(scounts[i].trim());
      }
      this.tupleCounts = counts;
    }

    @Override
    public void process(NodeContext context, com.malhartech.dag.StreamContext sc, Object payload) {
      LOG.info("Designed to do nothing!");
    }

    /**
     * Node will exit processing loop immediately and report not processing in heartbeat.
     */
    @Override
    protected boolean shouldShutdown() {
      return true;
    }
    
  }
  
  
}
