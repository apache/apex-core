/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.DAGPropertiesBuilder;
import com.datatorrent.stram.cli.StramClientUtils;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.google.common.collect.Sets;

public class PropertiesTest {

  private static OperatorMeta assertNode(LogicalPlan dag, String id) {
      OperatorMeta n = dag.getOperatorMeta(id);
      assertNotNull("operator exists id=" + id, n);
      return n;
  }

  /**
   * Test read from stram-site.xml in Hadoop configuration format.
   */
  @Test
  public void testLoadFromConfigXml() {
    Configuration conf = new Configuration(false);
    conf.addResource(StramClientUtils.STRAM_SITE_XML_FILE);
    //Configuration.dumpConfiguration(conf, new PrintWriter(System.out));

    DAGPropertiesBuilder builder = new DAGPropertiesBuilder();
    builder.addFromConfiguration(conf);

    LogicalPlan dag = new LogicalPlan();
    builder.populateDAG(dag, new Configuration(false));
    dag.validate();

//    Map<String, NodeConf> operatorConfs = tb.getAllOperators();
    assertEquals("number of operator confs", 6, dag.getAllOperators().size());

    OperatorMeta operator1 = assertNode(dag, "operator1");
    OperatorMeta operator2 = assertNode(dag, "operator2");
    OperatorMeta operator3 = assertNode(dag, "operator3");
    OperatorMeta operator4 = assertNode(dag, "operator4");

    assertNotNull("operatorConf for root", operator1);
    assertEquals("operatorId set", "operator1", operator1.getName());

    // verify operator instantiation
    assertEquals(operator1.getOperator().getClass(), GenericTestOperator.class);
    GenericTestOperator GenericTestNode = (GenericTestOperator)operator1.getOperator();
    assertEquals("myStringPropertyValue", GenericTestNode.getMyStringProperty());

    // check links
    assertEquals("operator1 inputs", 0, operator1.getInputStreams().size());
    assertEquals("operator1 outputs", 1, operator1.getOutputStreams().size());
    StreamMeta n1n2 = operator2.getInputStreams().get(operator2.getMeta(((GenericTestOperator)operator2.getOperator()).inport1));
    assertNotNull("n1n2", n1n2);

    // output/input stream object same
    assertEquals("rootNode out is operator2 in", n1n2, operator1.getOutputStreams().get(operator1.getMeta(((GenericTestOperator)operator1.getOperator()).outport1)));
    assertEquals("n1n2 source", operator1, n1n2.getSource().getOperatorWrapper());
    Assert.assertEquals("n1n2 targets", 1, n1n2.getSinks().size());
    Assert.assertEquals("n1n2 target", operator2, n1n2.getSinks().get(0).getOperatorWrapper());

    assertEquals("stream name", "n1n2", n1n2.getId());
    Assert.assertEquals("n1n2 not inline (default)", null, n1n2.getLocality());

    // operator 2 streams to operator 3 and operator 4
    assertEquals("operator 2 number of outputs", 1, operator2.getOutputStreams().size());
    StreamMeta fromNode2 = operator2.getOutputStreams().values().iterator().next();

    Set<OperatorMeta> targetNodes = new HashSet<OperatorMeta>();
    for (LogicalPlan.InputPortMeta ip : fromNode2.getSinks()) {
      targetNodes.add(ip.getOperatorWrapper());
    }
    Assert.assertEquals("outputs " + fromNode2, Sets.newHashSet(operator3, operator4), targetNodes);

    OperatorMeta operator6 = assertNode(dag, "operator6");

    List<OperatorMeta> rootNodes = dag.getRootOperators();
    assertEquals("number root operators", 2, rootNodes.size());
    assertTrue("root operator2", rootNodes.contains(operator1));
    assertTrue("root operator6", rootNodes.contains(operator6));

    for (OperatorMeta n : rootNodes) {
      printTopology(n, dag, 0);
    }

  }

  private void printTopology(OperatorMeta operator, DAG tplg, int level) {
      String prefix = "";
      if (level > 0) {
        prefix = StringUtils.repeat(" ", 20*(level-1)) + "   |" + StringUtils.repeat("-", 17);
      }
      System.out.println(prefix + operator.getName());
      for (StreamMeta downStream : operator.getOutputStreams().values()) {
          if (!downStream.getSinks().isEmpty()) {
            for (LogicalPlan.InputPortMeta targetNode : downStream.getSinks()) {
              printTopology(targetNode.getOperatorWrapper(), tplg, level+1);
            }
          }
      }
  }

  @Test
  public void testLoadFromPropertiesFile() throws IOException {
      Properties props = new Properties();
      String resourcePath = "/testTopology.properties";
      InputStream is = this.getClass().getResourceAsStream(resourcePath);
      if (is == null) {
        fail("Could not load " + resourcePath);
      }
      props.load(is);
      DAGPropertiesBuilder pb = new DAGPropertiesBuilder()
        .addFromProperties(props);

      LogicalPlan dag = new LogicalPlan();
      pb.populateDAG(dag, new Configuration(false));
      dag.validate();

      assertEquals("number of operator confs", 5, dag.getAllOperators().size());
      assertEquals("number of root operators", 1, dag.getRootOperators().size());

      StreamMeta s1 = dag.getStream("n1n2");
      assertNotNull(s1);
      assertTrue("n1n2 inline", DAG.Locality.CONTAINER_LOCAL == s1.getLocality());

      OperatorMeta operator3 = dag.getOperatorMeta("operator3");
      assertEquals("operator3.classname", GenericTestOperator.class, operator3.getOperator().getClass());

      GenericTestOperator doperator3 = (GenericTestOperator)operator3.getOperator();
      assertEquals("myStringProperty " + doperator3, "myStringPropertyValueFromTemplate", doperator3.getMyStringProperty());
      assertFalse("booleanProperty " + doperator3, doperator3.booleanProperty);

      OperatorMeta operator4 = dag.getOperatorMeta("operator4");
      GenericTestOperator doperator4 = (GenericTestOperator)operator4.getOperator();
      assertEquals("myStringProperty " + doperator4, "overrideOperator4", doperator4.getMyStringProperty());
      assertEquals("setterOnlyOperator4 " + doperator4, "setterOnlyOperator4", doperator4.propertySetterOnly);
      assertTrue("booleanProperty " + doperator4, doperator4.booleanProperty);

      StreamMeta input1 = dag.getStream("inputStream");
      assertNotNull(input1);
      Assert.assertEquals("input1 source", dag.getOperatorMeta("inputOperator"), input1.getSource().getOperatorWrapper());
      Set<OperatorMeta> targetNodes = new HashSet<OperatorMeta>();
      for (LogicalPlan.InputPortMeta targetPort : input1.getSinks()) {
        targetNodes.add(targetPort.getOperatorWrapper());
      }

      Assert.assertEquals("input1 target ", Sets.newHashSet(dag.getOperatorMeta("operator1"), operator3, operator4), targetNodes);

  }

  @Test
  public void testAppLevelAttributes() {
    String appName = "app1";

    Properties props = new Properties();
    props.put("stram." + DAG.CONTAINER_MEMORY_MB.name(), "123");
    props.put("stram." + DAG.APPLICATION_PATH.name(), "/defaultdir");
    props.put("stram.application." + appName + ".attr." + DAG.APPLICATION_PATH.name(), "/otherdir");
    props.put("stram.application." + appName + ".attr." + DAG.STREAMING_WINDOW_SIZE_MILLIS.name(), "1000");

    DAGPropertiesBuilder dagBuilder = new DAGPropertiesBuilder();
    dagBuilder.addFromProperties(props);

    LogicalPlan dag = new LogicalPlan();
    dagBuilder.populateDAG(dag, new Configuration(false));

    dagBuilder.setApplicationLevelAttributes(dag, appName);
    Assert.assertEquals("", "/otherdir", dag.attrValue(DAG.APPLICATION_PATH, null));
    Assert.assertEquals("", Integer.valueOf(123), dag.attrValue(DAG.CONTAINER_MEMORY_MB, null));
    Assert.assertEquals("", Integer.valueOf(1000), dag.attrValue(DAG.STREAMING_WINDOW_SIZE_MILLIS, null));

  }

  @Test
  public void testPrepareDAG() {
    final MutableBoolean appInitialized = new MutableBoolean(false);
    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        Assert.assertEquals("", "hostname:9090", dag.attrValue(DAG.DAEMON_ADDRESS, null));
        dag.setAttribute(DAG.DAEMON_ADDRESS, "hostname:9091");
        appInitialized.setValue(true);
      }
    };
    Configuration conf = new Configuration(false);
    conf.addResource(StramClientUtils.STRAM_SITE_XML_FILE);
    DAGPropertiesBuilder pb = new DAGPropertiesBuilder();
    pb.addFromConfiguration(conf);

    LogicalPlan dag = new LogicalPlan();
    pb.prepareDAG(dag, app, "testconfig", conf);

    Assert.assertTrue("populateDAG called", appInitialized.booleanValue());
    Assert.assertEquals("populateDAG overrides attribute", "hostname:9091", dag.attrValue(DAG.DAEMON_ADDRESS, null));
  }

}
