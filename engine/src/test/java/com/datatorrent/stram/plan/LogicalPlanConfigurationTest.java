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
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.AttributeMap.Attribute;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.plan.LogicalPlanTest.ValidationTestOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import com.google.common.collect.Sets;

public class LogicalPlanConfigurationTest {

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

    LogicalPlanConfiguration builder = new LogicalPlanConfiguration();
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

    assertEquals("stream name", "n1n2", n1n2.getName());
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
      LogicalPlanConfiguration pb = new LogicalPlanConfiguration()
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

  private static String getSimpleName(Attribute<?> attr) {
    return attr.name.substring(attr.name.lastIndexOf('.')+1);
  }

  @Test
  public void testAppLevelAttributes() {
    String appName = "app1";

    Properties props = new Properties();
    props.put("stram.containerMemoryMB", "123"); // backward compatibility mapping
    props.put("stram." + getSimpleName(DAG.APPLICATION_PATH), "/defaultdir");
    props.put("stram.application." + appName + ".attr." + getSimpleName(DAG.APPLICATION_PATH), "/otherdir");
    props.put("stram.application." + appName + ".attr." + getSimpleName(DAG.STREAMING_WINDOW_SIZE_MILLIS), "1000");

    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration();
    dagBuilder.addFromProperties(props);

    LogicalPlan dag = new LogicalPlan();
    dagBuilder.populateDAG(dag, new Configuration(false));

    dagBuilder.setApplicationLevelAttributes(dag, appName);
    Assert.assertEquals("", "/otherdir", dag.getValue(DAG.APPLICATION_PATH));
    Assert.assertEquals("", Integer.valueOf(123), dag.getValue(DAG.CONTAINER_MEMORY_MB));
    Assert.assertEquals("", Integer.valueOf(1000), dag.getValue(DAG.STREAMING_WINDOW_SIZE_MILLIS));

  }

  @Test
  public void testPrepareDAG() {
    final MutableBoolean appInitialized = new MutableBoolean(false);
    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        Assert.assertEquals("", "hostname:9090", dag.getValue(DAG.GATEWAY_ADDRESS));
        dag.setAttribute(DAG.GATEWAY_ADDRESS, "hostname:9091");
        appInitialized.setValue(true);
      }
    };
    Configuration conf = new Configuration(false);
    conf.addResource(StramClientUtils.STRAM_SITE_XML_FILE);
    LogicalPlanConfiguration pb = new LogicalPlanConfiguration();
    pb.addFromConfiguration(conf);

    LogicalPlan dag = new LogicalPlan();
    pb.prepareDAG(dag, app, "testconfig", conf);

    Assert.assertTrue("populateDAG called", appInitialized.booleanValue());
    Assert.assertEquals("populateDAG overrides attribute", "hostname:9091", dag.getValue(DAG.GATEWAY_ADDRESS));
  }

  @Test
  public void testOperatorConfigurationLookup() {

    Properties props = new Properties();

    // match operator by name
    props.put("stram.template.matchId1.matchIdRegExp", ".*operator1.*");
    props.put("stram.template.matchId1.stringProperty2", "stringProperty2Value-matchId1");
    props.put("stram.template.matchId1.nested.property", "nested.propertyValue-matchId1");

    // match class name, lower priority
    props.put("stram.template.matchClass1.matchClassNameRegExp", ".*" + ValidationTestOperator.class.getSimpleName());
    props.put("stram.template.matchClass1.stringProperty2", "stringProperty2Value-matchClass1");

    // match class name
    props.put("stram.template.t2.matchClassNameRegExp", ".*"+GenericTestOperator.class.getSimpleName());
    props.put("stram.template.t2.myStringProperty", "myStringPropertyValue");

    // direct setting
    props.put("stram.operator.operator3.emitFormat", "emitFormatValue");

    LogicalPlan dag = new LogicalPlan();
    Operator operator1 = dag.addOperator("operator1", new ValidationTestOperator());
    Operator operator2 = dag.addOperator("operator2", new ValidationTestOperator());
    Operator operator3 = dag.addOperator("operator3", new GenericTestOperator());

    LogicalPlanConfiguration pb = new LogicalPlanConfiguration();
    pb.addFromProperties(props);

    Map<String, String> configProps = pb.getProperties(dag.getMeta(operator1), "appName");
    Assert.assertEquals("" + configProps, 2, configProps.size());
    Assert.assertEquals("" + configProps, "stringProperty2Value-matchId1", configProps.get("stringProperty2"));
    Assert.assertEquals("" + configProps, "nested.propertyValue-matchId1", configProps.get("nested.property"));

    configProps = pb.getProperties(dag.getMeta(operator2), "appName");
    Assert.assertEquals("" + configProps, 1, configProps.size());
    Assert.assertEquals("" + configProps, "stringProperty2Value-matchClass1", configProps.get("stringProperty2"));

    configProps = pb.getProperties(dag.getMeta(operator3), "appName");
    Assert.assertEquals("" + configProps, 2, configProps.size());
    Assert.assertEquals("" + configProps, "myStringPropertyValue", configProps.get("myStringProperty"));
    Assert.assertEquals("" + configProps, "emitFormatValue", configProps.get("emitFormat"));

  }

  @Test
  public void testSetOperatorProperties() {

    Configuration conf = new Configuration(false);
    conf.set("stram.operator.o1.myStringProperty", "myStringPropertyValue");
    conf.set("stram.operator.o2.stringArrayField", "a,b,c");

    LogicalPlan dag = new LogicalPlan();
    GenericTestOperator o1 = dag.addOperator("o1", new GenericTestOperator());
    ValidationTestOperator o2 = dag.addOperator("o2", new ValidationTestOperator());

    LogicalPlanConfiguration pb = new LogicalPlanConfiguration();
    pb.addFromConfiguration(conf);

    pb.setOperatorProperties(dag, "testSetOperatorProperties");
    Assert.assertEquals("o1.myStringProperty", "myStringPropertyValue", o1.getMyStringProperty());
    Assert.assertArrayEquals("o2.stringArrayField", new String[] {"a", "b", "c"}, o2.getStringArrayField());
  }

  @Test
  public void testAppAlias() {
    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        dag.setAttribute(DAGContext.APPLICATION_NAME, "testApp");
      }
    };
    Configuration conf = new Configuration(false);
    conf.addResource(StramClientUtils.STRAM_SITE_XML_FILE);

    LogicalPlanConfiguration builder = new LogicalPlanConfiguration();

    Properties properties = new Properties();
    properties.put("stram.application.TestAliasApp.class", app.getClass().getName());

    builder.addFromProperties(properties);

    LogicalPlan dag = new LogicalPlan();
    String appPath = app.getClass().getName().replace(".", "/") + ".class";
    builder.prepareDAG(dag, app, appPath, conf);

    Assert.assertEquals("Application name", "TestAliasApp", dag.getAttributes().get(DAGContext.APPLICATION_NAME));
  }

}
