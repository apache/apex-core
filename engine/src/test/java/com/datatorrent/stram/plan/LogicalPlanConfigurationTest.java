/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.plan;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.*;

import com.google.common.collect.Sets;

import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.*;
import com.datatorrent.api.Attribute.AttributeMap.AttributeInitializer;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.stram.PartitioningTest.PartitionLoadWatch;
import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.plan.LogicalPlanTest.ValidationTestOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import com.datatorrent.stram.support.StramTestSupport.RegexMatcher;

import static org.junit.Assert.*;

public class LogicalPlanConfigurationTest {

  private static OperatorMeta assertNode(LogicalPlan dag, String id) {
      OperatorMeta n = dag.getOperatorMeta(id);
      assertNotNull("operator exists id=" + id, n);
      return n;
  }

  /**
   * Test read from dt-site.xml in Hadoop configuration format.
   */
  @Test
  public void testLoadFromConfigXml() {
    Configuration conf = new Configuration(false);
    conf.addResource(StramClientUtils.DT_SITE_XML_FILE);
    //Configuration.dumpConfiguration(conf, new PrintWriter(System.out));

    LogicalPlanConfiguration builder = new LogicalPlanConfiguration(conf);

    LogicalPlan dag = new LogicalPlan();
    builder.populateDAG(dag);
    dag.validate();

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
      logger.debug(prefix  + operator.getName());
      for (StreamMeta downStream : operator.getOutputStreams().values()) {
          if (!downStream.getSinks().isEmpty()) {
            for (LogicalPlan.InputPortMeta targetNode : downStream.getSinks()) {
              printTopology(targetNode.getOperatorWrapper(), tplg, level+1);
            }
          }
      }
  }

  @Test
  public void testLoadFromPropertiesFile() throws IOException
  {
      Properties props = new Properties();
      String resourcePath = "/testTopology.properties";
      InputStream is = this.getClass().getResourceAsStream(resourcePath);
      if (is == null) {
        fail("Could not load " + resourcePath);
      }
      props.load(is);
      LogicalPlanConfiguration pb = new LogicalPlanConfiguration(new Configuration(false))
        .addFromProperties(props);

      LogicalPlan dag = new LogicalPlan();
      pb.populateDAG(dag);
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
  public void testLoadFromJson() throws Exception
  {
    String resourcePath = "/testTopology.json";
    InputStream is = this.getClass().getResourceAsStream(resourcePath);
    if (is == null) {
      fail("Could not load " + resourcePath);
    }
    StringWriter writer = new StringWriter();

    IOUtils.copy(is, writer);
    JSONObject json = new JSONObject(writer.toString());

    Configuration conf = new Configuration(false);
    conf.set(StreamingApplication.DT_PREFIX + "operator.operator3.prop.myStringProperty", "o3StringFromConf");

    LogicalPlanConfiguration planConf = new LogicalPlanConfiguration(conf);
    LogicalPlan dag = planConf.createFromJson(json, "testLoadFromJson");
    dag.validate();

    assertEquals("number of operator confs", 5, dag.getAllOperators().size());
    assertEquals("number of root operators", 1, dag.getRootOperators().size());

    StreamMeta s1 = dag.getStream("n1n2");
    assertNotNull(s1);
    assertTrue("n1n2 inline", DAG.Locality.CONTAINER_LOCAL == s1.getLocality());

    OperatorMeta operator3 = dag.getOperatorMeta("operator3");
    assertEquals("operator3.classname", GenericTestOperator.class, operator3.getOperator().getClass());

    GenericTestOperator doperator3 = (GenericTestOperator)operator3.getOperator();
    assertEquals("myStringProperty " + doperator3, "o3StringFromConf", doperator3.getMyStringProperty());
    assertFalse("booleanProperty " + doperator3, doperator3.booleanProperty);

    OperatorMeta operator4 = dag.getOperatorMeta("operator4");
    GenericTestOperator doperator4 = (GenericTestOperator)operator4.getOperator();
    assertEquals("myStringProperty " + doperator4, "overrideOperator4", doperator4.getMyStringProperty());
    assertEquals("setterOnlyOperator4 " + doperator4, "setterOnlyOperator4", doperator4.propertySetterOnly);
    assertTrue("booleanProperty " + doperator4, doperator4.booleanProperty);

    StreamMeta input1 = dag.getStream("inputStream");
    assertNotNull(input1);
    OperatorMeta inputOperator = dag.getOperatorMeta("inputOperator");
    Assert.assertEquals("input1 source", inputOperator, input1.getSource().getOperatorWrapper());
    Set<OperatorMeta> targetNodes = new HashSet<OperatorMeta>();
    for (LogicalPlan.InputPortMeta targetPort : input1.getSinks()) {
      targetNodes.add(targetPort.getOperatorWrapper());
    }
    Assert.assertEquals("operator attribute " + inputOperator, 64, (int)inputOperator.getValue(OperatorContext.MEMORY_MB));
    Assert.assertEquals("port attribute " + inputOperator, 8, (int)input1.getSource().getValue(PortContext.UNIFIER_LIMIT));
    Assert.assertEquals("input1 target ", Sets.newHashSet(dag.getOperatorMeta("operator1"), operator3, operator4), targetNodes);
  }

  @Test
  public void testAppLevelAttributes()
  {
    String appName = "app1";

    Properties props = new Properties();
    props.put(StreamingApplication.DT_PREFIX + DAG.MASTER_MEMORY_MB.getName(), "123");
    props.put(StreamingApplication.DT_PREFIX + DAG.APPLICATION_PATH.getName(), "/defaultdir");
    props.put(StreamingApplication.DT_PREFIX + "application." + appName + "." + DAG.APPLICATION_PATH.getName(), "/otherdir");
    props.put(StreamingApplication.DT_PREFIX + "application." + appName + "." + DAG.STREAMING_WINDOW_SIZE_MILLIS.getName(), "1000");

    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    dagBuilder.addFromProperties(props);

    LogicalPlan dag = new LogicalPlan();

    dagBuilder.populateDAG(dag);

    dagBuilder.setApplicationConfiguration(dag, appName,null);

    Assert.assertEquals("", "/otherdir", dag.getValue(DAG.APPLICATION_PATH));
    Assert.assertEquals("", Integer.valueOf(123), dag.getValue(DAG.MASTER_MEMORY_MB));
    Assert.assertEquals("", Integer.valueOf(1000), dag.getValue(DAG.STREAMING_WINDOW_SIZE_MILLIS));

  }
  @Test
  public void testAppLevelProperties() {
	  String appName ="app1";
	  Properties props =new Properties();
	  props.put(StreamingApplication.DT_PREFIX + "application."+appName+".testprop1","10");
	  props.put(StreamingApplication.DT_PREFIX + "application." + appName + ".prop.testprop2", "100");
	  props.put(StreamingApplication.DT_PREFIX + "application.*.prop.testprop3","1000");
	  props.put(StreamingApplication.DT_PREFIX + "application." + appName + ".inncls.a", "10000");
	  LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
	  dagBuilder.addFromProperties(props);

	  LogicalPlan dag = new LogicalPlan();
	  TestApplication app1Test=new TestApplication();

	  dagBuilder.setApplicationConfiguration(dag, appName,app1Test);
	  Assert.assertEquals("",Integer.valueOf(10),app1Test.getTestprop1());
	  Assert.assertEquals("",Integer.valueOf(100),app1Test.getTestprop2());
	  Assert.assertEquals("",Integer.valueOf(1000),app1Test.getTestprop3());
	  Assert.assertEquals("",Integer.valueOf(10000),app1Test.getInncls().getA());
  }
  @Test
  public void testPrepareDAG() {
    final MutableBoolean appInitialized = new MutableBoolean(false);
    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        Assert.assertEquals("", "hostname:9090", dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS));
        dag.setAttribute(DAG.GATEWAY_CONNECT_ADDRESS, "hostname:9091");
        appInitialized.setValue(true);
      }
    };
    Configuration conf = new Configuration(false);
    conf.addResource(StramClientUtils.DT_SITE_XML_FILE);
    LogicalPlanConfiguration pb = new LogicalPlanConfiguration(conf);

    LogicalPlan dag = new LogicalPlan();
    pb.prepareDAG(dag, app, "testconfig");

    Assert.assertTrue("populateDAG called", appInitialized.booleanValue());
    Assert.assertEquals("populateDAG overrides attribute", "hostname:9091", dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS));
  }

  @Test
  public void testOperatorConfigurationLookup() {

    Properties props = new Properties();

    // match operator by name
    props.put(StreamingApplication.DT_PREFIX + "template.matchId1.matchIdRegExp", ".*operator1.*");
    props.put(StreamingApplication.DT_PREFIX + "template.matchId1.stringProperty2", "stringProperty2Value-matchId1");
    props.put(StreamingApplication.DT_PREFIX + "template.matchId1.nested.property", "nested.propertyValue-matchId1");

    // match class name, lower priority
    props.put(StreamingApplication.DT_PREFIX + "template.matchClass1.matchClassNameRegExp", ".*" + ValidationTestOperator.class.getSimpleName());
    props.put(StreamingApplication.DT_PREFIX + "template.matchClass1.stringProperty2", "stringProperty2Value-matchClass1");

    // match class name
    props.put(StreamingApplication.DT_PREFIX + "template.t2.matchClassNameRegExp", ".*"+GenericTestOperator.class.getSimpleName());
    props.put(StreamingApplication.DT_PREFIX + "template.t2.myStringProperty", "myStringPropertyValue");

    // direct setting
    props.put(StreamingApplication.DT_PREFIX + "operator.operator3.emitFormat", "emitFormatValue");

    LogicalPlan dag = new LogicalPlan();
    Operator operator1 = dag.addOperator("operator1", new ValidationTestOperator());
    Operator operator2 = dag.addOperator("operator2", new ValidationTestOperator());
    Operator operator3 = dag.addOperator("operator3", new GenericTestOperator());

    LogicalPlanConfiguration pb = new LogicalPlanConfiguration(new Configuration(false));
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
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.myStringProperty", "myStringPropertyValue");
    conf.set(StreamingApplication.DT_PREFIX + "operator.o2.prop.stringArrayField", "a,b,c");
    conf.set(StreamingApplication.DT_PREFIX + "operator.o2.prop.mapProperty.key1", "key1Val");
    conf.set(StreamingApplication.DT_PREFIX + "operator.o2.prop.mapProperty(key1.dot)", "key1dotVal");
    conf.set(StreamingApplication.DT_PREFIX + "operator.o2.prop.mapProperty(key2.dot)", "key2dotVal");

    LogicalPlan dag = new LogicalPlan();
    GenericTestOperator o1 = dag.addOperator("o1", new GenericTestOperator());
    ValidationTestOperator o2 = dag.addOperator("o2", new ValidationTestOperator());

    LogicalPlanConfiguration pb = new LogicalPlanConfiguration(conf);

    pb.setOperatorProperties(dag, "testSetOperatorProperties");
    Assert.assertEquals("o1.myStringProperty", "myStringPropertyValue", o1.getMyStringProperty());
    Assert.assertArrayEquals("o2.stringArrayField", new String[] {"a", "b", "c"}, o2.getStringArrayField());

    Assert.assertEquals("o2.mapProperty.key1", "key1Val", o2.getMapProperty().get("key1"));
    Assert.assertEquals("o2.mapProperty(key1.dot)", "key1dotVal", o2.getMapProperty().get("key1.dot"));
    Assert.assertEquals("o2.mapProperty(key2.dot)", "key2dotVal", o2.getMapProperty().get("key2.dot"));

  }

  @ApplicationAnnotation(name="AnnotatedAlias")
  class AnnotatedApplication implements StreamingApplication {

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      //dag.setAttribute(DAGContext.APPLICATION_NAME, "testApp");
    }

  }

  @Test
  public void testAppNameAttribute() {
    StreamingApplication app = new AnnotatedApplication();
    Configuration conf = new Configuration(false);
    conf.addResource(StramClientUtils.DT_SITE_XML_FILE);

    LogicalPlanConfiguration builder = new LogicalPlanConfiguration(conf);

    Properties properties = new Properties();
    properties.put(StreamingApplication.DT_PREFIX + "application.TestAliasApp.class", app.getClass().getName());

    builder.addFromProperties(properties);

    LogicalPlan dag = new LogicalPlan();
    String appPath = app.getClass().getName().replace(".", "/") + ".class";
    dag.setAttribute(com.datatorrent.api.Context.DAGContext.APPLICATION_NAME, "testApp");
    builder.prepareDAG(dag, app, appPath);

    Assert.assertEquals("Application name", "testApp", dag.getAttributes().get(com.datatorrent.api.Context.DAGContext.APPLICATION_NAME));
  }

  @Test
  public void testAppAlias() {
    StreamingApplication app = new AnnotatedApplication();
    Configuration conf = new Configuration(false);
    conf.addResource(StramClientUtils.DT_SITE_XML_FILE);

    LogicalPlanConfiguration builder = new LogicalPlanConfiguration(conf);

    Properties properties = new Properties();
    properties.put(StreamingApplication.DT_PREFIX + "application.TestAliasApp.class", app.getClass().getName());

    builder.addFromProperties(properties);

    LogicalPlan dag = new LogicalPlan();
    String appPath = app.getClass().getName().replace(".", "/") + ".class";
    builder.prepareDAG(dag, app, appPath);

    Assert.assertEquals("Application name", "TestAliasApp", dag.getAttributes().get(com.datatorrent.api.Context.DAGContext.APPLICATION_NAME));
  }


  @Test
  public void testAppAnnotationAlias() {
    StreamingApplication app = new AnnotatedApplication();
    Configuration conf = new Configuration(false);
    conf.addResource(StramClientUtils.DT_SITE_XML_FILE);

    LogicalPlanConfiguration builder = new LogicalPlanConfiguration(conf);

    LogicalPlan dag = new LogicalPlan();
    String appPath = app.getClass().getName().replace(".", "/") + ".class";
    builder.prepareDAG(dag, app, appPath);

    Assert.assertEquals("Application name", "AnnotatedAlias", dag.getAttributes().get(com.datatorrent.api.Context.DAGContext.APPLICATION_NAME));
  }

  @Test
  public void testOperatorLevelAttributes() {
    String appName = "app1";
    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        dag.addOperator("operator1", GenericTestOperator.class);
        dag.addOperator("operator2", GenericTestOperator.class);
      }
    };

    Properties props = new Properties();
    props.put(StreamingApplication.DT_PREFIX + "application." + appName + ".class", app.getClass().getName());
    props.put(StreamingApplication.DT_PREFIX + "operator.*." + OperatorContext.APPLICATION_WINDOW_COUNT.getName(), "2");
    props.put(StreamingApplication.DT_PREFIX + "operator.*." + OperatorContext.STATS_LISTENERS.getName(), PartitionLoadWatch.class.getName());
    props.put(StreamingApplication.DT_PREFIX + "application." + appName + ".operator.operator1." + OperatorContext.APPLICATION_WINDOW_COUNT.getName(), "20");

    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    dagBuilder.addFromProperties(props);

    String appPath = app.getClass().getName().replace(".", "/") + ".class";

    LogicalPlan dag = new LogicalPlan();
    dagBuilder.prepareDAG(dag, app, appPath);

    Assert.assertEquals("", Integer.valueOf(20), dag.getOperatorMeta("operator1").getValue(OperatorContext.APPLICATION_WINDOW_COUNT));
    Assert.assertEquals("", Integer.valueOf(2), dag.getOperatorMeta("operator2").getValue(OperatorContext.APPLICATION_WINDOW_COUNT));
    Assert.assertEquals("", PartitionLoadWatch.class, dag.getOperatorMeta("operator2").getValue(OperatorContext.STATS_LISTENERS).toArray()[0].getClass());
  }

  @Test
  public void testOperatorLevelProperties() {
    String appName = "app1";
    final GenericTestOperator operator1 = new GenericTestOperator();
    final GenericTestOperator operator2 = new GenericTestOperator();
    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        dag.addOperator("operator1", operator1);
        dag.addOperator("operator2", operator2);
      }
    };

    Properties props = new Properties();
    props.put(StreamingApplication.DT_PREFIX + "application." + appName + ".class", app.getClass().getName());
    props.put(StreamingApplication.DT_PREFIX + "operator.*.myStringProperty", "pv1");
    props.put(StreamingApplication.DT_PREFIX + "operator.*.booleanProperty", Boolean.TRUE.toString());
    props.put(StreamingApplication.DT_PREFIX + "application." + appName + ".operator.operator1.myStringProperty", "apv1");

    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    dagBuilder.addFromProperties(props);

    String appPath = app.getClass().getName().replace(".", "/") + ".class";

    LogicalPlan dag = new LogicalPlan();
    dagBuilder.prepareDAG(dag, app, appPath);

    Assert.assertEquals("apv1", operator1.getMyStringProperty());
    Assert.assertEquals("pv1", operator2.getMyStringProperty());
    Assert.assertEquals(true, operator2.isBooleanProperty());
  }

  @Test
  public void testPortLevelAttributes() {
    String appName = "app1";
    final GenericTestOperator gt1 = new GenericTestOperator();
    final GenericTestOperator gt2 = new GenericTestOperator();
    final GenericTestOperator gt3 = new GenericTestOperator();
    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        dag.addOperator("operator1", gt1);
        dag.addOperator("operator2", gt2);
        dag.addOperator("operator3", gt3);
        dag.addStream("s1", gt1.outport1, gt2.inport1);
        dag.addStream("s2", gt2.outport1, gt3.inport1, gt3.inport2);
      }
    };

    Properties props = new Properties();
    props.put(StreamingApplication.DT_PREFIX + "application." + appName + ".class", app.getClass().getName());
    props.put(StreamingApplication.DT_PREFIX + "application." + appName + ".operator.operator1.port.*." + PortContext.QUEUE_CAPACITY.getName(), "" + 16 * 1024);
    props.put(StreamingApplication.DT_PREFIX + "application." + appName + ".operator.operator2.inputport.inport1." + PortContext.QUEUE_CAPACITY.getName(), "" + 32 * 1024);
    props.put(StreamingApplication.DT_PREFIX + "application." + appName + ".operator.operator2.outputport.outport1." + PortContext.QUEUE_CAPACITY.getName(), "" + 32 * 1024);
    props.put(StreamingApplication.DT_PREFIX + "application." + appName + ".operator.operator3.port.*." + PortContext.QUEUE_CAPACITY.getName(), "" + 16 * 1024);
    props.put(StreamingApplication.DT_PREFIX + "application." + appName + ".operator.operator3.inputport.inport2." + PortContext.QUEUE_CAPACITY.getName(), "" + 32 * 1024);

    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    dagBuilder.addFromProperties(props);

    String appPath = app.getClass().getName().replace(".", "/") + ".class";

    LogicalPlan dag = new LogicalPlan();
    dagBuilder.prepareDAG(dag, app, appPath);
    //dagBuilder.populateDAG(dag, new Configuration(false));

    //dagBuilder.setApplicationConfiguration(dag, appName);
    OperatorMeta om1 = dag.getOperatorMeta("operator1");
    Assert.assertEquals("", Integer.valueOf(16 * 1024), om1.getMeta(gt1.outport1).getValue(PortContext.QUEUE_CAPACITY));
    OperatorMeta om2 = dag.getOperatorMeta("operator2");
    Assert.assertEquals("", Integer.valueOf(32 * 1024), om2.getMeta(gt2.inport1).getValue(PortContext.QUEUE_CAPACITY));
    Assert.assertEquals("", Integer.valueOf(32 * 1024), om2.getMeta(gt2.outport1).getValue(PortContext.QUEUE_CAPACITY));
    OperatorMeta om3 = dag.getOperatorMeta("operator3");
    Assert.assertEquals("", Integer.valueOf(16 * 1024), om3.getMeta(gt3.inport1).getValue(PortContext.QUEUE_CAPACITY));
    Assert.assertEquals("", Integer.valueOf(32 * 1024), om3.getMeta(gt3.inport2).getValue(PortContext.QUEUE_CAPACITY));
  }


  @Test
  public void testInvalidAttribute() throws Exception {

    Assert.assertNotSame(0, com.datatorrent.api.Context.DAGContext.serialVersionUID);
    Set<Attribute<Object>> appAttributes = AttributeInitializer.getAttributes(com.datatorrent.api.Context.DAGContext.class);
    Attribute<Object> attribute = new Attribute<Object>("", null);

    Field nameField = Attribute.class.getDeclaredField("name");
    nameField.setAccessible(true);
    nameField.set(attribute, "NOT_CONFIGURABLE");
    nameField.setAccessible(false);

    appAttributes.add(attribute);

    // attribute that cannot be configured

    Properties props = new Properties();
    props.put(StreamingApplication.DT_PREFIX + "attr.NOT_CONFIGURABLE", "value");

    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    dagBuilder.addFromProperties(props);

    try {
      dagBuilder.prepareDAG(new LogicalPlan(), null, "");
      Assert.fail("Exception expected");
    } catch (Exception e) {
      Assert.assertThat("Attribute not configurable", e.getMessage(), RegexMatcher.matches("Attribute does not support property configuration: NOT_CONFIGURABLE.*"));
    }

    appAttributes.remove(attribute);

    // invalid attribute name
    props = new Properties();
    String invalidAttribute = StreamingApplication.DT_PREFIX + "attr.INVALID_NAME";
    props.put(invalidAttribute, "value");

    try {
      new LogicalPlanConfiguration(new Configuration(false)).addFromProperties(props);
      Assert.fail("Exception expected");
    } catch (Exception e) {
      Assert.assertThat("Invalid attribute name", e.getMessage(), RegexMatcher.matches("Invalid attribute reference: " + invalidAttribute));
    }

  }

  @Test
  public void testAttributesCodec() {
    Assert.assertNotSame(null, new Long[] {com.datatorrent.api.Context.DAGContext.serialVersionUID, OperatorContext.serialVersionUID, PortContext.serialVersionUID});
    @SuppressWarnings("unchecked")
    Set<Class<? extends Context>> contextClasses = Sets.newHashSet(com.datatorrent.api.Context.DAGContext.class, OperatorContext.class, PortContext.class);
    for (Class<?> c : contextClasses) {
      for (Attribute<Object> attr : AttributeInitializer.getAttributes(c)) {
        Assert.assertNotNull(attr.name + " codec", attr.codec);
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(LogicalPlanConfigurationTest.class);

  public static class TestApplication implements StreamingApplication {
    Integer testprop1;
    Integer testprop2;
    Integer testprop3;
    TestInnerClass inncls;
    public TestApplication() {
      inncls=new TestInnerClass();
    }

    public Integer getTestprop1() {
      return testprop1;
    }

    public void setTestprop1(Integer testprop1) {
      this.testprop1 = testprop1;
    }

    public Integer getTestprop2() {
      return testprop2;
    }

    public void setTestprop2(Integer testprop2) {
      this.testprop2 = testprop2;
    }

    public Integer getTestprop3() {
      return testprop3;
    }

    public void setTestprop3(Integer testprop3) {
      this.testprop3 = testprop3;
    }

    public TestInnerClass getInncls() {
      return inncls;
    }

    public void setInncls(TestInnerClass inncls) {
      this.inncls = inncls;
    }

    @Override
    public void populateDAG(DAG dag, Configuration conf) {

    }
    public class TestInnerClass{
      Integer a;

      public Integer getA() {
        return a;
      }

      public void setA(Integer a) {
        this.a = a;
      }
    }
  }
}
