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
package com.datatorrent.stram.plan.logical;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.validation.ValidationException;

import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Attribute.AttributeMap.AttributeInitializer;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Module;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.StringCodec;
import com.datatorrent.api.StringCodec.Integer2String;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.codec.JsonStreamCodec;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.common.util.BasicContainerOptConfigurator;
import com.datatorrent.stram.PartitioningTest.PartitionLoadWatch;
import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.SchemaTestOperator;
import com.datatorrent.stram.plan.TestPlanContext;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OutputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration.AttributeParseUtils;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration.ConfElement;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration.ContextUtils;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration.StramElement;
import com.datatorrent.stram.plan.logical.LogicalPlanTest.ValidationTestOperator;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.support.StramTestSupport.RegexMatcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LogicalPlanConfigurationTest
{

  static {
    @SuppressWarnings("MismatchedReadAndWriteOfArray")
    Object[] serial = new Object[]{MockContext1.serialVersionUID, MockContext2.serialVersionUID};
  }

  private static OperatorMeta assertNode(LogicalPlan dag, String id)
  {
    OperatorMeta n = dag.getOperatorMeta(id);
    assertNotNull("operator exists id=" + id, n);
    return n;
  }

  public static class TestStreamCodec<T> extends JsonStreamCodec<T> implements Serializable
  {
    private static final long serialVersionUID = 1L;
  }

  /**
   * Test read from configuration file in Hadoop configuration format.
   */
  @Test
  public void testLoadFromConfigXml()
  {
    Configuration conf = new Configuration(false);
    conf.addResource(StramClientUtils.DT_SITE_XML_FILE);

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
    assertEquals(operator1.getOperator().getClass(), TestGeneratorInputOperator.class);
    TestGeneratorInputOperator GenericTestNode = (TestGeneratorInputOperator)operator1.getOperator();
    assertEquals("myStringPropertyValue", GenericTestNode.getMyStringProperty());

    // check links
    assertEquals("operator1 inputs", 0, operator1.getInputStreams().size());
    assertEquals("operator1 outputs", 1, operator1.getOutputStreams().size());
    StreamMeta n1n2 = operator2.getInputStreams().get(operator2.getMeta(((GenericTestOperator)operator2.getOperator()).inport1));
    assertNotNull("n1n2", n1n2);

    // output/input stream object same
    assertEquals("rootNode out is operator2 in", n1n2, operator1.getOutputStreams().get(operator1.getMeta(((TestGeneratorInputOperator)operator1.getOperator()).outport)));
    assertEquals("n1n2 source", operator1, n1n2.getSource().getOperatorMeta());
    Assert.assertEquals("n1n2 targets", 1, n1n2.getSinks().size());
    Assert.assertEquals("n1n2 target", operator2, n1n2.getSinks().iterator().next().getOperatorMeta());

    assertEquals("stream name", "n1n2", n1n2.getName());
    Assert.assertEquals("n1n2 not inline (default)", null, n1n2.getLocality());

    // operator 2 streams to operator 3 and operator 4
    assertEquals("operator 2 number of outputs", 1, operator2.getOutputStreams().size());
    StreamMeta fromNode2 = operator2.getOutputStreams().values().iterator().next();

    Set<OperatorMeta> targetNodes = Sets.newHashSet();
    for (LogicalPlan.InputPortMeta ip : fromNode2.getSinks()) {
      targetNodes.add(ip.getOperatorMeta());
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

  private void printTopology(OperatorMeta operator, DAG tplg, int level)
  {
    String prefix = "";
    if (level > 0) {
      prefix = StringUtils.repeat(" ", 20 * (level - 1)) + "   |" + StringUtils.repeat("-", 17);
    }
    logger.debug(prefix + operator.getName());
    for (StreamMeta downStream : operator.getOutputStreams().values()) {
      if (!downStream.getSinks().isEmpty()) {
        for (LogicalPlan.InputPortMeta targetNode : downStream.getSinks()) {
          printTopology(targetNode.getOperatorMeta(), tplg, level + 1);
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
    LogicalPlanConfiguration pb = new LogicalPlanConfiguration(new Configuration(false)).addFromProperties(props, null);

    LogicalPlan dag = new LogicalPlan();
    pb.populateDAG(dag);
    dag.validate();

    assertEquals("number of operator confs", 5, dag.getAllOperators().size());
    assertEquals("number of root operators", 1, dag.getRootOperators().size());

    StreamMeta s1 = dag.getStream("n1n2");
    assertNotNull(s1);
    assertTrue("n1n2 locality", DAG.Locality.CONTAINER_LOCAL == s1.getLocality());

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
    Assert.assertEquals("input1 source", dag.getOperatorMeta("inputOperator"), input1.getSource().getOperatorMeta());
    Set<OperatorMeta> targetNodes = Sets.newHashSet();
    for (LogicalPlan.InputPortMeta targetPort : input1.getSinks()) {
      targetNodes.add(targetPort.getOperatorMeta());
    }

    Assert.assertEquals("input1 target ", Sets.newHashSet(dag.getOperatorMeta("operator1"), operator3, operator4), targetNodes);

  }

  @Test
  public void testLoadFromPropertiesFileWithLegacyPrefix() throws IOException
  {
    Properties props = new Properties();
    String resourcePath = "/testTopologyLegacyPrefix.properties";
    InputStream is = this.getClass().getResourceAsStream(resourcePath);
    if (is == null) {
      fail("Could not load " + resourcePath);
    }
    props.load(is);
    LogicalPlanConfiguration pb = new LogicalPlanConfiguration(new Configuration(false)).addFromProperties(props, null);

    LogicalPlan dag = new LogicalPlan();
    pb.populateDAG(dag);
    dag.validate();

    assertEquals("number of operators", 2, dag.getAllOperators().size());
    assertEquals("number of root operators", 1, dag.getRootOperators().size());

    StreamMeta s1 = dag.getStream("s1");
    assertNotNull(s1);
    assertTrue("s1 locality", DAG.Locality.CONTAINER_LOCAL == s1.getLocality());

    OperatorMeta o2m = dag.getOperatorMeta("o2");
    assertEquals(GenericTestOperator.class, o2m.getOperator().getClass());
    GenericTestOperator o2 = (GenericTestOperator)o2m.getOperator();
    assertEquals("myStringProperty " + o2, "myStringPropertyValue", o2.getMyStringProperty());
  }

  @Test
  public void testDeprecation()
  {
    String value = "bar";
    String oldKey = StreamingApplication.DT_PREFIX + Context.DAGContext.APPLICATION_NAME.getName();
    String newKey = LogicalPlanConfiguration.KEY_APPLICATION_NAME;
    Configuration config = new Configuration(false);
    config.set(oldKey, value);
    Assert.assertEquals(value, config.get(newKey));
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
    conf.set(StreamingApplication.APEX_PREFIX + "operator.operator3.prop.myStringProperty", "o3StringFromConf");

    LogicalPlanConfiguration planConf = new LogicalPlanConfiguration(conf);
    LogicalPlan dag = planConf.createFromJson(json, "testLoadFromJson");
    dag.validate();

    assertEquals("DAG attribute CONTAINER_JVM_OPTIONS ", dag.getAttributes().get(DAGContext.CONTAINER_JVM_OPTIONS), "-Xmx16m");
    Map<Class<?>, Class<? extends StringCodec<?>>> stringCodecsMap = Maps.newHashMap();
    stringCodecsMap.put(Integer.class, Integer2String.class);
    assertEquals("DAG attribute STRING_CODECS ", stringCodecsMap, dag.getAttributes().get(DAGContext.STRING_CODECS));
    assertEquals("DAG attribute CONTAINER_OPTS_CONFIGURATOR ", BasicContainerOptConfigurator.class, dag.getAttributes().get(DAGContext.CONTAINER_OPTS_CONFIGURATOR).getClass());

    assertEquals("number of operator confs", 5, dag.getAllOperators().size());
    assertEquals("number of root operators", 1, dag.getRootOperators().size());

    StreamMeta s1 = dag.getStream("n1n2");
    assertNotNull(s1);
    assertTrue("n1n2 inline", DAG.Locality.CONTAINER_LOCAL == s1.getLocality());

    OperatorMeta input = dag.getOperatorMeta("inputOperator");
    TestStatsListener tsl = new TestStatsListener();
    tsl.setIntProp(222);
    List<StatsListener> sll = Lists.<StatsListener>newArrayList(tsl);
    assertEquals("inputOperator STATS_LISTENERS attribute ", sll, input.getAttributes().get(OperatorContext.STATS_LISTENERS));
    for (OutputPortMeta opm : input.getOutputStreams().keySet()) {
      assertTrue("output port of input Operator attribute is JsonStreamCodec ", opm.getAttributes().get(PortContext.STREAM_CODEC) instanceof JsonStreamCodec<?>);
    }

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
    Assert.assertEquals("input1 source", inputOperator, input1.getSource().getOperatorMeta());
    Set<OperatorMeta> targetNodes = Sets.newHashSet();
    for (LogicalPlan.InputPortMeta targetPort : input1.getSinks()) {
      targetNodes.add(targetPort.getOperatorMeta());
    }
    Assert.assertEquals("operator attribute " + inputOperator, 64, (int)inputOperator.getValue(OperatorContext.MEMORY_MB));
    Assert.assertEquals("port attribute " + inputOperator, 8, (int)input1.getSource().getValue(PortContext.UNIFIER_LIMIT));
    Assert.assertEquals("input1 target ", Sets.newHashSet(dag.getOperatorMeta("operator1"), operator3, operator4), targetNodes);
  }

  @Test
  @SuppressWarnings("UnnecessaryBoxing")
  public void testAppLevelAttributes()
  {
    String appName = "app1";

    Properties props = new Properties();
    props.put(StreamingApplication.APEX_PREFIX + DAG.MASTER_MEMORY_MB.getName(), "123");
    props.put(StreamingApplication.APEX_PREFIX + DAG.CONTAINER_JVM_OPTIONS.getName(), "-Dlog4j.properties=custom_log4j.properties");
    props.put(StreamingApplication.APEX_PREFIX + DAG.APPLICATION_PATH.getName(), "/defaultdir");
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + "." + DAG.APPLICATION_PATH.getName(), "/otherdir");
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + "." + DAG.STREAMING_WINDOW_SIZE_MILLIS.getName(), "1000");

    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    dagBuilder.addFromProperties(props, null);

    LogicalPlan dag = new LogicalPlan();

    dagBuilder.populateDAG(dag);

    dagBuilder.setApplicationConfiguration(dag, appName, null);

    Assert.assertEquals("", "/otherdir", dag.getValue(DAG.APPLICATION_PATH));
    Assert.assertEquals("", Integer.valueOf(123), dag.getValue(DAG.MASTER_MEMORY_MB));
    Assert.assertEquals("", Integer.valueOf(1000), dag.getValue(DAG.STREAMING_WINDOW_SIZE_MILLIS));
    Assert.assertEquals("", "-Dlog4j.properties=custom_log4j.properties", dag.getValue(DAG.CONTAINER_JVM_OPTIONS));

  }

  @Test
  @SuppressWarnings("UnnecessaryBoxing")
  public void testAppLevelProperties()
  {
    String appName = "app1";
    Properties props = new Properties();
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".testprop1", "10");
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".prop.testprop2", "100");
    props.put(StreamingApplication.APEX_PREFIX + "application.*.prop.testprop3", "1000");
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".inncls.a", "10000");
    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    dagBuilder.addFromProperties(props, null);

    LogicalPlan dag = new LogicalPlan();
    TestApplication app1Test = new TestApplication();

    dagBuilder.setApplicationConfiguration(dag, appName, app1Test);
    Assert.assertEquals("", Integer.valueOf(10), app1Test.getTestprop1());
    Assert.assertEquals("", Integer.valueOf(100), app1Test.getTestprop2());
    Assert.assertEquals("", Integer.valueOf(1000), app1Test.getTestprop3());
    Assert.assertEquals("", Integer.valueOf(10000), app1Test.getInncls().getA());
  }

  @Test
  public void testPrepareDAG()
  {
    final MutableBoolean appInitialized = new MutableBoolean(false);
    StreamingApplication app = new StreamingApplication()
    {
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
  public void testOperatorConfigurationLookup()
  {

    Properties props = new Properties();

    // match operator by name
    props.put(StreamingApplication.APEX_PREFIX + "template.matchId1.matchIdRegExp", ".*operator1.*");
    props.put(StreamingApplication.APEX_PREFIX + "template.matchId1.stringProperty2", "stringProperty2Value-matchId1");
    props.put(StreamingApplication.APEX_PREFIX + "template.matchId1.nested.property", "nested.propertyValue-matchId1");

    // match class name, lower priority
    props.put(StreamingApplication.APEX_PREFIX + "template.matchClass1.matchClassNameRegExp", ".*" + ValidationTestOperator.class.getSimpleName());
    props.put(StreamingApplication.APEX_PREFIX + "template.matchClass1.stringProperty2", "stringProperty2Value-matchClass1");

    // match class name
    props.put(StreamingApplication.APEX_PREFIX + "template.t2.matchClassNameRegExp", ".*" + GenericTestOperator.class.getSimpleName());
    props.put(StreamingApplication.APEX_PREFIX + "template.t2.myStringProperty", "myStringPropertyValue");

    // direct setting
    props.put(StreamingApplication.APEX_PREFIX + "operator.operator3.emitFormat", "emitFormatValue");

    LogicalPlan dag = new LogicalPlan();
    Operator operator1 = dag.addOperator("operator1", new ValidationTestOperator());
    Operator operator2 = dag.addOperator("operator2", new ValidationTestOperator());
    Operator operator3 = dag.addOperator("operator3", new GenericTestOperator());

    LogicalPlanConfiguration pb = new LogicalPlanConfiguration(new Configuration(false));
    LOG.debug("calling addFromProperties");
    pb.addFromProperties(props, null);

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
  public void testSetOperatorProperties()
  {

    Configuration conf = new Configuration(false);
    conf.set(StreamingApplication.APEX_PREFIX + "operator.o1.prop.myStringProperty", "myStringPropertyValue");
    conf.set(StreamingApplication.APEX_PREFIX + "operator.o2.prop.stringArrayField", "a,b,c");
    conf.set(StreamingApplication.APEX_PREFIX + "operator.o2.prop.mapProperty.key1", "key1Val");
    conf.set(StreamingApplication.APEX_PREFIX + "operator.o2.prop.mapProperty(key1.dot)", "key1dotVal");
    conf.set(StreamingApplication.APEX_PREFIX + "operator.o2.prop.mapProperty(key2.dot)", "key2dotVal");

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

  @ApplicationAnnotation(name = "AnnotatedAlias")
  class AnnotatedApplication implements StreamingApplication
  {

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
    }

  }

  @Test
  public void testAppNameAttribute()
  {
    StreamingApplication app = new AnnotatedApplication();
    Configuration conf = new Configuration(false);
    conf.addResource(StramClientUtils.DT_SITE_XML_FILE);

    LogicalPlanConfiguration builder = new LogicalPlanConfiguration(conf);

    Properties properties = new Properties();
    properties.put(StreamingApplication.APEX_PREFIX + "application.TestAliasApp.class", app.getClass().getName());

    builder.addFromProperties(properties, null);

    LogicalPlan dag = new LogicalPlan();
    String appPath = app.getClass().getName().replace(".", "/") + ".class";
    dag.setAttribute(com.datatorrent.api.Context.DAGContext.APPLICATION_NAME, "testApp");
    builder.prepareDAG(dag, app, appPath);

    Assert.assertEquals("Application name", "testApp", dag.getAttributes().get(com.datatorrent.api.Context.DAGContext.APPLICATION_NAME));
  }

  @Test
  public void testAppAlias()
  {
    StreamingApplication app = new AnnotatedApplication();
    Configuration conf = new Configuration(false);
    conf.addResource(StramClientUtils.DT_SITE_XML_FILE);

    LogicalPlanConfiguration builder = new LogicalPlanConfiguration(conf);

    Properties properties = new Properties();
    properties.put(StreamingApplication.APEX_PREFIX + "application.TestAliasApp.class", app.getClass().getName());

    builder.addFromProperties(properties, null);

    LogicalPlan dag = new LogicalPlan();
    String appPath = app.getClass().getName().replace(".", "/") + ".class";
    builder.prepareDAG(dag, app, appPath);

    Assert.assertEquals("Application name", "TestAliasApp", dag.getAttributes().get(com.datatorrent.api.Context.DAGContext.APPLICATION_NAME));
  }

  @Test
  public void testAppAnnotationAlias()
  {
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
  @SuppressWarnings({"UnnecessaryBoxing", "AssertEqualsBetweenInconvertibleTypes"})
  public void testOperatorLevelAttributes()
  {
    String appName = "app1";
    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        dag.addOperator("operator1", GenericTestOperator.class);
        dag.addOperator("operator2", GenericTestOperator.class);
      }
    };

    Properties props = new Properties();
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".class", app.getClass().getName());
    props.put(StreamingApplication.APEX_PREFIX + "operator.*." + OperatorContext.APPLICATION_WINDOW_COUNT.getName(), "2");
    props.put(StreamingApplication.APEX_PREFIX + "operator.*." + OperatorContext.STATS_LISTENERS.getName(), PartitionLoadWatch.class.getName());
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".operator.operator1." + OperatorContext.APPLICATION_WINDOW_COUNT.getName(), "20");

    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    dagBuilder.addFromProperties(props, null);

    String appPath = app.getClass().getName().replace(".", "/") + ".class";

    LogicalPlan dag = new LogicalPlan();
    dagBuilder.prepareDAG(dag, app, appPath);

    Assert.assertEquals("", Integer.valueOf(20), dag.getOperatorMeta("operator1").getValue(OperatorContext.APPLICATION_WINDOW_COUNT));
    Assert.assertEquals("", Integer.valueOf(2), dag.getOperatorMeta("operator2").getValue(OperatorContext.APPLICATION_WINDOW_COUNT));
    Assert.assertEquals("", PartitionLoadWatch.class, dag.getOperatorMeta("operator2").getValue(OperatorContext.STATS_LISTENERS).toArray()[0].getClass());
  }

  @Test
  @SuppressWarnings({"UnnecessaryBoxing", "AssertEqualsBetweenInconvertibleTypes"})
  public void testUnifierLevelAttributes()
  {
    String appName = "app1";
    final GenericTestOperator operator1 = new GenericTestOperator();
    final GenericTestOperator operator2 = new GenericTestOperator();
    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        dag.addOperator("operator1", operator1);
        dag.addOperator("operator2", operator2);
        dag.addStream("s1", operator1.outport1, operator2.inport1);
      }
    };

    Properties props = new Properties();
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".class", app.getClass().getName());
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".operator.operator1.outputport.outport1.unifier." + OperatorContext.APPLICATION_WINDOW_COUNT.getName(), "2");
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".operator.operator1.outputport.outport1.unifier." + OperatorContext.MEMORY_MB.getName(), "512");
    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    dagBuilder.addFromProperties(props, null);

    String appPath = app.getClass().getName().replace(".", "/") + ".class";

    LogicalPlan dag = new LogicalPlan();
    dagBuilder.prepareDAG(dag, app, appPath);

    OperatorMeta om = null;
    for (Map.Entry<OutputPortMeta, StreamMeta> entry : dag.getOperatorMeta("operator1").getOutputStreams().entrySet()) {
      if (entry.getKey().getPortName().equals("outport1")) {
        om = entry.getKey().getUnifierMeta();
      }
    }
    Assert.assertNotNull(om);
    Assert.assertEquals("", Integer.valueOf(2), om.getValue(OperatorContext.APPLICATION_WINDOW_COUNT));
    Assert.assertEquals("", Integer.valueOf(512), om.getValue(OperatorContext.MEMORY_MB));
  }

  @Test
  @SuppressWarnings({"UnnecessaryBoxing", "AssertEqualsBetweenInconvertibleTypes"})
  public void testModuleUnifierLevelAttributes()
  {
    class DummyOperator extends BaseOperator
    {
      int prop;

      public transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
      {
        @Override
        public void process(Integer tuple)
        {
          LOG.debug(tuple.intValue() + " processed");
          output.emit(tuple);
        }
      };
      public transient DefaultOutputPort<Integer> output = new DefaultOutputPort<>();
    }

    class DummyOutputOperator extends BaseOperator
    {
      int prop;

      public transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
      {
        @Override
        public void process(Integer tuple)
        {
          LOG.debug(tuple.intValue() + " processed");
        }
      };
    }

    class TestUnifierAttributeModule implements Module
    {
      public transient ProxyInputPort<Integer> moduleInput = new ProxyInputPort<>();
      public transient ProxyOutputPort<Integer> moduleOutput = new Module.ProxyOutputPort<>();

      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        DummyOperator dummyOperator = dag.addOperator("DummyOperator", new DummyOperator());
        dag.setOperatorAttribute(dummyOperator, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<DummyOperator>(3));
        dag.setUnifierAttribute(dummyOperator.output, OperatorContext.TIMEOUT_WINDOW_COUNT, 2);
        moduleInput.set(dummyOperator.input);
        moduleOutput.set(dummyOperator.output);
      }
    }

    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        Module m1 = dag.addModule("TestModule", new TestUnifierAttributeModule());
        DummyOutputOperator dummyOutputOperator = dag.addOperator("DummyOutputOperator", new DummyOutputOperator());
        dag.addStream("Module To Operator", ((TestUnifierAttributeModule)m1).moduleOutput, dummyOutputOperator.input);
      }
    };

    String appName = "UnifierApp";
    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new MockStorageAgent());
    dagBuilder.prepareDAG(dag, app, appName);
    LogicalPlan.OperatorMeta ometa = dag.getOperatorMeta("TestModule$DummyOperator");
    LogicalPlan.OperatorMeta om = null;
    for (Map.Entry<LogicalPlan.OutputPortMeta, LogicalPlan.StreamMeta> entry : ometa.getOutputStreams().entrySet()) {
      if (entry.getKey().getPortName().equals("output")) {
        om = entry.getKey().getUnifierMeta();
      }
    }

    /*
     * Verify the attribute value after preparing DAG.
     */
    Assert.assertNotNull(om);
    Assert.assertEquals("", Integer.valueOf(2), om.getValue(Context.OperatorContext.TIMEOUT_WINDOW_COUNT));

    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());
    List<PTContainer> containers = plan.getContainers();
    LogicalPlan.OperatorMeta operatorMeta = null;
    for (PTContainer container : containers) {
      List<PTOperator> operators = container.getOperators();
      for (PTOperator operator : operators) {
        if (operator.isUnifier()) {
          operatorMeta = operator.getOperatorMeta();
        }
      }
    }

    /*
     * Verify attribute after physical plan creation with partitioned operators.
     */
    Assert.assertEquals("", Integer.valueOf(2), operatorMeta.getValue(OperatorContext.TIMEOUT_WINDOW_COUNT));
  }

  @Test
  public void testOperatorLevelProperties()
  {
    String appName = "app1";
    final GenericTestOperator operator1 = new GenericTestOperator();
    final GenericTestOperator operator2 = new GenericTestOperator();
    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        dag.addOperator("operator1", operator1);
        dag.addOperator("operator2", operator2);
      }
    };

    Properties props = new Properties();
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".class", app.getClass().getName());
    props.put(StreamingApplication.APEX_PREFIX + "operator.*.myStringProperty", "pv1");
    props.put(StreamingApplication.APEX_PREFIX + "operator.*.booleanProperty", Boolean.TRUE.toString());
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".operator.operator1.myStringProperty", "apv1");

    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    dagBuilder.addFromProperties(props, null);

    String appPath = app.getClass().getName().replace(".", "/") + ".class";

    LogicalPlan dag = new LogicalPlan();
    dagBuilder.prepareDAG(dag, app, appPath);

    Assert.assertEquals("apv1", operator1.getMyStringProperty());
    Assert.assertEquals("pv1", operator2.getMyStringProperty());
    Assert.assertEquals(true, operator2.isBooleanProperty());
  }

  @Test
  public void testApplicationLevelParameter()
  {
    String appName = "app1";
    final GenericTestOperator operator1 = new GenericTestOperator();
    final GenericTestOperator operator2 = new GenericTestOperator();
    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        dag.addOperator("operator1", operator1);
        dag.addOperator("operator2", operator2);
      }
    };

    Properties props = new Properties();
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".class", app.getClass().getName());
    props.put(StreamingApplication.APEX_PREFIX + "operator.*.myStringProperty", "foo ${xyz} bar ${zzz} baz");
    props.put(StreamingApplication.APEX_PREFIX + "operator.*.booleanProperty", Boolean.TRUE.toString());
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".operator.operator1.myStringProperty", "apv1");

    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));

    Configuration vars = new Configuration(false);
    vars.set("xyz", "123");
    vars.set("zzz", "456");
    dagBuilder.addFromProperties(props, vars);

    String appPath = app.getClass().getName().replace(".", "/") + ".class";

    LogicalPlan dag = new LogicalPlan();
    dagBuilder.prepareDAG(dag, app, appPath);

    Assert.assertEquals("apv1", operator1.getMyStringProperty());
    Assert.assertEquals("foo 123 bar 456 baz", operator2.getMyStringProperty());
    Assert.assertEquals(true, operator2.isBooleanProperty());
  }

  @Test
  @SuppressWarnings("UnnecessaryBoxing")
  public void testPortLevelAttributes()
  {
    String appName = "app1";
    SimpleTestApplication app = new SimpleTestApplication();

    Properties props = new Properties();
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".class", app.getClass().getName());
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".operator.operator1.port.*." + PortContext.QUEUE_CAPACITY.getName(), "" + 16 * 1024);
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".operator.operator2.inputport.inport1." + PortContext.QUEUE_CAPACITY.getName(), "" + 32 * 1024);
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".operator.operator2.outputport.outport1." + PortContext.QUEUE_CAPACITY.getName(), "" + 32 * 1024);
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".operator.operator3.port.*." + PortContext.QUEUE_CAPACITY.getName(), "" + 16 * 1024);
    props.put(StreamingApplication.APEX_PREFIX + "application." + appName + ".operator.operator3.inputport.inport2." + PortContext.QUEUE_CAPACITY.getName(), "" + 32 * 1024);

    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    dagBuilder.addFromProperties(props, null);

    String appPath = app.getClass().getName().replace(".", "/") + ".class";

    LogicalPlan dag = new LogicalPlan();
    dagBuilder.prepareDAG(dag, app, appPath);

    OperatorMeta om1 = dag.getOperatorMeta("operator1");
    Assert.assertEquals("", Integer.valueOf(16 * 1024), om1.getMeta(app.gt1.outport1).getValue(PortContext.QUEUE_CAPACITY));
    OperatorMeta om2 = dag.getOperatorMeta("operator2");
    Assert.assertEquals("", Integer.valueOf(32 * 1024), om2.getMeta(app.gt2.inport1).getValue(PortContext.QUEUE_CAPACITY));
    Assert.assertEquals("", Integer.valueOf(32 * 1024), om2.getMeta(app.gt2.outport1).getValue(PortContext.QUEUE_CAPACITY));
    OperatorMeta om3 = dag.getOperatorMeta("operator3");
    Assert.assertEquals("", Integer.valueOf(16 * 1024), om3.getMeta(app.gt3.inport1).getValue(PortContext.QUEUE_CAPACITY));
    Assert.assertEquals("", Integer.valueOf(32 * 1024), om3.getMeta(app.gt3.inport2).getValue(PortContext.QUEUE_CAPACITY));
  }

  @Test
  public void testInvalidAttribute() throws Exception
  {
    Assert.assertNotSame(0, com.datatorrent.api.Context.DAGContext.serialVersionUID);
    Attribute<String> attribute = new Attribute<>("", null);

    Field nameField = Attribute.class.getDeclaredField("name");
    nameField.setAccessible(true);
    nameField.set(attribute, "NOT_CONFIGURABLE");
    nameField.setAccessible(false);

    ContextUtils.addAttribute(com.datatorrent.api.Context.DAGContext.class, attribute);
    AttributeParseUtils.initialize();
    ConfElement.initialize();

    // attribute that cannot be configured

    Properties props = new Properties();
    props.put(StreamingApplication.APEX_PREFIX + "attr.NOT_CONFIGURABLE", "value");

    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    dagBuilder.addFromProperties(props, null);

    try {
      dagBuilder.prepareDAG(new LogicalPlan(), null, "");
      Assert.fail("Exception expected");
    } catch (Exception e) {
      Assert.assertThat("Attribute not configurable", e.getMessage(), RegexMatcher.matches("Attribute does not support property configuration: NOT_CONFIGURABLE.*"));
    }

    ContextUtils.removeAttribute(com.datatorrent.api.Context.DAGContext.class, attribute);
    AttributeParseUtils.initialize();
    ConfElement.initialize();

    // invalid attribute name
    props = new Properties();
    String invalidAttribute = StreamingApplication.APEX_PREFIX + "attr.INVALID_NAME";
    props.put(invalidAttribute, "value");

    try {
      new LogicalPlanConfiguration(new Configuration(false)).addFromProperties(props, null);
      Assert.fail("Exception expected");
    } catch (Exception e) {
      LOG.debug("Exception message: {}", e);
      Assert.assertThat("Invalid attribute name", e.getMessage(), RegexMatcher.matches("Invalid attribute reference: " + invalidAttribute));
    }
  }

  @Test
  public void testAttributesCodec()
  {
    Assert.assertNotSame(null, new Long[] {com.datatorrent.api.Context.DAGContext.serialVersionUID, OperatorContext.serialVersionUID, PortContext.serialVersionUID});
    @SuppressWarnings("unchecked")
    Set<Class<? extends Context>> contextClasses = Sets.newHashSet(com.datatorrent.api.Context.DAGContext.class, OperatorContext.class, PortContext.class);
    for (Class<?> c : contextClasses) {
      for (Attribute<Object> attr : AttributeInitializer.getAttributes(c)) {
        Assert.assertNotNull(attr.name + " codec", attr.codec);
      }
    }
  }

  @Test
  public void testTupleClassAttr() throws Exception
  {
    String resourcePath = "/schemaTestTopology.json";
    InputStream is = this.getClass().getResourceAsStream(resourcePath);
    if (is == null) {
      fail("Could not load " + resourcePath);
    }
    StringWriter writer = new StringWriter();

    IOUtils.copy(is, writer);
    JSONObject json = new JSONObject(writer.toString());

    Configuration conf = new Configuration(false);

    LogicalPlanConfiguration planConf = new LogicalPlanConfiguration(conf);
    LogicalPlan dag = planConf.createFromJson(json, "testLoadFromJson");
    dag.validate();

    OperatorMeta operator1 = dag.getOperatorMeta("operator1");
    assertEquals("operator1.classname", SchemaTestOperator.class, operator1.getOperator().getClass());

    StreamMeta input1 = dag.getStream("inputStream");
    assertNotNull(input1);
    for (LogicalPlan.InputPortMeta targetPort : input1.getSinks()) {
      Assert.assertEquals("tuple class name required", TestSchema.class, targetPort.getAttributes().get(PortContext.TUPLE_CLASS));
    }
  }

  @Test(expected = ValidationException.class)
  public void testTupleClassAttrValidation() throws Exception
  {
    String resourcePath = "/schemaTestTopology.json";
    InputStream is = this.getClass().getResourceAsStream(resourcePath);
    if (is == null) {
      fail("Could not load " + resourcePath);
    }
    StringWriter writer = new StringWriter();

    IOUtils.copy(is, writer);
    JSONObject json = new JSONObject(writer.toString());

    //removing schema so that validation fails
    json.getJSONArray("streams").getJSONObject(0).remove("schema");
    Configuration conf = new Configuration(false);

    LogicalPlanConfiguration planConf = new LogicalPlanConfiguration(conf);
    LogicalPlan dag = planConf.createFromJson(json, "testLoadFromJson");

    dag.validate();
  }

  @Test
  public void testTestTupleClassAttrSetFromConfig()
  {
    Configuration conf = new Configuration(false);
    conf.set(StreamingApplication.APEX_PREFIX + "operator.o2.port.schemaRequiredPort.attr.TUPLE_CLASS",
        "com.datatorrent.stram.plan.logical.LogicalPlanConfigurationTest$TestSchema");

    StreamingApplication streamingApplication = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        TestGeneratorInputOperator o1 = dag.addOperator("o1", new TestGeneratorInputOperator());
        SchemaTestOperator o2 = dag.addOperator("o2", new SchemaTestOperator());
        dag.addStream("stream", o1.outport, o2.schemaRequiredPort);
      }
    };
    LogicalPlan dag = new LogicalPlan();
    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(conf);
    lpc.prepareDAG(dag, streamingApplication, "app");
    dag.validate();
  }

  /*
   * This test and all of the following ambiguous attribute tests verify that when an ambiguous attribute
   * name is provided, all the corresponding attributes are set.
   * <br/><br/>
   * <b>Note:</b> Ambiguous attribute means that when multiple attributes with the same
   * simple name exist for multiple types of dag elements (like operators and ports).
   * An example of such attributes are the com.datatorrent.api.Context.OperatorContext.AUTO_RECORD
   * and com.datatorrent.api.Context.PortContext.AUTO_RECORD.
   * <br/><br/>
   * This test should set the attribute on the operators and ports.
   */
  /**
   * This test checks if an ambiguous DAG attribute does not get set on operators.
   */
  @Test
  public void testDagAttributeNotSetOnOperator()
  {
    dagOperatorAttributeHelper(true);
  }

  @Test
  public void testAmbiguousAttributeSetOnOperatorAndNotDAG()
  {
    dagOperatorAttributeHelper(false);
  }

  private void dagOperatorAttributeHelper(boolean attrOnDag)
  {
    String attributeName = null;

    if (attrOnDag) {
      attributeName = DAGContext.CHECKPOINT_WINDOW_COUNT.getSimpleName();
    } else {
      attributeName = OperatorContext.class.getCanonicalName() + LogicalPlanConfiguration.KEY_SEPARATOR + DAGContext.CHECKPOINT_WINDOW_COUNT.getSimpleName();
    }

    Properties props = new Properties();
    String propName = StreamingApplication.APEX_PREFIX + StramElement.ATTR.getValue() + LogicalPlanConfiguration.KEY_SEPARATOR + attributeName;
    props.put(propName, "5");

    SimpleTestApplicationWithName app = new SimpleTestApplicationWithName();

    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    dagBuilder.addFromProperties(props, null);

    String appPath = app.getClass().getName().replace(".", "/") + ".class";

    LogicalPlan dag = new LogicalPlan();
    dagBuilder.prepareDAG(dag, app, appPath);

    OperatorMeta om1 = dag.getOperatorMeta("operator1");

    if (attrOnDag) {
      Assert.assertNotEquals((Integer)5, om1.getValue(OperatorContext.CHECKPOINT_WINDOW_COUNT));
    } else {
      Assert.assertEquals((Integer)5, om1.getValue(OperatorContext.CHECKPOINT_WINDOW_COUNT));
    }
  }

  /**
   * This test should set the attribute on the operators and ports.
   */
  @Test
  public void testRootLevelAmbiguousAttributeSimple()
  {
    testAttributeAmbiguousSimpleHelper(Context.OperatorContext.AUTO_RECORD, Context.PortContext.AUTO_RECORD,
        StreamingApplication.APEX_PREFIX, null, Boolean.TRUE, true, true);
  }

  /**
   * This test should set the attribute on the operators and ports.
   */
  @Test
  public void testApplicationLevelAmbiguousAttributeSimple()
  {
    testAttributeAmbiguousSimpleHelper(Context.OperatorContext.AUTO_RECORD, Context.PortContext.AUTO_RECORD,
        StreamingApplication.APEX_PREFIX + "application" + LogicalPlanConfiguration.KEY_SEPARATOR + "*" + LogicalPlanConfiguration.KEY_SEPARATOR, null, Boolean.TRUE, true, true);
  }

  /**
   * This should only set the attribute on the operator
   */
  @Test
  public void testOperatorLevelAmbiguousAttributeSimple()
  {
    testAttributeAmbiguousSimpleHelper(Context.OperatorContext.AUTO_RECORD, Context.PortContext.AUTO_RECORD,
        StreamingApplication.APEX_PREFIX + "operator" + LogicalPlanConfiguration.KEY_SEPARATOR + "*" + LogicalPlanConfiguration.KEY_SEPARATOR, null, Boolean.TRUE, true, false);
  }

  /**
   * This should only set the attribute on the port
   */
  @Test
  public void testPortLevelAmbiguousAttributeSimple()
  {
    testAttributeAmbiguousSimpleHelper(Context.OperatorContext.AUTO_RECORD, Context.PortContext.AUTO_RECORD,
        StreamingApplication.APEX_PREFIX + "port" + LogicalPlanConfiguration.KEY_SEPARATOR + "*" + LogicalPlanConfiguration.KEY_SEPARATOR, null, Boolean.TRUE, false, true);
  }

  /**
   * This test should set the attribute on the operators and ports.
   */
  @Test
  public void testRootLevelAmbiguousAttributeComplex()
  {
    testAttributeAmbiguousSimpleHelper(Context.OperatorContext.AUTO_RECORD, Context.PortContext.AUTO_RECORD, StreamingApplication.APEX_PREFIX,
        PortContext.class.getCanonicalName(), Boolean.TRUE, false, true);
  }

  /**
   * This test should set the attribute on the operators and ports.
   */
  @Test
  public void testApplicationLevelAmbiguousAttributeComplex()
  {
    testAttributeAmbiguousSimpleHelper(Context.OperatorContext.AUTO_RECORD, Context.PortContext.AUTO_RECORD,
        StreamingApplication.APEX_PREFIX + "application" + LogicalPlanConfiguration.KEY_SEPARATOR + "*" + LogicalPlanConfiguration.KEY_SEPARATOR, PortContext.class.getCanonicalName(),
        Boolean.TRUE, false, true);
  }

  /**
   * This should only set the attribute on the operator
   */
  @Test
  public void testOperatorLevelAmbiguousAttributeComplex()
  {
    testAttributeAmbiguousSimpleHelper(Context.OperatorContext.AUTO_RECORD, Context.PortContext.AUTO_RECORD,
        StreamingApplication.APEX_PREFIX + "operator" + LogicalPlanConfiguration.KEY_SEPARATOR + "*" + LogicalPlanConfiguration.KEY_SEPARATOR, OperatorContext.class.getCanonicalName(),
        Boolean.TRUE, true, false);
  }

  /**
   * This should only set the attribute on the port
   */
  @Test
  public void testOperatorLevelAmbiguousAttributeComplex2()
  {
    testAttributeAmbiguousSimpleHelper(Context.OperatorContext.AUTO_RECORD, Context.PortContext.AUTO_RECORD,
        StreamingApplication.APEX_PREFIX + "operator" + LogicalPlanConfiguration.KEY_SEPARATOR + "*" + LogicalPlanConfiguration.KEY_SEPARATOR, PortContext.class.getCanonicalName(),
        Boolean.TRUE, false, true);
  }

  /**
   * This should only set the attribute on the port
   */
  @Test
  public void testPortLevelAmbiguousAttributeComplex()
  {
    testAttributeAmbiguousSimpleHelper(Context.OperatorContext.AUTO_RECORD, Context.PortContext.AUTO_RECORD,
        StreamingApplication.APEX_PREFIX + "port" + LogicalPlanConfiguration.KEY_SEPARATOR + "*" + LogicalPlanConfiguration.KEY_SEPARATOR, PortContext.class.getCanonicalName(),
        Boolean.TRUE, false, true);
  }

  private void testAttributeAmbiguousSimpleHelper(Attribute<?> attributeObjOperator, Attribute<?> attributeObjPort,
      String root, String contextClass, Object val, boolean operatorSet,
      boolean portSet)
  {
    Properties props = propertiesBuilder(attributeObjOperator.getSimpleName(), root, contextClass, val);

    simpleAttributeOperatorHelperAssert(attributeObjOperator, props, val, operatorSet);

    simpleNamePortAssertHelperAssert(attributeObjPort, props, val, portSet);
  }

  @Test
  public void testRootLevelAttributeSimpleNameOperator()
  {
    simpleAttributeOperatorHelper(OperatorContext.MEMORY_MB, StreamingApplication.APEX_PREFIX, true, 4096, true, true);
  }

  @Test
  public void testRootLevelStorageAgentSimpleNameOperator()
  {
    MockStorageAgent mockAgent = new MockStorageAgent();

    simpleAttributeOperatorHelper(OperatorContext.STORAGE_AGENT, StreamingApplication.APEX_PREFIX, true, mockAgent, true, false);
  }

  @Test
  public void testRootLevelAttributeSimpleNameOperatorNoScope()
  {
    simpleAttributeOperatorHelper(OperatorContext.MEMORY_MB, StreamingApplication.APEX_PREFIX, true, 4096, true, false);
  }

  @Test
  public void testApplicationLevelAttributeSimpleNameOperator()
  {
    simpleAttributeOperatorHelper(OperatorContext.MEMORY_MB, StreamingApplication.APEX_PREFIX + "application" + LogicalPlanConfiguration.KEY_SEPARATOR + "SimpleTestApp" + LogicalPlanConfiguration.KEY_SEPARATOR,
        true, 4096, true, true);
  }

  private void simpleAttributeOperatorHelper(Attribute<?> attributeObj, String root, boolean simpleName,
      Object val, boolean set, boolean scope)
  {
    Properties props = propertiesBuilderOperator(attributeObj.getSimpleName(), root, simpleName,
        val, scope);

    simpleAttributeOperatorHelperAssert(attributeObj, props, val, set);
  }

  private void simpleAttributeOperatorHelperAssert(Attribute<?> attributeObj, Properties props, Object val, boolean set)
  {
    SimpleTestApplicationWithName app = new SimpleTestApplicationWithName();

    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    dagBuilder.addFromProperties(props, null);

    String appPath = app.getClass().getName().replace(".", "/") + ".class";

    LogicalPlan dag = new LogicalPlan();
    dagBuilder.prepareDAG(dag, app, appPath);

    OperatorMeta om1 = dag.getOperatorMeta("operator1");

    if (set) {
      Assert.assertEquals(val, om1.getValue(attributeObj));
    } else {
      Assert.assertNotEquals(val, om1.getValue(attributeObj));
    }

    OperatorMeta om2 = dag.getOperatorMeta("operator2");

    if (set) {
      Assert.assertEquals(val, om2.getValue(attributeObj));
    } else {
      Assert.assertNotEquals(val, om2.getValue(attributeObj));
    }

    OperatorMeta om3 = dag.getOperatorMeta("operator3");

    if (set) {
      Assert.assertEquals(val, om3.getValue(attributeObj));
    } else {
      Assert.assertNotEquals(val, om3.getValue(attributeObj));
    }
  }

  /* Port tests */
  @Test
  public void testRootLevelAttributeSimpleNamePort()
  {
    simpleAttributePortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX,
        true, 4096, true, true);
  }

  @Test
  public void testRootLevelAttributeSimpleNamePortNoScope()
  {
    simpleAttributePortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX,
        true, 4096, true, false);
  }

  @Test
  public void testOperatorLevelAttributeSimpleNamePort()
  {
    simpleAttributePortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX + "operator" + LogicalPlanConfiguration.KEY_SEPARATOR + "*" + LogicalPlanConfiguration.KEY_SEPARATOR,
        true, 4096, true, true);
  }

  @Test
  public void testApplicationLevelAttributeSimpleNamePort()
  {
    simpleAttributePortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX + "application" + LogicalPlanConfiguration.KEY_SEPARATOR + "SimpleTestApp" + LogicalPlanConfiguration.KEY_SEPARATOR,
        true, 4096, true, true);
  }

  @Test
  public void testRootLevelAttributeComplexNamePort()
  {
    simpleAttributePortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX, false,
        4096, true, true);
  }

  @Test
  public void testRootLevelAttributeComplexNamePortNoScope()
  {
    simpleAttributePortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX, false,
        4096, true, false);
  }

  @Test
  public void testOperatorLevelAttributeComplexNamePort()
  {
    simpleAttributePortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX + "operator" + LogicalPlanConfiguration.KEY_SEPARATOR + "*" + LogicalPlanConfiguration.KEY_SEPARATOR,
        false, 4096, true, true);
  }

  @Test
  public void testApplicationLevelAttributeComplexNamePort()
  {
    simpleAttributePortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX + "application" + LogicalPlanConfiguration.KEY_SEPARATOR + "SimpleTestApp" + LogicalPlanConfiguration.KEY_SEPARATOR,
        false, 4096, true, true);
  }

  /* Input port tests */
  @Test
  public void testRootLevelAttributeSimpleNameInputPort()
  {
    simpleAttributeInputPortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX, true,
        4096, true);
  }

  @Test
  public void testOperatorLevelAttributeSimpleNameInputPort()
  {
    simpleAttributeInputPortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX + "operator" + LogicalPlanConfiguration.KEY_SEPARATOR + "*" + LogicalPlanConfiguration.KEY_SEPARATOR, true, 4096, true);
  }

  @Test
  public void testApplicationLevelAttributeSimpleNameInputPort()
  {
    simpleAttributeInputPortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX + "application" + LogicalPlanConfiguration.KEY_SEPARATOR + "SimpleTestApp" + LogicalPlanConfiguration.KEY_SEPARATOR,
        true, 4096, true);
  }

  @Test
  public void testRootLevelAttributeComplexNameInputPort()
  {
    simpleAttributeInputPortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX, false, 4096, true);
  }

  @Test
  public void testOperatorLevelAttributeComplexNameInputPort()
  {
    simpleAttributeInputPortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX + "operator" + LogicalPlanConfiguration.KEY_SEPARATOR + "*" + LogicalPlanConfiguration.KEY_SEPARATOR, false,
        4096, true);
  }

  @Test
  public void testApplicationLevelAttributeComplexNameInputPort()
  {
    simpleAttributeInputPortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX + "application" + LogicalPlanConfiguration.KEY_SEPARATOR + "SimpleTestApp" + LogicalPlanConfiguration.KEY_SEPARATOR,
        false, 4096, true);
  }

  /* Output port tests */
  @Test
  public void testRootLevelAttributeSimpleNameOutputPort()
  {
    simpleAttributeOutputPortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX, true, 4096, true);
  }

  @Test
  public void testOperatorLevelAttributeSimpleNameOutputPort()
  {
    simpleAttributeOutputPortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX + "operator" + LogicalPlanConfiguration.KEY_SEPARATOR + "*" + LogicalPlanConfiguration.KEY_SEPARATOR, true, 4096, true);
  }

  @Test
  public void testApplicationLevelAttributeSimpleNameOutputPort()
  {
    simpleAttributeOutputPortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX + "application" + LogicalPlanConfiguration.KEY_SEPARATOR +
        "SimpleTestApp" + LogicalPlanConfiguration.KEY_SEPARATOR, true, 4096, true);
  }

  @Test
  public void testRootLevelAttributeComplexNameOutputPort()
  {
    simpleAttributeOutputPortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX, false, 4096, true);
  }

  @Test
  public void testOperatorLevelAttributeComplexNameOutputPort()
  {
    simpleAttributeOutputPortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX + "operator" + LogicalPlanConfiguration.KEY_SEPARATOR + "*" + LogicalPlanConfiguration.KEY_SEPARATOR, false, 4096, true);
  }

  @Test
  public void testApplicationLevelAttributeComplexNameOutputPort()
  {
    simpleAttributeOutputPortHelper(PortContext.QUEUE_CAPACITY, StreamingApplication.APEX_PREFIX + "application" + LogicalPlanConfiguration.KEY_SEPARATOR +
        "SimpleTestApp" + LogicalPlanConfiguration.KEY_SEPARATOR, false, 4096, true);
  }

  /* Helpers for building ports */
  private void simpleAttributePortHelper(Attribute<?> attributeObj, String root, boolean simpleName, Object val, boolean set, boolean scope)
  {
    Properties props = propertiesBuilderPort(attributeObj.getSimpleName(), root, simpleName, val, scope);

    simpleNamePortAssertHelperAssert(attributeObj, props, val, set);
  }

  private void simpleAttributeInputPortHelper(Attribute<?> attributeObj, String root, boolean simpleName, Object val, boolean set)
  {
    Properties props = propertiesBuilderInputPort(attributeObj.getSimpleName(), root, simpleName, val);

    simpleNameInputPortAssertHelperAssert(attributeObj, props, val, set);
    simpleNameOutputPortAssertHelperAssert(attributeObj, props, val, !set);
  }

  private void simpleAttributeOutputPortHelper(Attribute<?> attributeObj, String root, boolean simpleName, Object val, boolean set)
  {
    Properties props = propertiesBuilderOutputPort(attributeObj.getSimpleName(), root, simpleName, val);

    simpleNameOutputPortAssertHelperAssert(attributeObj, props, val, set);
    simpleNameInputPortAssertHelperAssert(attributeObj, props, val, !set);
  }

  private void simpleNamePortAssertHelperAssert(Attribute<?> attributeObj, Properties props, Object val, boolean set)
  {
    SimpleTestApplicationWithName app = new SimpleTestApplicationWithName();

    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    dagBuilder.addFromProperties(props, null);

    String appPath = app.getClass().getName().replace(".", "/") + ".class";

    LogicalPlan dag = new LogicalPlan();
    dagBuilder.prepareDAG(dag, app, appPath);

    simpleNamePortAssertHelper(attributeObj, dag, "operator1", val, set);
    simpleNamePortAssertHelper(attributeObj, dag, "operator2", val, set);
    simpleNamePortAssertHelper(attributeObj, dag, "operator3", val, set);
  }

  private void simpleNameInputPortAssertHelperAssert(Attribute<?> attributeObj, Properties props, Object val, boolean set)
  {
    SimpleTestApplicationWithName app = new SimpleTestApplicationWithName();

    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    dagBuilder.addFromProperties(props, null);

    String appPath = app.getClass().getName().replace(".", "/") + ".class";

    LogicalPlan dag = new LogicalPlan();
    dagBuilder.prepareDAG(dag, app, appPath);

    simpleNameInputPortAssertHelper(attributeObj, dag, "operator1", val, set);
    simpleNameInputPortAssertHelper(attributeObj, dag, "operator2", val, set);
    simpleNameInputPortAssertHelper(attributeObj, dag, "operator3", val, set);
  }

  private void simpleNameOutputPortAssertHelperAssert(Attribute<?> attributeObj, Properties props, Object val, boolean set)
  {
    SimpleTestApplicationWithName app = new SimpleTestApplicationWithName();

    LogicalPlanConfiguration dagBuilder = new LogicalPlanConfiguration(new Configuration(false));
    dagBuilder.addFromProperties(props, null);

    String appPath = app.getClass().getName().replace(".", "/") + ".class";

    LogicalPlan dag = new LogicalPlan();
    dagBuilder.prepareDAG(dag, app, appPath);

    simpleNameOutputPortAssertHelper(attributeObj, dag, "operator1", val, set);
    simpleNameOutputPortAssertHelper(attributeObj, dag, "operator2", val, set);
    simpleNameOutputPortAssertHelper(attributeObj, dag, "operator3", val, set);
  }

  private void simpleNamePortAssertHelper(Attribute<?> attributeObj, LogicalPlan dag, String operatorName, Object queueCapacity, boolean set)
  {
    simpleNameInputPortAssertHelper(attributeObj, dag, operatorName, queueCapacity, set);
    simpleNameOutputPortAssertHelper(attributeObj, dag, operatorName, queueCapacity, set);
  }

  private void simpleNameInputPortAssertHelper(Attribute<?> attributeObj, LogicalPlan dag, String operatorName, Object queueCapacity, boolean set)
  {
    OperatorMeta operatorMeta = dag.getOperatorMeta(operatorName);

    for (InputPortMeta inputPortMeta: operatorMeta.getInputStreams().keySet()) {
      if (set) {
        Assert.assertEquals(queueCapacity, inputPortMeta.getValue(attributeObj));
      } else {
        Assert.assertNotEquals(queueCapacity, inputPortMeta.getValue(attributeObj));
      }
    }
  }

  private void simpleNameOutputPortAssertHelper(Attribute<?> attributeObj, LogicalPlan dag, String operatorName, Object queueCapacity, boolean set)
  {
    OperatorMeta operatorMeta = dag.getOperatorMeta(operatorName);

    for (OutputPortMeta outputPortMeta: operatorMeta.getOutputStreams().keySet()) {
      if (set) {
        Assert.assertEquals(queueCapacity, outputPortMeta.getValue(attributeObj));
      } else {
        Assert.assertNotEquals(queueCapacity, outputPortMeta.getValue(attributeObj));
      }
    }
  }

  /* Helpers for building properties */
  private Properties propertiesBuilder(String attributeName, String root, String contextClass, Object val)
  {
    boolean simpleName = contextClass == null;

    if (!simpleName) {
      attributeName = contextClass + LogicalPlanConfiguration.KEY_SEPARATOR + attributeName;
    }

    Properties props = new Properties();
    String propName = root + StramElement.ATTR.getValue() + LogicalPlanConfiguration.KEY_SEPARATOR + attributeName;
    props.put(propName, val.toString());

    return props;
  }

  private Properties propertiesBuilderOperator(String attributeName, String root, boolean simpleName, Object val, boolean addOperator)
  {
    String contextClass = simpleName ? null : OperatorContext.class.getCanonicalName();

    if (addOperator) {
      root += "operator" + LogicalPlanConfiguration.KEY_SEPARATOR + "*" + LogicalPlanConfiguration.KEY_SEPARATOR;
    }

    return propertiesBuilder(attributeName, root, contextClass, val);
  }

  private Properties propertiesBuilderPort(String attributeName, String root, boolean simpleName, Object val, boolean addPort)
  {
    String contextClass = simpleName ? null : PortContext.class.getCanonicalName();

    if (addPort) {
      root += "port" + LogicalPlanConfiguration.KEY_SEPARATOR + "*" + LogicalPlanConfiguration.KEY_SEPARATOR;
    }

    return propertiesBuilder(attributeName, root, contextClass, val);
  }

  private Properties propertiesBuilderInputPort(String attributeName, String root, boolean simpleName, Object val)
  {
    String contextClass = simpleName ? null : PortContext.class.getCanonicalName();

    root += "inputport" + LogicalPlanConfiguration.KEY_SEPARATOR + "*" + LogicalPlanConfiguration.KEY_SEPARATOR;

    return propertiesBuilder(attributeName, root, contextClass, val);
  }

  private Properties propertiesBuilderOutputPort(String attributeName, String root, boolean simpleName, Object val)
  {
    String contextClass = simpleName ? null : PortContext.class.getCanonicalName();

    root += "outputport" + LogicalPlanConfiguration.KEY_SEPARATOR + "*" + LogicalPlanConfiguration.KEY_SEPARATOR;

    return propertiesBuilder(attributeName, root, contextClass, val);
  }

  /**
   * Note: If the same name is given to an Attribute specified in multiple Context classes, then the type of that
   * Attribute is required to be the same accross all Context classes. This is required because if a simple attribute
   * name is specified in a properties file at the top level context then that attribute needs to be set in all child configurations. If
   * there were multiple Attributes specified in different Contexts with the same name, but a different type, then
   * it would not be possible to set the values of Attributes specified by a simple attribute name in the root
   * context of a properties file. If this were the case, then adding another Attribute with the same name as a pre-existing Attribute to a new Context
   * class would be a backwards incompatible change.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testErrorSameAttrMultipleTypes()
  {
    //Trigger initialization of attributes for existing Contexts.
    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(new Configuration());

    Exception ex = null;
    try {
      ContextUtils.buildAttributeMaps(Sets.newHashSet(MockContext1.class, MockContext2.class));
    } catch (ValidationException e) {
      ex = e;
    }

    Assert.assertNotNull(ex);
    Assert.assertTrue(ex.getMessage().contains("is defined with two different types in two different context classes:"));

    //Clear test data from Context.
    ContextUtils.initialize();
  }

  private static final Logger logger = LoggerFactory.getLogger(LogicalPlanConfigurationTest.class);

  public static class TestApplication implements StreamingApplication
  {
    Integer testprop1;
    Integer testprop2;
    Integer testprop3;
    TestInnerClass inncls;

    public TestApplication()
    {
      inncls = new TestInnerClass();
    }

    public Integer getTestprop1()
    {
      return testprop1;
    }

    public void setTestprop1(Integer testprop1)
    {
      this.testprop1 = testprop1;
    }

    public Integer getTestprop2()
    {
      return testprop2;
    }

    public void setTestprop2(Integer testprop2)
    {
      this.testprop2 = testprop2;
    }

    public Integer getTestprop3()
    {
      return testprop3;
    }

    public void setTestprop3(Integer testprop3)
    {
      this.testprop3 = testprop3;
    }

    public TestInnerClass getInncls()
    {
      return inncls;
    }

    public void setInncls(TestInnerClass inncls)
    {
      this.inncls = inncls;
    }

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {

    }

    public class TestInnerClass
    {
      Integer a;

      public Integer getA()
      {
        return a;
      }

      public void setA(Integer a)
      {
        this.a = a;
      }
    }
  }

  public static class TestStatsListener implements StatsListener
  {

    private int intProp;

    public TestStatsListener()
    {
    }

    @Override
    public Response processStats(BatchedOperatorStats stats)
    {
      return null;
    }

    public int getIntProp()
    {
      return intProp;
    }

    public void setIntProp(int intProp)
    {
      this.intProp = intProp;
    }

    @Override
    public int hashCode()
    {
      final int prime = 31;
      int result = 1;
      result = prime * result + intProp;
      return result;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      TestStatsListener other = (TestStatsListener)obj;
      if (intProp != other.intProp) {
        return false;
      }
      return true;
    }
  }

  public static class TestSchema
  {
  }

  public static class SimpleTestApplication implements StreamingApplication
  {
    public final GenericTestOperator gt1 = new GenericTestOperator();
    public final GenericTestOperator gt2 = new GenericTestOperator();
    public final GenericTestOperator gt3 = new GenericTestOperator();

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      dag.addOperator("operator1", gt1);
      dag.addOperator("operator2", gt2);
      dag.addOperator("operator3", gt3);
      dag.addStream("s1", gt1.outport1, gt2.inport1);
      dag.addStream("s2", gt2.outport1, gt3.inport1, gt3.inport2);
    }
  }

  public interface MockContext1 extends Context
  {
    /**
     * Number of tuples the poll buffer can cache without blocking the input stream to the port.
     */
    Attribute<Integer> TEST_ATTR = new Attribute<>(1024);

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    long serialVersionUID = AttributeMap.AttributeInitializer.initialize(MockContext1.class);
  }

  public interface MockContext2 extends Context
  {
    /**
     * Number of tuples the poll buffer can cache without blocking the input stream to the port.
     */
    Attribute<Boolean> TEST_ATTR = new Attribute<>(false);

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    long serialVersionUID = AttributeMap.AttributeInitializer.initialize(MockContext2.class);
  }

  @ApplicationAnnotation(name = "SimpleTestApp")
  public static class SimpleTestApplicationWithName extends SimpleTestApplication
  {
  }

  private static final Logger LOG = LoggerFactory.getLogger(LogicalPlanConfigurationTest.class);
}

