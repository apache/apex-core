/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.google.common.collect.Sets;
import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.Context.PortContext;
import com.malhartech.api.DAG;
import com.malhartech.api.DAG.OperatorMeta;
import com.malhartech.api.DAG.StreamMeta;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator;
import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.engine.GenericTestOperator;
import com.malhartech.engine.TestGeneratorInputOperator;
import com.malhartech.engine.TestOutputOperator;
import com.malhartech.stram.cli.StramClientUtils;
import com.malhartech.codec.KryoJdkSerializer;

public class DAGBuilderTest {

  public static OperatorMeta assertNode(DAG dag, String id) {
      OperatorMeta n = dag.getOperatorMeta(id);
      assertNotNull("operator exists id=" + id, n);
      return n;
  }

  /**
   * Test read from stram-site.xml in Hadoop configuration format.
   */
  @Test
  public void testLoadFromConfigXml() {
    Configuration conf = StramClientUtils.addStramResources(new Configuration());
    //Configuration.dumpConfiguration(conf, new PrintWriter(System.out));

    DAGPropertiesBuilder builder = new DAGPropertiesBuilder();
    builder.addFromConfiguration(conf);

    DAG dag = builder.getApplication(new Configuration(false));
    dag.validate();

//    Map<String, NodeConf> operatorConfs = tb.getAllOperators();
    assertEquals("number of operator confs", 6, dag.getAllOperators().size());

    OperatorMeta operator1 = assertNode(dag, "operator1");
    OperatorMeta operator2 = assertNode(dag, "operator2");
    OperatorMeta operator3 = assertNode(dag, "operator3");
    OperatorMeta operator4 = assertNode(dag, "operator4");

    assertNotNull("operatorConf for root", operator1);
    assertEquals("operatorId set", "operator1", operator1.getId());

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
    Assert.assertFalse("n1n2 not inline (default)", n1n2.isInline());

    // operator 2 streams to operator 3 and operator 4
    assertEquals("operator 2 number of outputs", 1, operator2.getOutputStreams().size());
    StreamMeta fromNode2 = operator2.getOutputStreams().values().iterator().next();

    Set<OperatorMeta> targetNodes = new HashSet<OperatorMeta>();
    for (DAG.InputPortMeta ip : fromNode2.getSinks()) {
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

  public void printTopology(OperatorMeta operator, DAG tplg, int level) {
      String prefix = "";
      if (level > 0) {
        prefix = StringUtils.repeat(" ", 20*(level-1)) + "   |" + StringUtils.repeat("-", 17);
      }
      System.out.println(prefix + operator.getId());
      for (StreamMeta downStream : operator.getOutputStreams().values()) {
          if (!downStream.getSinks().isEmpty()) {
            for (DAG.InputPortMeta targetNode : downStream.getSinks()) {
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

      DAG dag = pb.getApplication(new Configuration(false));
      dag.validate();

      assertEquals("number of operator confs", 5, dag.getAllOperators().size());
      assertEquals("number of root operators", 1, dag.getRootOperators().size());

      StreamMeta s1 = dag.getStream("n1n2");
      assertNotNull(s1);
      assertTrue("n1n2 inline", s1.isInline());

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
      for (DAG.InputPortMeta targetPort : input1.getSinks()) {
        targetNodes.add(targetPort.getOperatorWrapper());
      }

      Assert.assertEquals("input1 target ", Sets.newHashSet(dag.getOperatorMeta("operator1"), operator3, operator4), targetNodes);

  }

  @Test
  public void testCycleDetection() {
     DAG dag = new DAG();

     //NodeConf operator1 = b.getOrAddNode("operator1");
     GenericTestOperator operator2 = dag.addOperator("operator2", GenericTestOperator.class);
     GenericTestOperator operator3 = dag.addOperator("operator3", GenericTestOperator.class);
     GenericTestOperator operator4 = dag.addOperator("operator4", GenericTestOperator.class);
     //NodeConf operator5 = b.getOrAddNode("operator5");
     //NodeConf operator6 = b.getOrAddNode("operator6");
     GenericTestOperator operator7 = dag.addOperator("operator7", GenericTestOperator.class);

     // strongly connect n2-n3-n4-n2
     dag.addStream("n2n3", operator2.outport1, operator3.inport1);

     dag.addStream("n3n4", operator3.outport1, operator4.inport1);

     dag.addStream("n4n2", operator4.outport1, operator2.inport1);

     // self referencing operator cycle
     StreamMeta n7n7 = dag.addStream("n7n7", operator7.outport1, operator7.inport1);
     try {
       n7n7.addSink(operator7.inport1);
       fail("cannot add to stream again");
     } catch (Exception e) {
       // expected, stream can have single input/output only
     }

     List<List<String>> cycles = new ArrayList<List<String>>();
     dag.findStronglyConnected(dag.getMeta(operator7), cycles);
     assertEquals("operator self reference", 1, cycles.size());
     assertEquals("operator self reference", 1, cycles.get(0).size());
     assertEquals("operator self reference", operator7.getName(), cycles.get(0).get(0));

     // 3 operator cycle
     cycles.clear();
     dag.findStronglyConnected(dag.getMeta(operator4), cycles);
     assertEquals("3 operator cycle", 1, cycles.size());
     assertEquals("3 operator cycle", 3, cycles.get(0).size());
     assertTrue("operator2", cycles.get(0).contains(operator2.getName()));
     assertTrue("operator3", cycles.get(0).contains(operator3.getName()));
     assertTrue("operator4", cycles.get(0).contains(operator4.getName()));

     try {
       dag.validate();
       fail("validation should fail");
     } catch (IllegalStateException e) {
       // expected
     }

  }

  static class ValidationOperator extends BaseOperator {
    @OutputPortFieldAnnotation(name="goodOutputPort")
    final public transient DefaultOutputPort<Object> goodOutputPort = new DefaultOutputPort<Object>(this);

    @OutputPortFieldAnnotation(name="badOutputPort")
    final public transient DefaultOutputPort<Object> badOutputPort = new DefaultOutputPort<Object>(this);
  }

  static class CounterOperator extends BaseOperator {
    @InputPortFieldAnnotation(name="countInputPort")
    final public transient InputPort<Object> countInputPort = new DefaultInputPort<Object>(this) {
      @Override
      final public void process(Object payload) {
      }
    };
  }

  @Test
  public void testJavaBuilder() throws Exception {

    DAG dag = new DAG();

    ValidationOperator validationNode = dag.addOperator("validationNode", ValidationOperator.class);
    CounterOperator countGoodNode = dag.addOperator("countGoodNode", CounterOperator.class);
    CounterOperator countBadNode = dag.addOperator("countBadNode", CounterOperator.class);
    //ConsoleOutputOperator echoBadNode = dag.addOperator("echoBadNode", ConsoleOutputOperator.class);

    // good tuples to counter operator
    dag.addStream("goodTuplesStream", validationNode.goodOutputPort, countGoodNode.countInputPort);

    // bad tuples to separate stream and echo operator
    // (stream with 2 outputs)
    dag.addStream("badTuplesStream", validationNode.badOutputPort, countBadNode.countInputPort);

    Assert.assertEquals("number root operators", 1, dag.getRootOperators().size());
    Assert.assertEquals("root operator id", "validationNode", dag.getRootOperators().get(0).getId());

    dag.getContextAttributes(countGoodNode).attr(OperatorContext.SPIN_MILLIS).set(10);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DAG.write(dag, bos);

    System.out.println("serialized size: " + bos.toByteArray().length);

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DAG dagClone = DAG.read(bis);
    Assert.assertNotNull(dagClone);
    Assert.assertEquals("number operators in clone", dag.getAllOperators().size(), dagClone.getAllOperators().size());
    Assert.assertEquals("number root operators in clone", 1, dagClone.getRootOperators().size());
    Assert.assertTrue("root operator in operators", dagClone.getAllOperators().contains(dagClone.getRootOperators().get(0)));


    Operator countGoodNodeClone = dagClone.getOperatorMeta("countGoodNode").getOperator();
    Assert.assertEquals("", new Integer(10), dagClone.getContextAttributes(countGoodNodeClone).attr(OperatorContext.SPIN_MILLIS).get());

  }

  public static class ValidationTestOperator extends BaseOperator {
    @NotNull
    @Pattern(regexp=".*malhar.*", message="Value has to contain 'malhar'!")
    private String stringField1;

    @Min(2)
    private int intField1;

    @AssertTrue(message="stringField1 should end with intField1")
    private boolean isValidConfiguration() {
      return stringField1.endsWith(String.valueOf(intField1));
    }

    private String getterProperty2 = "";

    @NotNull
    public String getProperty2() {
      return getterProperty2;
    }

    public void setProperty2(String s) {
      // annotations need to be on the getter
      getterProperty2 = s;
    }

    private String[] stringArrayField;

    public String[] getStringArrayField() {
      return stringArrayField;
    }

    public void setStringArrayField(String[] stringArrayField) {
      this.stringArrayField = stringArrayField;
    }

    public class Nested {
      @NotNull
      private String property = "";

      public String getProperty() {
        return property;
      }

      public void setProperty(String property) {
        this.property = property;
      }

    }

    @Valid
    private final Nested nestedBean = new Nested();

    private String stringProperty2;

    public String getStringProperty2() {
      return stringProperty2;
    }

    public void setStringProperty2(String stringProperty2) {
      this.stringProperty2 = stringProperty2;
    }

  }

  @Test
  public void testOperatorValidation() {

    ValidationTestOperator bean = new ValidationTestOperator();
    bean.stringField1 = "malhar1";
    bean.intField1 = 1;

    // ensure validation standalone produces expected results
    ValidatorFactory factory =
        Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();
    Set<ConstraintViolation<ValidationTestOperator>> constraintViolations =
             validator.validate(bean);
    //for (ConstraintViolation<ValidationTestOperator> cv : constraintViolations) {
    //  System.out.println("validation error: " + cv);
    //}
    Assert.assertEquals("" + constraintViolations,1, constraintViolations.size());
    ConstraintViolation<ValidationTestOperator> cv = constraintViolations.iterator().next();
    Assert.assertEquals("", bean.intField1, cv.getInvalidValue());
    Assert.assertEquals("", "intField1", cv.getPropertyPath().toString());

    // ensure DAG validation produces matching results
    DAG dag = new DAG();
    bean = dag.addOperator("testOperator", bean);

    try {
      dag.validate();
      Assert.fail("should throw ConstraintViolationException");
    } catch (ConstraintViolationException e) {
      Assert.assertEquals("", constraintViolations, e.getConstraintViolations());
    }

    try {
      bean.intField1 = 3;
      dag.validate();
      Assert.fail("should throw ConstraintViolationException");
    } catch (ConstraintViolationException e) {
      ConstraintViolation<?> cv2 = e.getConstraintViolations().iterator().next();
      Assert.assertEquals("" + e.getConstraintViolations(), 1, constraintViolations.size());
      Assert.assertEquals("", false, cv2.getInvalidValue());
      Assert.assertEquals("", "validConfiguration", cv2.getPropertyPath().toString());
    }
    bean.stringField1 = "malhar3";

    // annotated getter
    try {
      bean.getterProperty2 = null;
      dag.validate();
      Assert.fail("should throw ConstraintViolationException");
    } catch (ConstraintViolationException e) {
      ConstraintViolation<?> cv2 = e.getConstraintViolations().iterator().next();
      Assert.assertEquals("" + e.getConstraintViolations(), 1, constraintViolations.size());
      Assert.assertEquals("", null, cv2.getInvalidValue());
      Assert.assertEquals("", "property2", cv2.getPropertyPath().toString());
    }
    bean.getterProperty2 = "";

    // nested property
    try {
      bean.nestedBean.property = null;
      dag.validate();
      Assert.fail("should throw ConstraintViolationException");
    } catch (ConstraintViolationException e) {
      ConstraintViolation<?> cv2 = e.getConstraintViolations().iterator().next();
      Assert.assertEquals("" + e.getConstraintViolations(), 1, constraintViolations.size());
      Assert.assertEquals("", null, cv2.getInvalidValue());
      Assert.assertEquals("", "nestedBean.property", cv2.getPropertyPath().toString());
    }
    bean.nestedBean.property = "";

    // all valid
    dag.validate();

  }

  @Test
  public void testPortConnectionValidation() {

    DAG dag = new DAG();
    TestGeneratorInputOperator input = dag.addOperator("input1", TestGeneratorInputOperator.class);

    try {
    dag.validate();
    Assert.fail("should raise port not connected for o1.input1");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("", "Output port connection required: input1.outputPort", e.getMessage());
    }

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    dag.addStream("stream1", input.outport, o1.inport1);
    dag.validate();

  }

  private class TestAnnotationsOperator extends BaseOperator {
    @OutputPortFieldAnnotation(name="oport1")
    final public transient DefaultOutputPort<Object> outport1 = new DefaultOutputPort<Object>(this);

    @OutputPortFieldAnnotation(name="oport2", optional=false)
    final public transient DefaultOutputPort<Object> outport2 = new DefaultOutputPort<Object>(this);
  }

  private class TestAnnotationsOperator2 extends BaseOperator {
    // multiple ports w/o annotation, one of them must be connected
    final public transient DefaultOutputPort<Object> outport1 = new DefaultOutputPort<Object>(this);
    final public transient DefaultOutputPort<Object> outport2 = new DefaultOutputPort<Object>(this);
  }

  private class TestAnnotationsOperator3 extends BaseOperator {
    // multiple ports w/o annotation, one of them must be connected
    @OutputPortFieldAnnotation(name="oport1", optional=true)
    final public transient DefaultOutputPort<Object> outport1 = new DefaultOutputPort<Object>(this);
    @OutputPortFieldAnnotation(name="oport2", optional=true)
    final public transient DefaultOutputPort<Object> outport2 = new DefaultOutputPort<Object>(this);
  }

  @Test
  public void testOutputPortAnnotation() {
    DAG dag = new DAG();
    TestAnnotationsOperator ta1 = dag.addOperator("testAnnotationsOperator", new TestAnnotationsOperator());

    try {
      dag.validate();
      Assert.fail("should raise: port connection required");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("", "Output port connection required: testAnnotationsOperator.oport2", e.getMessage());
    }

    TestOutputOperator o2 = dag.addOperator("sink", new TestOutputOperator());
    dag.addStream("s1", ta1.outport2, o2.inport);

    dag.validate();

    TestAnnotationsOperator2 ta2 = dag.addOperator("multiOutputPorts1", new TestAnnotationsOperator2());

    try {
      dag.validate();
      Assert.fail("should raise: At least one output port must be connected");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("", "At least one output port must be connected: multiOutputPorts1", e.getMessage());
    }
    TestOutputOperator o3 = dag.addOperator("o3", new TestOutputOperator());
    dag.addStream("s2", ta2.outport1, o3.inport);

    TestAnnotationsOperator3 ta3 = dag.addOperator("multiOutputPorts3", new TestAnnotationsOperator3());
    dag.validate();

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

    DAG dag = new DAG();
    Operator operator1 = dag.addOperator("operator1", new ValidationTestOperator());
    Operator operator2 = dag.addOperator("operator2", new ValidationTestOperator());
    Operator operator3 = dag.addOperator("operator3", new GenericTestOperator());

    DAGPropertiesBuilder pb = new DAGPropertiesBuilder();
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

    DAG dag = new DAG();
    GenericTestOperator o1 = dag.addOperator("o1", new GenericTestOperator());
    ValidationTestOperator o2 = dag.addOperator("o2", new ValidationTestOperator());

    DAGPropertiesBuilder pb = new DAGPropertiesBuilder();
    pb.addFromConfiguration(conf);

    pb.setOperatorProperties(dag, "testSetOperatorProperties");
    Assert.assertEquals("o1.myStringProperty", "myStringPropertyValue", o1.getMyStringProperty());
    Assert.assertArrayEquals("o2.stringArrayField", new String[] {"a", "b", "c"}, o2.stringArrayField);
  }

  public class DuplicatePortOperator extends GenericTestOperator {
    @OutputPortFieldAnnotation(name=OPORT1)
    final public transient DefaultOutputPort<Object> outport1 = new DefaultOutputPort<Object>(this);
  }

  @Test
  public void testDuplicatePort() {
    DAG dag = new DAG();
    DuplicatePortOperator o1 = dag.addOperator("o1", new DuplicatePortOperator());
    try {
      dag.setOutputPortAttribute(o1.outport1, PortContext.QUEUE_CAPACITY, 0);
      Assert.fail("Should detect duplicate port");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  /**
   * Operator that can be used with default Java serialization instead of Kryo
   */
  @DefaultSerializer(KryoJdkSerializer.class)
  public static class JdkSerializableOperator extends BaseOperator implements Serializable {
    private static final long serialVersionUID = -4024202339520027097L;

    public abstract class SerializableInputPort<T> implements InputPort<T>, Sink<T>, java.io.Serializable {
      private static final long serialVersionUID = 1L;

      @Override
      public Operator getOperator() {
        return JdkSerializableOperator.this;
      }

      @Override
      public Sink<T> getSink() {
        return this;
      }

      @Override
      public void setConnected(boolean connected) {
      }

      @Override
      public Class<? extends StreamCodec<T>> getStreamCodec() {
        return null;
      }
    }

    @InputPortFieldAnnotation(name="", optional=true)
    final public InputPort<Object> inport1 = new SerializableInputPort<Object>() {
      private static final long serialVersionUID = 1L;

      @Override
      final public void put(Object payload)
      {
      }

      @Override
      public int getCount(boolean reset)
      {
        return 0;
      }

    };
  }

  @Test
  public void testJdkSerializableOperator() throws Exception {
    DAG dag = new DAG();
    JdkSerializableOperator o1 = dag.addOperator("o1", new JdkSerializableOperator());

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DAG.write(dag, outStream);
    outStream.close();

    DAG clonedDag = DAG.read(new ByteArrayInputStream(outStream.toByteArray()));
    JdkSerializableOperator o1Clone = (JdkSerializableOperator)clonedDag.getOperatorMeta("o1").getOperator();
    Assert.assertNotNull("port object null", o1Clone.inport1);
    Assert.assertEquals("", o1Clone, o1Clone.inport1.getOperator());

  }

}
