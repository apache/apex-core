/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.plan;

import com.datatorrent.api.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.Validation;
import javax.validation.ValidationException;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Sink;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.codec.KryoJdkSerializer;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.engine.TestOutputOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.support.StramTestSupport.RegexMatcher;
import com.esotericsoftware.kryo.DefaultSerializer;

public class LogicalPlanTest {

  @Test
  public void testCycleDetection() {
     LogicalPlan dag = new LogicalPlan();

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
     } catch (ValidationException e) {
       // expected
     }

  }

  public static class ValidationOperator extends BaseOperator {
    @OutputPortFieldAnnotation(name="goodOutputPort")
    public final transient DefaultOutputPort<Object> goodOutputPort = new DefaultOutputPort<Object>();

    @OutputPortFieldAnnotation(name="badOutputPort")
    public final transient DefaultOutputPort<Object> badOutputPort = new DefaultOutputPort<Object>();
  }

  public static class CounterOperator extends BaseOperator {
    @InputPortFieldAnnotation(name="countInputPort")
    final public transient InputPort<Object> countInputPort = new DefaultInputPort<Object>() {
      @Override
      final public void process(Object payload) {
      }
    };
  }

  @Test
  public void testLogicalPlanSerialization() throws Exception {

    LogicalPlan dag = new LogicalPlan();

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
    Assert.assertEquals("root operator id", "validationNode", dag.getRootOperators().get(0).getName());

    dag.getContextAttributes(countGoodNode).put(OperatorContext.SPIN_MILLIS, 10);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    LogicalPlan.write(dag, bos);

    // System.out.println("serialized size: " + bos.toByteArray().length);

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    LogicalPlan dagClone = LogicalPlan.read(bis);
    Assert.assertNotNull(dagClone);
    Assert.assertEquals("number operators in clone", dag.getAllOperators().size(), dagClone.getAllOperators().size());
    Assert.assertEquals("number root operators in clone", 1, dagClone.getRootOperators().size());
    Assert.assertTrue("root operator in operators", dagClone.getAllOperators().contains(dagClone.getRootOperators().get(0)));


    Operator countGoodNodeClone = dagClone.getOperatorMeta("countGoodNode").getOperator();
    Assert.assertEquals("", new Integer(10), dagClone.getContextAttributes(countGoodNodeClone).get(OperatorContext.SPIN_MILLIS));

  }

  @Test
  public void testDeleteOperator()
  {
    LogicalPlan dag = new LogicalPlan();
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    StreamMeta s1 = dag.addStream("s1", o1.outport1, o2.inport1);
    dag.validate();
    Assert.assertEquals("", 2, dag.getAllOperators().size());

    dag.removeOperator(o2);
    s1.remove();
    dag.validate();
    Assert.assertEquals("", 1, dag.getAllOperators().size());
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
    LogicalPlan dag = new LogicalPlan();
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

  @OperatorAnnotation(partitionable = false)
  public static class TestOperatorAnnotationOperator extends BaseOperator {

    @InputPortFieldAnnotation(name = "input1", optional = true)
    final public transient DefaultInputPort<Object> input1 = new DefaultInputPort<Object>() {
      @Override
      public void process(Object tuple) {
      }
    };
  }

  @OperatorAnnotation(partitionable = false)
  public static class TestOperatorAnnotationOperator2 extends BaseOperator implements Partitionable<TestOperatorAnnotationOperator2> {

    @Override
    public Collection<Partition<TestOperatorAnnotationOperator2>> definePartitions(Collection<? extends Partition<TestOperatorAnnotationOperator2>> partitions, int incrementalCapacity)
    {
      return null;
    }

  }

  @Test
  public void testOperatorAnnotation() {
    LogicalPlan dag = new LogicalPlan();
    TestOperatorAnnotationOperator operator = dag.addOperator("operator1", TestOperatorAnnotationOperator.class);

    dag.setAttribute(operator, OperatorContext.INITIAL_PARTITION_COUNT, 2);

    try {
      dag.validate();
      Assert.fail("should raise operator is not partitionable for operator1");
    } catch (ValidationException e) {
      Assert.assertEquals("", "Operator " + operator.getName() + " is not partitionable but INITIAL_PARTITION_COUNT attribute is set", e.getMessage());
    }

    dag.setAttribute(operator, OperatorContext.INITIAL_PARTITION_COUNT, 0);
    dag.validate();

    dag.setInputPortAttribute(operator.input1, PortContext.PARTITION_PARALLEL, true);

    try {
      dag.validate();
      Assert.fail("should raise operator is not partitionable for operator1");
    } catch (ValidationException e) {
      Assert.assertEquals("", "Operator " + operator.getName() + " is not partitionable but PARTITION_PARALLEL attribute is set", e.getMessage());
    }

    dag.setInputPortAttribute(operator.input1, PortContext.PARTITION_PARALLEL, false);
    dag.validate();

    dag.removeOperator(operator);
    TestOperatorAnnotationOperator2 operator2 = dag.addOperator("operator2", TestOperatorAnnotationOperator2.class);

    try {
      dag.validate();
      Assert.fail("should raise operator is not partitionable for operator2");
    } catch (ValidationException e) {
      Assert.assertEquals("", "Operator " + operator2.getName() + " is not partitionable but implements PartitionableOperator", e.getMessage());
    }
  }

  @Test
  public void testPortConnectionValidation() {

    LogicalPlan dag = new LogicalPlan();
    TestGeneratorInputOperator input = dag.addOperator("input1", TestGeneratorInputOperator.class);

    try {
      dag.validate();
      Assert.fail("should raise port not connected for input1.outputPort");
    } catch (ValidationException e) {
      Assert.assertEquals("", "Output port connection required: input1.outputPort", e.getMessage());
    }

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    dag.addStream("stream1", input.outport, o1.inport1);
    dag.validate();

    // required input
    dag.addOperator("counter", CounterOperator.class);
    try {
      dag.validate();
    } catch (ValidationException e) {
      Assert.assertEquals("", "Input port connection required: counter.countInputPort", e.getMessage());
    }

  }

  @Test
  public void testAtMostOnceProcessingModeValidation() {
    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator input1 = dag.addOperator("input1", TestGeneratorInputOperator.class);
    TestGeneratorInputOperator input2 = dag.addOperator("input2", TestGeneratorInputOperator.class);

    GenericTestOperator amoOper = dag.addOperator("amoOper", GenericTestOperator.class);
    dag.setAttribute(amoOper, OperatorContext.PROCESSING_MODE, Operator.ProcessingMode.AT_MOST_ONCE);

    dag.addStream("input1.outport", input1.outport, amoOper.inport1);
    dag.addStream("input2.outport", input2.outport, amoOper.inport2);

    GenericTestOperator outputOper = dag.addOperator("outputOper", GenericTestOperator.class);
    dag.setAttribute(outputOper, OperatorContext.PROCESSING_MODE, Operator.ProcessingMode.AT_LEAST_ONCE);
    dag.addStream("aloOper.outport1", amoOper.outport1, outputOper.inport1);

    try {
      dag.validate();
      Assert.fail("Exception expected for " + outputOper);
    } catch (ValidationException ve) {
      Assert.assertEquals("", ve.getMessage(), "Processing mode outputOper/AT_LEAST_ONCE not valid for source amoOper/AT_MOST_ONCE");
    }
    dag.setAttribute(outputOper, OperatorContext.PROCESSING_MODE, null);
    dag.validate();

    OperatorMeta outputOperOm = dag.getMeta(outputOper);
    Assert.assertEquals("" + outputOperOm.getAttributes(), Operator.ProcessingMode.AT_MOST_ONCE, outputOperOm.getValue(OperatorContext.PROCESSING_MODE));

  }

    @Test
  public void testExactlyOnceProcessingModeValidation() {
    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator input1 = dag.addOperator("input1", TestGeneratorInputOperator.class);
    TestGeneratorInputOperator input2 = dag.addOperator("input2", TestGeneratorInputOperator.class);

    GenericTestOperator amoOper = dag.addOperator("amoOper", GenericTestOperator.class);
    dag.setAttribute(amoOper, OperatorContext.PROCESSING_MODE, Operator.ProcessingMode.EXACTLY_ONCE);

    dag.addStream("input1.outport", input1.outport, amoOper.inport1);
    dag.addStream("input2.outport", input2.outport, amoOper.inport2);

    GenericTestOperator outputOper = dag.addOperator("outputOper", GenericTestOperator.class);
    dag.addStream("aloOper.outport1", amoOper.outport1, outputOper.inport1);

    try {
      dag.validate();
      Assert.fail("Exception expected for " + outputOper);
    } catch (ValidationException ve) {
      Assert.assertEquals("", ve.getMessage(), "Processing mode for outputOper should be AT_MOST_ONCE for source amoOper/EXACTLY_ONCE");
    }

    dag.setAttribute(outputOper, OperatorContext.PROCESSING_MODE, Operator.ProcessingMode.AT_LEAST_ONCE);

    try {
      dag.validate();
      Assert.fail("Exception expected for " + outputOper);
    } catch (ValidationException ve) {
      Assert.assertEquals("", ve.getMessage(), "Processing mode outputOper/AT_LEAST_ONCE not valid for source amoOper/EXACTLY_ONCE");
    }

    // AT_MOST_ONCE is valid
    dag.setAttribute(outputOper, OperatorContext.PROCESSING_MODE, Operator.ProcessingMode.AT_MOST_ONCE);
    dag.validate();
  }

  @Test
  public void testLocalityValidation() {
    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator input1 = dag.addOperator("input1", TestGeneratorInputOperator.class);
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    StreamMeta s1 = dag.addStream("input1.outport", input1.outport, o1.inport1).setLocality(Locality.THREAD_LOCAL);
    dag.validate();

    TestGeneratorInputOperator input2 = dag.addOperator("input2", TestGeneratorInputOperator.class);
    dag.addStream("input2.outport", input2.outport, o1.inport2);

    try {
      dag.validate();
      Assert.fail("Exception expected for " + o1);
    } catch (ValidationException ve) {
      Assert.assertThat("", ve.getMessage(), RegexMatcher.matches("Locality THREAD_LOCAL invalid for operator .* with multiple input streams .*"));
    }

    s1.setLocality(null);
    dag.validate();
  }

  private class TestAnnotationsOperator extends BaseOperator {
    @OutputPortFieldAnnotation(name="oport1")
    final public transient DefaultOutputPort<Object> outport1 = new DefaultOutputPort<Object>();

    @OutputPortFieldAnnotation(name="oport2", optional=false)
    final public transient DefaultOutputPort<Object> outport2 = new DefaultOutputPort<Object>();
  }

  private class TestAnnotationsOperator2 extends BaseOperator {
    // multiple ports w/o annotation, one of them must be connected
    final public transient DefaultOutputPort<Object> outport1 = new DefaultOutputPort<Object>();
  }

  private class TestAnnotationsOperator3 extends BaseOperator {
    // multiple ports w/o annotation, one of them must be connected
    @OutputPortFieldAnnotation(name="oport1", optional=true)
    final public transient DefaultOutputPort<Object> outport1 = new DefaultOutputPort<Object>();
    @OutputPortFieldAnnotation(name="oport2", optional=true)
    final public transient DefaultOutputPort<Object> outport2 = new DefaultOutputPort<Object>();
  }

  @Test
  public void testOutputPortAnnotation() {
    LogicalPlan dag = new LogicalPlan();
    TestAnnotationsOperator ta1 = dag.addOperator("testAnnotationsOperator", new TestAnnotationsOperator());

    try {
      dag.validate();
      Assert.fail("should raise: port connection required");
    } catch (ValidationException e) {
      Assert.assertEquals("", "Output port connection required: testAnnotationsOperator.oport2", e.getMessage());
    }

    TestOutputOperator o2 = dag.addOperator("sink", new TestOutputOperator());
    dag.addStream("s1", ta1.outport2, o2.inport);

    dag.validate();

    TestAnnotationsOperator2 ta2 = dag.addOperator("multiOutputPorts1", new TestAnnotationsOperator2());

    try {
      dag.validate();
      Assert.fail("should raise: At least one output port must be connected");
    } catch (ValidationException e) {
      Assert.assertEquals("", "At least one output port must be connected: multiOutputPorts1", e.getMessage());
    }
    TestOutputOperator o3 = dag.addOperator("o3", new TestOutputOperator());
    dag.addStream("s2", ta2.outport1, o3.inport);

    dag.addOperator("multiOutputPorts3", new TestAnnotationsOperator3());
    dag.validate();

  }

  public class DuplicatePortOperator extends GenericTestOperator {
    @OutputPortFieldAnnotation(name=OPORT1)
    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    final public transient DefaultOutputPort<Object> outport1 = new DefaultOutputPort<Object>();
  }

  @Test
  public void testDuplicatePort() {
    LogicalPlan dag = new LogicalPlan();
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
      public Sink<T> getSink() {
        return this;
      }

      @Override
      public void setConnected(boolean connected) {
      }

      @Override
      public void setup(PortContext context)
      {
      }

      @Override
      public void teardown()
      {
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
    LogicalPlan dag = new LogicalPlan();
    dag.addOperator("o1", new JdkSerializableOperator());

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    LogicalPlan.write(dag, outStream);
    outStream.close();

    LogicalPlan clonedDag = LogicalPlan.read(new ByteArrayInputStream(outStream.toByteArray()));
    JdkSerializableOperator o1Clone = (JdkSerializableOperator)clonedDag.getOperatorMeta("o1").getOperator();
    Assert.assertNotNull("port object null", o1Clone.inport1);
  }

}
