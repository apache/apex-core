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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.*;
import com.malhartech.dag.DAG.InputPort;
import com.malhartech.dag.DAG.OperatorInstance;
import com.malhartech.dag.DAG.StreamDecl;
import com.malhartech.stram.cli.StramClientUtils;

public class DAGBuilderTest {

  public static OperatorInstance assertNode(DAG tplg, String id) {
      OperatorInstance n = tplg.getOperator(id);
      assertNotNull("module exists id=" + id, n);
      return n;
  }

  /**
   * Test read from stram-site.xml in Hadoop configuration format.
   */
  @Test
  public void testLoadFromConfigXml() {
    Configuration conf = StramClientUtils.addStramResources(new Configuration());
    //Configuration.dumpConfiguration(conf, new PrintWriter(System.out));

    DAGPropertiesBuilder tb = new DAGPropertiesBuilder();
    tb.addFromConfiguration(conf);

    DAG dag = tb.getApplication(new Configuration(false));
    dag.validate();

//    Map<String, NodeConf> moduleConfs = tb.getAllOperators();
    assertEquals("number of module confs", 6, dag.getAllOperators().size());

    OperatorInstance module1 = assertNode(dag, "module1");
    OperatorInstance module2 = assertNode(dag, "module2");
    OperatorInstance module3 = assertNode(dag, "module3");
    OperatorInstance module4 = assertNode(dag, "module4");

    assertNotNull("moduleConf for root", module1);
    assertEquals("moduleId set", "module1", module1.getId());

    // verify module instantiation
    Module dNode = initOperator(module1);
    assertNotNull(dNode);
    assertEquals(dNode.getClass(), GenericTestModule.class);
    GenericTestModule GenericTestNode = (GenericTestModule)dNode;
    assertEquals("myStringPropertyValue", GenericTestNode.getMyStringProperty());

    // check links
    assertEquals("module1 inputs", 0, module1.getInputStreams().size());
    assertEquals("module1 outputs", 1, module1.getOutputStreams().size());
    StreamDecl n1n2 = module2.getInputStreams().get(GenericTestNode.INPUT1);
    assertNotNull("n1n2", n1n2);

    // output/input stream object same
    assertEquals("rootNode out is module2 in", n1n2, module1.getOutputStreams().get(GenericTestNode.OUTPUT1));
    assertEquals("n1n2 source", module1, n1n2.getSource().getNode());
    Assert.assertEquals("n1n2 targets", 1, n1n2.getSinks().size());
    Assert.assertEquals("n1n2 target", module2, n1n2.getSinks().get(0).getNode());

    assertEquals("stream name", "n1n2", n1n2.getId());
    Assert.assertFalse("n1n2 not inline (default)", n1n2.isInline());

    // module 2 streams to module 3 and module 4
    assertEquals("module 2 number of outputs", 1, module2.getOutputStreams().size());
    StreamDecl fromNode2 = module2.getOutputStreams().values().iterator().next();

    Set<OperatorInstance> targetNodes = new HashSet<OperatorInstance>();
    for (InputPort ip : fromNode2.getSinks()) {
      targetNodes.add(ip.getNode());
    }
    Assert.assertEquals("outputs " + fromNode2, Sets.newHashSet(module3, module4), targetNodes);

    OperatorInstance module6 = assertNode(dag, "module6");

    List<OperatorInstance> rootNodes = dag.getRootOperators();
    assertEquals("number root modules", 2, rootNodes.size());
    assertTrue("root module2", rootNodes.contains(module1));
    assertTrue("root module6", rootNodes.contains(module6));

    for (OperatorInstance n : rootNodes) {
      printTopology(n, dag, 0);
    }

  }

  @SuppressWarnings("unchecked")
  private <T extends Module> T initOperator(OperatorInstance moduleConf) {
    return (T)StramUtils.initNode(moduleConf.getNodeClass(), moduleConf.getId(), moduleConf.getProperties());
  }

  public void printTopology(OperatorInstance module, DAG tplg, int level) {
      String prefix = "";
      if (level > 0) {
        prefix = StringUtils.repeat(" ", 20*(level-1)) + "   |" + StringUtils.repeat("-", 17);
      }
      System.out.println(prefix + module.getId());
      for (StreamDecl downStream : module.getOutputStreams().values()) {
          if (!downStream.getSinks().isEmpty()) {
            for (InputPort targetNode : downStream.getSinks()) {
              printTopology(targetNode.getNode(), tplg, level+1);
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

      assertEquals("number of module confs", 5, dag.getAllOperators().size());
      assertEquals("number of root modules", 1, dag.getRootOperators().size());

      StreamDecl s1 = dag.getStream("n1n2");
      assertNotNull(s1);
      assertTrue("n1n2 inline", s1.isInline());

      OperatorInstance module3 = dag.getOperator("module3");
      Map<String, String> module3Props = module3.getProperties();

      assertEquals("module3.myStringProperty", "myStringPropertyValueFromTemplate", module3Props.get("myStringProperty"));
      assertEquals("module3.classname", GenericTestModule.class.getName(), module3Props.get(DAGPropertiesBuilder.OPERATOR_CLASSNAME));

      GenericTestModule dmodule3 = initOperator(module3);
      assertEquals("module3.myStringProperty", "myStringPropertyValueFromTemplate", dmodule3.getMyStringProperty());
      assertFalse("module3.booleanProperty", dmodule3.booleanProperty);

      OperatorInstance module4 = dag.getOperator("module4");
      assertEquals("module4.myStringProperty", "overrideModule4", module4.getProperties().get("myStringProperty"));
      GenericTestModule dmodule4 = (GenericTestModule)initOperator(module4);
      assertEquals("module4.myStringProperty", "overrideModule4", dmodule4.getMyStringProperty());
      assertTrue("module4.booleanProperty", dmodule4.booleanProperty);

      StreamDecl input1 = dag.getStream("inputStream");
      assertNotNull(input1);
      Assert.assertEquals("input1 source", dag.getOperator("inputModule"), input1.getSource().getNode());
      Set<OperatorInstance> targetNodes = new HashSet<OperatorInstance>();
      for (InputPort targetPort : input1.getSinks()) {
        targetNodes.add(targetPort.getNode());
      }

      Assert.assertEquals("input1 target ", Sets.newHashSet(dag.getOperator("module1"), module3, module4), targetNodes);

  }

  @Test
  public void testCycleDetection() {
     DAG dag = new DAG();

     //NodeConf module1 = b.getOrAddNode("module1");
     OperatorInstance module2 = dag.addOperator("module2", GenericTestModule.class);
     OperatorInstance module3 = dag.addOperator("module3", GenericTestModule.class);
     OperatorInstance module4 = dag.addOperator("module4", GenericTestModule.class);
     //NodeConf module5 = b.getOrAddNode("module5");
     //NodeConf module6 = b.getOrAddNode("module6");
     OperatorInstance module7 = dag.addOperator("module7", GenericTestModule.class);

     // strongly connect n2-n3-n4-n2
     dag.addStream("n2n3")
       .setSource(module2.getOutput(GenericTestModule.OUTPUT1))
       .addSink(module3.getInput(GenericTestModule.INPUT1));

     dag.addStream("n3n4")
       .setSource(module3.getOutput(GenericTestModule.OUTPUT1))
       .addSink(module4.getInput(GenericTestModule.INPUT1));

     dag.addStream("n4n2")
       .setSource(module4.getOutput(GenericTestModule.OUTPUT1))
       .addSink(module2.getInput(GenericTestModule.INPUT1));

     // self referencing module cycle
     StreamDecl n7n7 = dag.addStream("n7n7")
         .setSource(module7.getOutput(GenericTestModule.OUTPUT1))
         .addSink(module7.getInput(GenericTestModule.INPUT1));
     try {
       n7n7.addSink(module7.getInput(GenericTestModule.INPUT1));
       fail("cannot add to stream again");
     } catch (Exception e) {
       // expected, stream can have single input/output only
     }

     List<List<String>> cycles = new ArrayList<List<String>>();
     dag.findStronglyConnected(module7, cycles);
     assertEquals("module self reference", 1, cycles.size());
     assertEquals("module self reference", 1, cycles.get(0).size());
     assertEquals("module self reference", module7.getId(), cycles.get(0).get(0));

     // 3 module cycle
     cycles.clear();
     dag.findStronglyConnected(module4, cycles);
     assertEquals("3 module cycle", 1, cycles.size());
     assertEquals("3 module cycle", 3, cycles.get(0).size());
     assertTrue("module2", cycles.get(0).contains(module2.getId()));
     assertTrue("module3", cycles.get(0).contains(module3.getId()));
     assertTrue("module4", cycles.get(0).contains(module4.getId()));

     try {
       dag.validate();
       fail("validation should fail");
     } catch (IllegalStateException e) {
       // expected
     }

  }

  public static class TestSerDe extends DefaultSerDe {

  }

  @ModuleAnnotation(
      ports = {
          @PortAnnotation(name = "goodOutputPort",  type = PortType.OUTPUT),
          @PortAnnotation(name = "badOutputPort",  type = PortType.OUTPUT)
      }
  )
  static class ValidationModule extends Module implements Sink {
    @Override
    public void process(Object payload) {
      // classify tuples
    }
  }

  @ModuleAnnotation(
      ports = {
          @PortAnnotation(name = "countInputPort",  type = PortType.INPUT)
      }
  )
  static class CounterModule extends Module implements Sink {
    @Override
    public void process(Object payload) {
      // count tuples
    }
  }

  @ModuleAnnotation(
      ports = {
          @PortAnnotation(name = "echoInputPort",  type = PortType.INPUT)
      }
  )
  static class ConsoleOutputModule extends Module implements Sink{
    @Override
    public void process(Object payload) {
      // print tuples
    }
  }

  @Test
  public void testJavaBuilder() throws Exception {

    DAG dag = new DAG();

    OperatorInstance validationNode = dag.addOperator("validationNode", ValidationModule.class);
    OperatorInstance countGoodNode = dag.addOperator("countGoodNode", CounterModule.class);
    OperatorInstance countBadNode = dag.addOperator("countBadNode", CounterModule.class);
    OperatorInstance echoBadNode = dag.addOperator("echoBadNode", ConsoleOutputModule.class);

    // good tuples to counter module
    dag.addStream("goodTuplesStream")
      .setSource(validationNode.getOutput("goodOutputPort"))
      .addSink(countGoodNode.getInput("countInputPort"));

    // bad tuples to separate stream and echo module
    // (stream with 2 outputs)
    dag.addStream("badTuplesStream")
      .setSource(validationNode.getOutput("badOutputPort"))
      .addSink(countBadNode.getInput("countInputPort"))
      .addSink(echoBadNode.getInput("echoInputPort"));

    Assert.assertEquals("number root modules", 1, dag.getRootOperators().size());
    Assert.assertEquals("root module id", "validationNode", dag.getRootOperators().get(0).getId());

    System.out.println(dag);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DAG.write(dag, bos);

    System.out.println("serialized size: " + bos.toByteArray().length);

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DAG dagClone = DAG.read(bis);
    Assert.assertNotNull(dagClone);
    Assert.assertEquals("number modules in clone", dag.getAllOperators().size(), dagClone.getAllOperators().size());
    Assert.assertEquals("number root modules in clone", 1, dagClone.getRootOperators().size());
    Assert.assertTrue("root module in modules", dagClone.getAllOperators().contains(dagClone.getRootOperators().get(0)));
  }

}
