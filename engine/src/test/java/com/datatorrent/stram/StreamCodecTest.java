package com.datatorrent.stram;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.stram.api.OperatorDeployInfo;
import com.datatorrent.stram.codec.DefaultStatefulStreamCodec;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.support.StramTestSupport;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 9/5/14.
 */
public class StreamCodecTest
{
  @Rule
  public StramTestSupport.TestMeta testMeta = new StramTestSupport.TestMeta();

  @Test
  public void testStreamCodec() {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(DAGContext.APPLICATION_PATH, testMeta.dir);

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node3.inport1, Context.PortContext.STREAM_CODEC, serDe);

    dag.addStream("n1n2", node1.outport1, node2.inport1);
    dag.addStream("n2n3", node2.outport1, node3.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals("number containers", 3, containers.size());

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);
    LogicalPlan.OperatorMeta n3meta = dag.getMeta(node3);

    OperatorDeployInfo n1di = getSingleOperatorDeployInfo(node1, node1.getName(), dnm);

    OperatorDeployInfo.OutputDeployInfo n1odi = getOutputDeployInfo(n1di, n1meta.getMeta(node1.outport1));
    String id = n1meta.getName() + " " + n1odi.portName;
    Assert.assertEquals("number stream codecs " + id, n1odi.streamCodecs.size(), 1);
    OperatorDeployInfo.StreamIdentifier streamIdentifier = new OperatorDeployInfo.StreamIdentifier();
    streamIdentifier.operName = n2meta.getName();
    streamIdentifier.portName = n2meta.getMeta(node2.inport1).getPortName();
    checkNotSetStreamCodecInfo(n1odi.streamCodecs, id, streamIdentifier);


    OperatorDeployInfo n2di = getSingleOperatorDeployInfo(node2, node2.getName(), dnm);

    OperatorDeployInfo.InputDeployInfo n2idi = getInputDeployInfo(n2di, n2meta.getMeta(node2.inport1));
    id = n2meta.getName() + " " + n2idi.portName;
    Assert.assertEquals("number stream codecs " + id, n2idi.streamCodecs.size(), 1);
    checkNotSetStreamCodecInfo(n2idi.streamCodecs, id, streamIdentifier);

    OperatorDeployInfo.OutputDeployInfo n2odi = getOutputDeployInfo(n2di, n2meta.getMeta(node2.outport1));
    id = n2meta.getName() + " " + n2odi.portName;
    Assert.assertEquals("number stream codecs " + id, n2odi.streamCodecs.size(), 1);
    streamIdentifier.operName = n3meta.getName();
    streamIdentifier.portName = n3meta.getMeta(node3.inport1).getPortName();
    checkStreamCodecInfo(n2odi.streamCodecs, id, streamIdentifier, serDe);


    OperatorDeployInfo n3di = getSingleOperatorDeployInfo(node3, node3.getName(), dnm);

    OperatorDeployInfo.InputDeployInfo n3idi = getInputDeployInfo(n3di, n3meta.getMeta(node3.inport1));
    id = n3meta.getName() + " " + n3idi.portName;
    Assert.assertEquals("number stream codecs " + id, n3idi.streamCodecs.size(), 1);
    checkStreamCodecInfo(n3idi.streamCodecs, id, streamIdentifier, serDe);
  }

  @Test
  public void testDefaultStreamCodec() {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(DAGContext.APPLICATION_PATH, testMeta.dir);

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    DefaultCodecOperator node2 = dag.addOperator("node2", DefaultCodecOperator.class);
    DefaultCodecOperator node3 = dag.addOperator("node3", DefaultCodecOperator.class);
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node3.inportWithCodec, Context.PortContext.STREAM_CODEC, serDe);

    dag.addStream("n1n2", node1.outport1, node2.inportWithCodec);
    dag.addStream("n2n3", node2.outport1, node3.inportWithCodec);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals("number containers", 3, containers.size());

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);
    LogicalPlan.OperatorMeta n3meta = dag.getMeta(node3);

    OperatorDeployInfo n1di = getSingleOperatorDeployInfo(node1, node1.getName(), dnm);

    OperatorDeployInfo.OutputDeployInfo n1odi = getOutputDeployInfo(n1di, n1meta.getMeta(node1.outport1));
    String id = n1meta.getName() + " " + n1odi.portName;
    Assert.assertEquals("number stream codecs " + id, n1odi.streamCodecs.size(), 1);
    OperatorDeployInfo.StreamIdentifier streamIdentifier = new OperatorDeployInfo.StreamIdentifier();
    streamIdentifier.operName = n2meta.getName();
    streamIdentifier.portName = n2meta.getMeta(node2.inportWithCodec).getPortName();
    checkStreamCodecInfo(n1odi.streamCodecs, id, streamIdentifier, null, node2.inportWithCodec.getStreamCodec().getName());


    OperatorDeployInfo n2di = getSingleOperatorDeployInfo(node2, node2.getName(), dnm);

    OperatorDeployInfo.InputDeployInfo n2idi = getInputDeployInfo(n2di, n2meta.getMeta(node2.inportWithCodec));
    id = n2meta.getName() + " " + n2idi.portName;
    Assert.assertEquals("number stream codecs " + id, n2idi.streamCodecs.size(), 1);
    checkStreamCodecInfo(n2idi.streamCodecs, id, streamIdentifier, null, node2.inportWithCodec.getStreamCodec().getName());

    OperatorDeployInfo.OutputDeployInfo n2odi = getOutputDeployInfo(n2di, n2meta.getMeta(node2.outport1));
    id = n2meta.getName() + " " + n2odi.portName;
    Assert.assertEquals("number stream codecs " + id, n2odi.streamCodecs.size(), 1);
    streamIdentifier.operName = n3meta.getName();
    streamIdentifier.portName = n3meta.getMeta(node3.inportWithCodec).getPortName();
    checkStreamCodecInfo(n2odi.streamCodecs, id, streamIdentifier, serDe);


    OperatorDeployInfo n3di = getSingleOperatorDeployInfo(node3, node3.getName(), dnm);

    OperatorDeployInfo.InputDeployInfo n3idi = getInputDeployInfo(n3di, n3meta.getMeta(node3.inportWithCodec));
    id = n3meta.getName() + " " + n3idi.portName;
    Assert.assertEquals("number stream codecs " + id, n3idi.streamCodecs.size(), 1);
    checkStreamCodecInfo(n3idi.streamCodecs, id, streamIdentifier, serDe);
  }

  @Test
  public void testPartitioningStreamCodec() {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(DAGContext.APPLICATION_PATH, testMeta.dir);

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    dag.setAttribute(node2, Context.OperatorContext.INITIAL_PARTITION_COUNT, 3);
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.STREAM_CODEC, serDe);

    dag.addStream("n1n2", node1.outport1, node2.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals("number containers", 4, containers.size());

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);

    OperatorDeployInfo n1di = getSingleOperatorDeployInfo(node1, node1.getName(), dnm);

    OperatorDeployInfo.OutputDeployInfo n1odi = getOutputDeployInfo(n1di, n1meta.getMeta(node1.outport1));
    String id = n1meta.getName() + " " + n1odi.portName;
    Assert.assertEquals("number stream codecs " + id, n1odi.streamCodecs.size(), 1);
    OperatorDeployInfo.StreamIdentifier streamIdentifier = new OperatorDeployInfo.StreamIdentifier();
    streamIdentifier.operName = n2meta.getName();
    streamIdentifier.portName = n2meta.getMeta(node2.inport1).getPortName();
    checkStreamCodecInfo(n1odi.streamCodecs, id, streamIdentifier, serDe);


    List<PTOperator> operators = plan.getOperators(n2meta);
    Assert.assertEquals("number operators " + n2meta.getName(), 3, operators.size());
    for (PTOperator operator : operators) {
      OperatorDeployInfo odi = getOperatorDeployInfo(operator, n2meta.getName(), dnm);

      OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n2meta.getMeta(node2.inport1));
      id = n2meta.getName() + " " + idi.portName;
      Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
      checkStreamCodecInfo(idi.streamCodecs, id, streamIdentifier, serDe);
    }
  }

  @Test
  public void testMxNPartitioningStreamCodec() {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(DAGContext.APPLICATION_PATH, testMeta.dir);

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    dag.setAttribute(node1, Context.OperatorContext.INITIAL_PARTITION_COUNT, 2);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    dag.setAttribute(node2, Context.OperatorContext.INITIAL_PARTITION_COUNT, 3);
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.STREAM_CODEC, serDe);

    dag.addStream("n1n2", node1.outport1, node2.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);

    // Sanity check that physical operators have been allocated for n1meta and n2meta
    Assert.assertEquals("number operators " + n1meta.getName(), 2, plan.getOperators(n1meta).size());
    Assert.assertEquals("number operators " + n2meta.getName(), 3, plan.getOperators(n2meta).size());

    for (PTContainer container : containers) {
      List<PTOperator> operators = container.getOperators();
      for (PTOperator operator :operators) {
        if (!operator.isUnifier()) {
          if (operator.getOperatorMeta() == n1meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n1meta.getName(), dnm);

            OperatorDeployInfo.OutputDeployInfo otdi = getOutputDeployInfo(odi, n1meta.getMeta(node1.outport1));
            String id = n1meta.getName() + " " + otdi.portName;
            Assert.assertEquals("number stream codecs " + id, otdi.streamCodecs.size(), 1);
            OperatorDeployInfo.StreamIdentifier streamIdentifier = new OperatorDeployInfo.StreamIdentifier();
            streamIdentifier.operName = n2meta.getName();
            streamIdentifier.portName = n2meta.getMeta(node2.inport1).getPortName();
            checkStreamCodecInfo(otdi.streamCodecs, id, streamIdentifier, serDe);
          } else if (operator.getOperatorMeta() == n2meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n2meta.getName(), dnm);

            OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n2meta.getMeta(node2.inport1));
            String id = n1meta.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            OperatorDeployInfo.StreamIdentifier streamIdentifier = new OperatorDeployInfo.StreamIdentifier();
            streamIdentifier.operName = n2meta.getName();
            streamIdentifier.portName = n2meta.getMeta(node2.inport1).getPortName();
            checkStreamCodecInfo(idi.streamCodecs, id, streamIdentifier, serDe);
          }
        } else {
          OperatorDeployInfo odi = getOperatorDeployInfo(operator, operator.getName(), dnm);
          List<OperatorDeployInfo.InputDeployInfo> idis = odi.inputs;
          for (OperatorDeployInfo.InputDeployInfo idi : idis) {
            String id = operator.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            OperatorDeployInfo.StreamIdentifier streamIdentifier = new OperatorDeployInfo.StreamIdentifier();
            streamIdentifier.operName = n2meta.getName();
            streamIdentifier.portName = n2meta.getMeta(node2.inport1).getPortName();
            checkStreamCodecInfo(idi.streamCodecs, id, streamIdentifier, serDe);
          }
        }
      }
    }
  }

  @Test
  public void testParallelPartitioningStreamCodec() {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(DAGContext.APPLICATION_PATH, testMeta.dir);

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    dag.setAttribute(node1, Context.OperatorContext.INITIAL_PARTITION_COUNT, 2);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.PARTITION_PARALLEL, true);
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.STREAM_CODEC, serDe);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);
    TestStreamCodec2 serDe2 = new TestStreamCodec2();
    dag.setInputPortAttribute(node3.inport1, Context.PortContext.STREAM_CODEC, serDe2);

    dag.addStream("n1n2", node1.outport1, node2.inport1);
    dag.addStream("n2n3", node2.outport1, node3.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i+1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);
    LogicalPlan.OperatorMeta n3meta = dag.getMeta(node3);

    // Sanity check that physical operators have been allocated for n1meta and n2meta
    Assert.assertEquals("number operators " + n1meta.getName(), 2, plan.getOperators(n1meta).size());
    Assert.assertEquals("number operators " + n2meta.getName(), 2, plan.getOperators(n2meta).size());
    Assert.assertEquals("number operators " + n3meta.getName(), 1, plan.getOperators(n3meta).size());

    for (PTContainer container : containers) {
      List<PTOperator> operators = container.getOperators();
      for (PTOperator operator :operators) {
        if (!operator.isUnifier()) {
          if (operator.getOperatorMeta() == n1meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n1meta.getName(), dnm);

            OperatorDeployInfo.OutputDeployInfo otdi = getOutputDeployInfo(odi, n1meta.getMeta(node1.outport1));
            String id = n1meta.getName() + " " + otdi.portName;
            Assert.assertEquals("number stream codecs " + id, otdi.streamCodecs.size(), 1);
            OperatorDeployInfo.StreamIdentifier streamIdentifier = new OperatorDeployInfo.StreamIdentifier();
            streamIdentifier.operName = n2meta.getName();
            streamIdentifier.portName = n2meta.getMeta(node2.inport1).getPortName();
            checkStreamCodecInfo(otdi.streamCodecs, id, streamIdentifier, serDe);
          } else if (operator.getOperatorMeta() == n2meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n2meta.getName(), dnm);

            OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n2meta.getMeta(node2.inport1));
            String id = n1meta.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            OperatorDeployInfo.StreamIdentifier streamIdentifier = new OperatorDeployInfo.StreamIdentifier();
            streamIdentifier.operName = n2meta.getName();
            streamIdentifier.portName = n2meta.getMeta(node2.inport1).getPortName();
            checkStreamCodecInfo(idi.streamCodecs, id, streamIdentifier, serDe);

            OperatorDeployInfo.OutputDeployInfo otdi = getOutputDeployInfo(odi, n2meta.getMeta(node2.outport1));
            id = n2meta.getName() + " " + otdi.portName;
            Assert.assertEquals("number stream codecs " + id, otdi.streamCodecs.size(), 1);
            streamIdentifier.operName = n3meta.getName();
            streamIdentifier.portName = n3meta.getMeta(node3.inport1).getPortName();
            checkStreamCodecInfo(otdi.streamCodecs, id, streamIdentifier, serDe2);
          } else if (operator.getOperatorMeta() == n3meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n3meta.getName(), dnm);

            OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n3meta.getMeta(node3.inport1));
            String id = n3meta.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            OperatorDeployInfo.StreamIdentifier streamIdentifier = new OperatorDeployInfo.StreamIdentifier();
            streamIdentifier.operName = n3meta.getName();
            streamIdentifier.portName = n3meta.getMeta(node3.inport1).getPortName();
            checkStreamCodecInfo(idi.streamCodecs, id, streamIdentifier, serDe2);
          }
        } else {
          OperatorDeployInfo odi = getOperatorDeployInfo(operator, operator.getName(), dnm);
          List<OperatorDeployInfo.InputDeployInfo> idis = odi.inputs;
          for (OperatorDeployInfo.InputDeployInfo idi : idis) {
            String id = operator.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            OperatorDeployInfo.StreamIdentifier streamIdentifier = new OperatorDeployInfo.StreamIdentifier();
            streamIdentifier.operName = n3meta.getName();
            streamIdentifier.portName = n3meta.getMeta(node3.inport1).getPortName();
            checkStreamCodecInfo(idi.streamCodecs, id, streamIdentifier, serDe2);
          }
        }
      }
    }
  }

  @Test
  public void testMultipleStreamCodecs() {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(DAGContext.APPLICATION_PATH, testMeta.dir);

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.STREAM_CODEC, serDe);
    TestStreamCodec2 serDe2 = new TestStreamCodec2();
    dag.setInputPortAttribute(node3.inport1, Context.PortContext.STREAM_CODEC, serDe2);

    dag.addStream("n1n2n3", node1.outport1, node2.inport1, node3.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals("number containers", 3, containers.size());

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);
    LogicalPlan.OperatorMeta n3meta = dag.getMeta(node3);

    OperatorDeployInfo n1di = getSingleOperatorDeployInfo(node1, node1.getName(), dnm);

    OperatorDeployInfo.OutputDeployInfo n1odi = getOutputDeployInfo(n1di, n1meta.getMeta(node1.outport1));
    String id = n1meta.getName() + " " + n1odi.portName;
    Assert.assertEquals("number stream codecs " + id, n1odi.streamCodecs.size(), 2);
    OperatorDeployInfo.StreamIdentifier streamIdentifier = new OperatorDeployInfo.StreamIdentifier();
    streamIdentifier.operName = n2meta.getName();
    streamIdentifier.portName = n2meta.getMeta(node2.inport1).getPortName();
    checkStreamCodecInfo(n1odi.streamCodecs, id, streamIdentifier, serDe);
    streamIdentifier.operName = n3meta.getName();
    streamIdentifier.portName = n3meta.getMeta(node3.inport1).getPortName();
    checkStreamCodecInfo(n1odi.streamCodecs, id, streamIdentifier, serDe2);

    OperatorDeployInfo n2di = getSingleOperatorDeployInfo(node2, node2.getName(), dnm);

    OperatorDeployInfo.InputDeployInfo n2idi = getInputDeployInfo(n2di, n2meta.getMeta(node2.inport1));
    id = n2meta.getName() + " " + n2idi.portName;
    Assert.assertEquals("number stream codecs " + id, n2idi.streamCodecs.size(), 1);
    streamIdentifier.operName = n2meta.getName();
    streamIdentifier.portName = n2meta.getMeta(node2.inport1).getPortName();
    checkStreamCodecInfo(n2idi.streamCodecs, id, streamIdentifier, serDe);

    OperatorDeployInfo n3di = getSingleOperatorDeployInfo(node3, node3.getName(), dnm);

    OperatorDeployInfo.InputDeployInfo n3idi = getInputDeployInfo(n3di, n3meta.getMeta(node3.inport1));
    id = n3meta.getName() + " " + n3idi.portName;
    Assert.assertEquals("number stream codecs " + id, n3idi.streamCodecs.size(), 1);
    streamIdentifier.operName = n3meta.getName();
    streamIdentifier.portName = n3meta.getMeta(node3.inport1).getPortName();
    checkStreamCodecInfo(n3idi.streamCodecs, id, streamIdentifier, serDe2);
  }

  @Test
  public void testMultipleMxNStreamCodecs() {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(DAGContext.APPLICATION_PATH, testMeta.dir);

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    dag.setAttribute(node1, Context.OperatorContext.INITIAL_PARTITION_COUNT, 2);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    dag.setAttribute(node2, Context.OperatorContext.INITIAL_PARTITION_COUNT, 3);
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.STREAM_CODEC, serDe);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);
    dag.setAttribute(node3, Context.OperatorContext.INITIAL_PARTITION_COUNT, 3);
    TestStreamCodec serDe2 = new TestStreamCodec();
    dag.setInputPortAttribute(node3.inport1, Context.PortContext.STREAM_CODEC, serDe2);


    dag.addStream("n1n2n3", node1.outport1, node2.inport1, node3.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);
    LogicalPlan.OperatorMeta n3meta = dag.getMeta(node3);

    // Sanity check that physical operators have been allocated for n1meta and n2meta
    Assert.assertEquals("number operators " + n1meta.getName(), 2, plan.getOperators(n1meta).size());
    Assert.assertEquals("number operators " + n2meta.getName(), 3, plan.getOperators(n2meta).size());
    Assert.assertEquals("number operators " + n3meta.getName(), 3, plan.getOperators(n3meta).size());

    for (PTContainer container : containers) {
      List<PTOperator> operators = container.getOperators();
      for (PTOperator operator :operators) {
        if (!operator.isUnifier()) {
          if (operator.getOperatorMeta() == n1meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n1meta.getName(), dnm);

            OperatorDeployInfo.OutputDeployInfo otdi = getOutputDeployInfo(odi, n1meta.getMeta(node1.outport1));
            String id = n1meta.getName() + " " + otdi.portName;
            Assert.assertEquals("number stream codecs " + id, otdi.streamCodecs.size(), 2);
            OperatorDeployInfo.StreamIdentifier streamIdentifier = new OperatorDeployInfo.StreamIdentifier();
            streamIdentifier.operName = n2meta.getName();
            streamIdentifier.portName = n2meta.getMeta(node2.inport1).getPortName();
            checkStreamCodecInfo(otdi.streamCodecs, id, streamIdentifier, serDe);
            streamIdentifier.operName = n3meta.getName();
            streamIdentifier.portName = n3meta.getMeta(node3.inport1).getPortName();
            checkStreamCodecInfo(otdi.streamCodecs, id, streamIdentifier, serDe2);
          } else if (operator.getOperatorMeta() == n2meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n2meta.getName(), dnm);

            OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n2meta.getMeta(node2.inport1));
            String id = n1meta.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            OperatorDeployInfo.StreamIdentifier streamIdentifier = new OperatorDeployInfo.StreamIdentifier();
            streamIdentifier.operName = n2meta.getName();
            streamIdentifier.portName = n2meta.getMeta(node2.inport1).getPortName();
            checkStreamCodecInfo(idi.streamCodecs, id, streamIdentifier, serDe);
          } else if (operator.getOperatorMeta() == n3meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n3meta.getName(), dnm);

            OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n3meta.getMeta(node3.inport1));
            String id = n3meta.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            OperatorDeployInfo.StreamIdentifier streamIdentifier = new OperatorDeployInfo.StreamIdentifier();
            streamIdentifier.operName = n3meta.getName();
            streamIdentifier.portName = n3meta.getMeta(node3.inport1).getPortName();
            checkStreamCodecInfo(idi.streamCodecs, id, streamIdentifier, serDe2);
          }
        } else {
          OperatorDeployInfo odi = getOperatorDeployInfo(operator, operator.getName(), dnm);
          Assert.assertEquals("unifier outputs " + operator.getName(), 1, operator.getOutputs().size());
          PTOperator.PTOutput out = operator.getOutputs().get(0);
          Assert.assertEquals("unifier sinks " + operator.getName(), 1, out.sinks.size());
          PTOperator.PTInput idInput = out.sinks.get(0);
          LogicalPlan.OperatorMeta idMeta = idInput.target.getOperatorMeta();
          List<OperatorDeployInfo.InputDeployInfo> idis = odi.inputs;
          StreamCodec<?> streamCodec = null;
          if (idMeta == n2meta) {
            streamCodec = serDe;
          } else if (idMeta == n3meta) {
            streamCodec = serDe2;
          }
          for (OperatorDeployInfo.InputDeployInfo idi : idis) {
            String id = operator.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            OperatorDeployInfo.StreamIdentifier streamIdentifier = new OperatorDeployInfo.StreamIdentifier();
            streamIdentifier.operName = idMeta.getName();
            streamIdentifier.portName = idInput.portName;
            checkStreamCodecInfo(idi.streamCodecs, id, streamIdentifier, streamCodec);
          }
        }
      }
    }
  }

  private void checkNotSetStreamCodecInfo(Map<OperatorDeployInfo.StreamIdentifier, OperatorDeployInfo.StreamCodecInfo> streamCodecs, String id,
                                          OperatorDeployInfo.StreamIdentifier streamIdentifier) {
    OperatorDeployInfo.StreamCodecInfo streamCodecInfo = streamCodecs.get(streamIdentifier);
    Assert.assertNotNull("stream codec info " + id, streamCodecInfo);
    Assert.assertNull("stream codec object " + id, streamCodecInfo.streamCodec);
    Assert.assertNull("stream codec class " + id, streamCodecInfo.serDeClassName);
  }

  private void checkStreamCodecInfo(Map<OperatorDeployInfo.StreamIdentifier, OperatorDeployInfo.StreamCodecInfo> streamCodecs, String id,
                                    OperatorDeployInfo.StreamIdentifier streamIdentifier, StreamCodec<?> streamCodec) {
    checkStreamCodecInfo(streamCodecs, id, streamIdentifier, streamCodec, null);
  }

  private void checkStreamCodecInfo(Map<OperatorDeployInfo.StreamIdentifier, OperatorDeployInfo.StreamCodecInfo> streamCodecs, String id,
                                    OperatorDeployInfo.StreamIdentifier streamIdentifier, StreamCodec<?> streamCodec, String className) {
    OperatorDeployInfo.StreamCodecInfo streamCodecInfo = streamCodecs.get(streamIdentifier);
    Assert.assertNotNull("stream codec info " + id, streamCodecInfo);
    Assert.assertEquals("stream codec object " + id, streamCodec, streamCodecInfo.streamCodec);
    Assert.assertEquals("stream codec class " + id, className, streamCodecInfo.serDeClassName);
  }

  private OperatorDeployInfo getSingleOperatorDeployInfo(Operator oper, String id, StreamingContainerManager scm)
  {
    LogicalPlan dag = scm.getLogicalPlan();
    PhysicalPlan plan = scm.getPhysicalPlan();
    List<PTOperator> operators = plan.getOperators(dag.getMeta(oper));
    Assert.assertEquals("number of operators " + id, 1, operators.size());

    PTOperator operator = operators.get(0);
    return getOperatorDeployInfo(operator, id, scm);
  }

  private OperatorDeployInfo getOperatorDeployInfo(PTOperator operator, String id, StreamingContainerManager scm)
  {
    String containerId = operator.getContainer().getExternalId();

    List<OperatorDeployInfo> cdi = StreamingContainerManagerTest.getDeployInfo(scm.getContainerAgent(containerId));

    OperatorDeployInfo odi = null;
    for (OperatorDeployInfo iodi : cdi) {
      if (iodi.id == operator.getId()) {
        odi = iodi;
        break;
      }
    }

    Assert.assertNotNull(id + " assigned to " + containerId + " deploy info", odi );
    return odi;
  }

  private OperatorDeployInfo.InputDeployInfo getInputDeployInfo(OperatorDeployInfo odi, LogicalPlan.InputPortMeta portMeta) {
    OperatorDeployInfo.InputDeployInfo idi = null;
    List<OperatorDeployInfo.InputDeployInfo> inputs = odi.inputs;
    for (OperatorDeployInfo.InputDeployInfo input : inputs) {
      if (input.portName == portMeta.getPortName()) {
        idi = input;
        break;
      }
    }
    Assert.assertNotNull("input deploy info " + portMeta.getPortName(), idi);
    return idi;
  }

  private OperatorDeployInfo.OutputDeployInfo getOutputDeployInfo(OperatorDeployInfo odi, LogicalPlan.OutputPortMeta portMeta) {
    OperatorDeployInfo.OutputDeployInfo otdi = null;
    List<OperatorDeployInfo.OutputDeployInfo> outputs = odi.outputs;
    for (OperatorDeployInfo.OutputDeployInfo output : outputs) {
      if (output.portName == portMeta.getPortName()) {
        otdi = output;
        break;
      }
    }
    Assert.assertNotNull("output deploy info " + portMeta.getPortName(), otdi);
    return otdi;
  }

  public static class TestStreamCodec extends DefaultStatefulStreamCodec<Object>
  {

    @Override
    public int getPartition(Object o)
    {
      return o.hashCode()/2;
    }
  }

  public static class TestStreamCodec2 extends DefaultStatefulStreamCodec<Object> {

    @Override
    public int getPartition(Object o)
    {
      return o.hashCode()/3;
    }
  }

  public static class DefaultTestStreamCodec extends DefaultStatefulStreamCodec<Object> {

  }

  public static class DefaultCodecOperator extends GenericTestOperator {
    final static String INPORT_WITH_CODEC = "inportWithCodec";
    @InputPortFieldAnnotation(name = INPORT_WITH_CODEC, optional = true)
    final public transient InputPort<Object> inportWithCodec = new DefaultInputPort<Object>() {
      @Override
      public Class<? extends StreamCodec<Object>> getStreamCodec() {
        return DefaultTestStreamCodec.class;
      }

      @Override
      final public void process(Object payload) {
      }

    };
  }
}
