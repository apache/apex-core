package com.datatorrent.stram.moduleexperiment;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Module;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.ProxyInputPort;
import com.datatorrent.api.Operator.ProxyOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

/**
 * Unit tests for testing Dag expansion with modules and proxy port substitution
 */
public class ModuleTest
{

  /*
   * Input Operator - 1
   */
  public static class DummyInputOperator extends BaseOperator implements InputOperator {

    Random r = new Random();
    public transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();

    @Override
    public void emitTuples()
    {
      output.emit(r.nextInt());
    }
  }

  /*
   * Input Operator - 1.1
   */
  public static class DummyOperatorAfterInput extends BaseOperator {

    public transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>() {
      @Override
      public void process(Integer tuple)
      {
        output.emit(tuple);
      }
    };
    public transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();
  }

  /*
   * Operator - 2
   */
  public static class DummyOperator extends BaseOperator {
    int prop;

    public transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>() {
      @Override
      public void process(Integer tuple)
      {
        LOG.info(tuple.intValue()+" processed");
      }
    };
    public transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();
  }

  /*
   * Output Operator - 3
   */
  public static class DummyOutputOperator extends BaseOperator {
    int prop;

    public transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>() {
      @Override
      public void process(Integer tuple)
      {
        LOG.info(tuple.intValue()+" processed");
      }
    };
  }

  /*
   * Module Definition
   */
  public static class TestModule implements Module {

    public transient ProxyInputPort<Integer> moduleInput = new Operator.ProxyInputPort<Integer>();
    public transient ProxyOutputPort<Integer> moduleOutput = new Operator.ProxyOutputPort<Integer>();

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      LOG.info("Module - PopulateDAG");
      DummyOperator dummyOperator = dag.addOperator("DummyOperator", new DummyOperator());
      moduleInput.set(dummyOperator.input);
      moduleOutput.set(dummyOperator.output);
    }
  }

  @Test
  public void moduleTest(){

    /*
     * Streaming App
     */
    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        LOG.info("Application - PopulateDAG");
        DummyInputOperator dummyInputOperator = dag.addOperator("DummyInputOperator", new DummyInputOperator());
        DummyOperatorAfterInput dummyOperatorAfterInput = dag.addOperator("DummyOperatorAfterInput", new DummyOperatorAfterInput());
        Module m1 = dag.addModule("TestModule1", new TestModule());
        Module m2 = dag.addModule("TestModule2", new TestModule());
        DummyOutputOperator dummyOutputOperator = dag.addOperator("DummyOutputOperator", new DummyOutputOperator());
        dag.addStream("Operator To Operator", dummyInputOperator.output, dummyOperatorAfterInput.input);
        dag.addStream("Operator To Module", dummyOperatorAfterInput.output, ((TestModule)m1).moduleInput);
        dag.addStream("Module To Module", ((TestModule)m1).moduleOutput, ((TestModule)m2).moduleInput);
        dag.addStream("Module To Operator", ((TestModule)m2).moduleOutput, dummyOutputOperator.input);
      }
    };

    Configuration conf = new Configuration(false);
    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(conf);
    LogicalPlan dag = new LogicalPlan();
    lpc.prepareDAG(dag, app, "TestApp");

    Assert.assertEquals(2, dag.getAllModules().size(), 2);
    Assert.assertEquals(5, dag.getAllOperators().size());
    Assert.assertEquals(4, dag.getAllStreams().size());
    dag.validate();
  }

  private static Logger LOG = LoggerFactory.getLogger(ModuleTest.class);
}
