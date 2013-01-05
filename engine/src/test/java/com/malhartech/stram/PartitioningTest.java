package com.malhartech.stram;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import junit.framework.Assert;

import org.junit.Test;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import com.malhartech.stram.PhysicalPlan.PMapping;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stream.StramTestSupport;
import com.malhartech.stream.StramTestSupport.WaitCondition;

public class PartitioningTest {

  public static class CollectorOperator extends BaseOperator
  {
    /*
     * Received tuples are stored in a map keyed with the system assigned operator id.
     */
    public static final Map<String, List<Object>> receivedTuples = new ConcurrentHashMap<String, List<Object>>();
    private transient String operatorId;

    @Override
    public void setup(OperatorContext context) {
      this.operatorId = context.getId();
    }

    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>(this)
    {
      @Override
      public void process(Object tuple)
      {
        Assert.assertNotNull(CollectorOperator.this.operatorId);
        List<Object> l = receivedTuples.get(CollectorOperator.this.operatorId);
        if (l == null) {
          l = new ArrayList<Object>();
          receivedTuples.put(CollectorOperator.this.operatorId, l);
        }
        l.add(tuple);
      }
    };

    @OutputPortFieldAnnotation(name="output", optional=true)
    public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>(this);

  }

  public static class TestInputOperator<T> extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<T> output = new DefaultOutputPort<T>(this);
    transient boolean first;
    transient long windowId;

    /**
     * Number of tuples to emit in beginWindow.
     * Used to control rate of tuples without making assumptions on total number of windows.
     */
    int emitOnBeginWindowCount = 0;
    T emitOnBeginWindowTuple;

    /**
     * Tuples to be emitted by the operator, with one entry per window.
     */
    public List<List<T>> testTuples;

    @Override
    public void emitTuples()
    {
      if (emitOnBeginWindowCount > 0) {
        return;
      }

      if (testTuples == null || testTuples.isEmpty()) {
        throw new RuntimeException(new InterruptedException("No more tuples to send!"));
      }

      if (first) {
        List<T> tuples = testTuples.remove(0);
        for (T t : tuples) {
          output.emit(t);
        }
        first = false;
      }
    }

    @Override
    public void beginWindow(long windowId)
    {
      this.windowId = windowId;
      first = true;

      for (int i=0; i<this.emitOnBeginWindowCount; i++) {
        output.emit(this.emitOnBeginWindowTuple);
      }
    }
  }

  @Test
  public void testDefaultPartitioning() throws Exception {
    DAG dag = new DAG();

    Integer[][] testData = {
        {4, 5}
    };

    TestInputOperator<Integer> input = dag.addOperator("input", new TestInputOperator<Integer>());
    input.testTuples = new ArrayList<List<Integer>>();
    for (Integer[] tuples : testData) {
      input.testTuples.add(new ArrayList<Integer>(Arrays.asList(tuples)));
    }
    CollectorOperator collector = dag.addOperator("collector", new CollectorOperator());
    dag.getOperatorWrapper(collector).getAttributes().attr(OperatorContext.INITIAL_PARTITION_COUNT).set(2);
    dag.addStream("fromInput", input.output, collector.input);

    CollectorOperator merged = dag.addOperator("merged", new CollectorOperator());
    dag.addStream("toMerged", collector.output, merged.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();

    List<PTOperator> operators = lc.getPlanOperators(dag.getOperatorWrapper(collector));
    Assert.assertEquals("number operator instances " + operators, 2, operators.size());

    // one entry for each partition
    Assert.assertEquals("received tuples " + CollectorOperator.receivedTuples, 2, CollectorOperator.receivedTuples.size());
    Assert.assertEquals("received tuples " + operators.get(0), Arrays.asList(4), CollectorOperator.receivedTuples.get(operators.get(0).id));
    Assert.assertEquals("received tuples " + operators.get(1), Arrays.asList(5), CollectorOperator.receivedTuples.get(operators.get(1).id));
  }

  public static class PartitionLoadWatch extends PhysicalPlan.PartitionLoadWatch {
    final private static Map<PTOperator, Integer> loadIndicators = new ConcurrentHashMap<PTOperator, Integer>();

    public PartitionLoadWatch(PMapping mapping) {
      super(mapping);
      super.evalIntervalMillis = 0;
    }

    @Override
    protected int getLoadIndicator(PTOperator operatorInstance, long tps) {
      Integer l = loadIndicators.get(operatorInstance);
      return (l == null) ? 0 : l;
    }
  }

  @Test
  public void testDefaultDynamicPartitioning() throws Exception {

    DAG dag = new DAG();

    dag.getAttributes().attr(DAG.STRAM_STATS_HANDLER).set(PartitionLoadWatch.class.getName());

    TestInputOperator<Integer> input = dag.addOperator("input", new TestInputOperator<Integer>());
    input.emitOnBeginWindowCount = 50;
    input.emitOnBeginWindowTuple = 0;

    CollectorOperator collector = dag.addOperator("collector", new CollectorOperator());
    dag.getOperatorWrapper(collector).getAttributes().attr(OperatorContext.INITIAL_PARTITION_COUNT).set(2);
    //dag.getOperatorWrapper(collector).getAttributes().attr(OperatorContext.PARTITION_TPS_MIN).set(20);
    //dag.getOperatorWrapper(collector).getAttributes().attr(OperatorContext.PARTITION_TPS_MAX).set(200);
    dag.addStream("fromInput", input.output, collector.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();

    List<PTOperator> partitions = assertNumberPartitions(2, lc, dag.getOperatorWrapper(collector));

    PartitionLoadWatch.loadIndicators.put(partitions.get(0), 1);

    int count = 0;
    long startMillis = System.currentTimeMillis();
    while (count == 0 && startMillis > System.currentTimeMillis() - StramTestSupport.DEFAULT_TIMEOUT_MILLIS ) {
      count += lc.dnmgr.processEvents();
    }

    partitions = assertNumberPartitions(3, lc, dag.getOperatorWrapper(collector));
    // check deployment
    for (PTOperator p : partitions) {
      StramTestSupport.waitForActivation(lc, p);
    }

  //  PTOperator planInput = lc.findByLogicalNode(dag.getOperatorWrapper(input));
  //  LocalStramChild c = StramTestSupport.waitForActivation(lc, planInput);
  //  Map<String, Node<?>> nodeMap = c.getNodes();
  //  Assert.assertEquals("number operators", 1, nodeMap.size());
  //  @SuppressWarnings({ "unchecked" })
  //  TestInputOperator<Integer> inputDeployed = (TestInputOperator<Integer>)nodeMap.get(planInput.id).getOperator();
  //  Assert.assertNotNull(inputDeployed);

    lc.shutdown();

  }

  private List<PTOperator> assertNumberPartitions(final int count, final StramLocalCluster lc, final DAG.OperatorWrapper ow) throws Exception {
    WaitCondition c = new WaitCondition() {
      @Override
      public boolean isComplete() {
        List<PTOperator> operators = lc.getPlanOperators(ow);
        return (operators.size() == count);
      }
    };
    StramTestSupport.awaitCompletion(c, 10000);
    Assert.assertTrue("Number partitions " + ow,  c.isComplete());
    return lc.getPlanOperators(ow);
  };

}
