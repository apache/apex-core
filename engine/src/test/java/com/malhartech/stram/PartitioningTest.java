package com.malhartech.stram;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.*;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import junit.framework.Assert;
import org.junit.Test;

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

    @OutputPortFieldAnnotation(name="outputPort", optional=true)
    public final transient DefaultOutputPort<Object> outputPort = new DefaultOutputPort<Object>(this);

  }

  public static class TestInputOperator<T> extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<T> output = new DefaultOutputPort<T>(this);
    transient boolean first;
    transient long windowId;

    /**
     * Tuples to be emitted by the operator, with one entry per window.
     */
    public List<List<T>> testTuples;

    @Override
    public void emitTuples()
    {
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
    dag.addStream("toMerged", collector.outputPort, merged.input);

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

}
