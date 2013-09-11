package com.datatorrent.stram;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import static java.lang.Thread.sleep;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.PartitionableOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.StramLocalCluster.LocalStramChild;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.plan.physical.PhysicalPlan.PMapping;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.WaitCondition;
import com.google.common.collect.Sets;

public class PartitioningTest
{
  private static final Logger LOG = LoggerFactory.getLogger(PartitioningTest.class);
  private static final File TEST_OUTPUT_DIR = new File("target", PartitioningTest.class.getName());

  @Before
  public void setup() throws IOException
  {
    StramChild.eventloop.start();
  }

  @After
  public void teardown()
  {
    StramChild.eventloop.stop();
  }

  public static class CollectorOperator extends BaseOperator
  {
    /*
     * Received tuples are stored in a map keyed with the system assigned operator id.
     */
    public static final ConcurrentHashMap<String, List<Object>> receivedTuples = new ConcurrentHashMap<String, List<Object>>();
    private transient int operatorId;
    public String prefix = "";

    @Override
    public void setup(OperatorContext context)
    {
      this.operatorId = context.getId();
    }

    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        LOG.debug("===received tuple {}===", tuple);
        Assert.assertNotNull(CollectorOperator.this.operatorId);
        String id = prefix + CollectorOperator.this.operatorId;
        synchronized (receivedTuples) {
          List<Object> l = receivedTuples.get(id);
          if (l == null) {
            l = Collections.synchronizedList(new ArrayList<Object>());
            //LOG.debug("adding {} {}", id, l);
            receivedTuples.put(id, l);
          }
          l.add(tuple);
        }

        if (output.isConnected()) {
          output.emit(tuple);
        }
      }

    };
    @OutputPortFieldAnnotation(name = "output", optional = true)
    public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>();
  }

  public static class TestInputOperator<T> extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<T> output = new DefaultOutputPort<T>();
    transient boolean first;
    transient long windowId;
    boolean blockEndStream = false;
    /**
     * Tuples to be emitted by the operator, with one entry per window.
     */
    public List<List<T>> testTuples;

    @Override
    public void emitTuples()
    {
      if (testTuples == null || testTuples.isEmpty()) {
        if (blockEndStream) {
          return;
        }
        throw new RuntimeException(new InterruptedException("No more tuples to send!"));
      }

      if (first) {
        List<T> tuples = testTuples.remove(0);
        for (T t: tuples) {
          output.emit(t);
          LOG.debug("sent tuple ==={}===", t);
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
  public void testDefaultPartitioning() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();

    Integer[][] testData = {
      {4, 5}
    };

    CollectorOperator.receivedTuples.clear();

    TestInputOperator<Integer> input = dag.addOperator("input", new TestInputOperator<Integer>());
    input.testTuples = new ArrayList<List<Integer>>();
    for (Integer[] tuples: testData) {
      input.testTuples.add(new ArrayList<Integer>(Arrays.asList(tuples)));
    }
    CollectorOperator collector = dag.addOperator("collector", new CollectorOperator());
    collector.prefix = "" + System.identityHashCode(collector);
    dag.getMeta(collector).getAttributes().attr(OperatorContext.INITIAL_PARTITION_COUNT).set(2);
    dag.addStream("fromInput", input.output, collector.input);

    CollectorOperator merged = dag.addOperator("merged", new CollectorOperator());
    merged.prefix = "" + System.identityHashCode(merged);
    dag.addStream("toMerged", collector.output, merged.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run(); // terminates on end of stream

    List<PTOperator> operators = lc.getPlanOperators(dag.getMeta(collector));
    Assert.assertEquals("number operator instances " + operators, 2, operators.size());

    // one entry for each partition + merged output
    Assert.assertEquals("received tuples " + CollectorOperator.receivedTuples, 3, CollectorOperator.receivedTuples.size());
    Assert.assertEquals("received tuples " + operators.get(0), Arrays.asList(4), CollectorOperator.receivedTuples.get(collector.prefix + operators.get(0).getId()));
    Assert.assertEquals("received tuples " + operators.get(1), Arrays.asList(5), CollectorOperator.receivedTuples.get(collector.prefix + operators.get(1).getId()));

    PTOperator pmerged = lc.findByLogicalNode(dag.getMeta(merged));
    List<Object> tuples = CollectorOperator.receivedTuples.get(merged.prefix + pmerged.getId());
    Assert.assertNotNull("merged tuples " + pmerged, tuples);
    Assert.assertEquals("merged tuples " + pmerged, Sets.newHashSet(testData[0]), Sets.newHashSet(tuples));
  }

  public static class PartitionLoadWatch extends PhysicalPlan.PartitionLoadWatch
  {
    final private static Map<PTOperator, Integer> loadIndicators = new ConcurrentHashMap<PTOperator, Integer>();

    public PartitionLoadWatch(PMapping mapping)
    {
      super(mapping);
      super.evalIntervalMillis = 0;
    }

    @Override
    protected int getLoadIndicator(PTOperator operatorInstance, long tps)
    {
      Integer l = loadIndicators.get(operatorInstance);
      return (l == null) ? 0 : l;
    }

  }

  private static List<PTOperator> assertNumberPartitions(final int count, final StramLocalCluster lc, final LogicalPlan.OperatorMeta ow) throws Exception
  {
    WaitCondition c = new WaitCondition()
    {
      @Override
      public boolean isComplete()
      {
        List<PTOperator> operators = lc.getPlanOperators(ow);
        return (operators.size() == count);
      }

    };
    StramTestSupport.awaitCompletion(c, 10000);
    Assert.assertTrue("Number partitions " + ow, c.isComplete());
    return lc.getPlanOperators(ow);
  }

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testDynamicDefaultPartitioning() throws Exception
  {

    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, 5);
    CollectorOperator.receivedTuples.clear();

    TestInputOperator<Integer> input = dag.addOperator("input", new TestInputOperator<Integer>());
    input.blockEndStream = true;

    CollectorOperator collector = dag.addOperator("partitionedCollector", new CollectorOperator());
    collector.prefix = "" + System.identityHashCode(collector);
    dag.setAttribute(collector, OperatorContext.INITIAL_PARTITION_COUNT, 2);
    dag.setAttribute(collector, OperatorContext.PARTITION_STATS_HANDLER, PartitionLoadWatch.class.getName());
    dag.addStream("fromInput", input.output, collector.input);

    CollectorOperator singleCollector = dag.addOperator("singleCollector", new CollectorOperator());
    dag.addStream("toSingleCollector", collector.output, singleCollector.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();

    List<PTOperator> partitions = assertNumberPartitions(2, lc, dag.getMeta(collector));

    PTOperator splitPartition = partitions.get(0);
    PartitionLoadWatch.loadIndicators.put(splitPartition, 1);

    int count = 0;
    long startMillis = System.currentTimeMillis();
    while (count == 0 && startMillis > System.currentTimeMillis() - StramTestSupport.DEFAULT_TIMEOUT_MILLIS) {
      count += lc.dnmgr.processEvents();
    }

    partitions = assertNumberPartitions(3, lc, dag.getMeta(collector));
    // check deployment
    for (PTOperator p: partitions) {
      StramTestSupport.waitForActivation(lc, p);
    }

    PartitionLoadWatch.loadIndicators.remove(splitPartition);

    PTOperator planInput = lc.findByLogicalNode(dag.getMeta(input));
    LocalStramChild c = StramTestSupport.waitForActivation(lc, planInput);
    Map<Integer, Node<?>> nodeMap = c.getNodes();
    Assert.assertEquals("number operators " + nodeMap, 1, nodeMap.size());
    @SuppressWarnings({"unchecked"})
    TestInputOperator<Integer> inputDeployed = (TestInputOperator<Integer>)nodeMap.get(planInput.getId()).getOperator();
    Assert.assertNotNull("" + nodeMap, inputDeployed);

    // add tuple that matches the partition key and check that each partition receives it
    ArrayList<Integer> inputTuples = new ArrayList<Integer>();
    for (PTOperator p: partitions) {
      // default partitioning has one port mapping with a single partition key
      inputTuples.add(p.getPartition().getPartitionKeys().values().iterator().next().partitions.iterator().next());
    }
    inputDeployed.testTuples = Collections.synchronizedList(new ArrayList<List<Integer>>());
    inputDeployed.testTuples.add(inputTuples);

    for (PTOperator p: partitions) {
      Integer expectedTuple = p.getPartition().getPartitionKeys().values().iterator().next().partitions.iterator().next();
      List<Object> receivedTuples;
      int i = 0;
      while ((receivedTuples = CollectorOperator.receivedTuples.get(collector.prefix + p.getId())) == null || receivedTuples.isEmpty()) {
        if (i++ % 100 == 0) {
          LOG.debug("Waiting for tuple: " + p);
        }
        sleep(20);
      }
      Assert.assertEquals("received " + p, Arrays.asList(expectedTuple), receivedTuples);
    }

    // single output operator to receive tuple from each partition
    List<PTOperator> operators = lc.getPlanOperators(dag.getMeta(singleCollector));
    Assert.assertEquals("number output operator instances " + operators, 1, operators.size());
    StramTestSupport.waitForActivation(lc, operators.get(0)); // ensure redeploy

    List<Object> receivedTuples;
    while ((receivedTuples = CollectorOperator.receivedTuples.get(singleCollector.prefix + operators.get(0).getId())) == null || receivedTuples.size() < inputTuples.size()) {
      LOG.debug("Waiting for tuple: " + operators.get(0) + " expected: " + inputTuples + " received: " + receivedTuples);
      sleep(20);
    }
    Assert.assertEquals("output tuples " + receivedTuples, Sets.newHashSet(inputTuples), Sets.newHashSet(receivedTuples));
    lc.shutdown();

  }

  public static class PartitionableInputOperator extends BaseOperator implements InputOperator, PartitionableOperator
  {
    String partitionProperty = "partition";

    @Override
    public void emitTuples()
    {
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<Partition<?>> definePartitions(Collection<? extends Partition<?>> partitions, int incrementalCount)
    {
      List<Partition<?>> newPartitions = new ArrayList<Partition<?>>(3);
      Iterator<? extends Partition<PartitionableInputOperator>> iterator = (Iterator<? extends Partition<PartitionableInputOperator>>)partitions.iterator();
      Partition<PartitionableInputOperator> templatePartition = null;
      for (int i = 0; i < 3; i++) {
        PartitionableInputOperator op = new PartitionableInputOperator();
        if (iterator.hasNext()) {
          templatePartition = iterator.next();
          op.partitionProperty = templatePartition.getOperator().partitionProperty;
        }
        op.partitionProperty += "_" + i;
        Partition<PartitionableInputOperator> p = templatePartition.getInstance(op);
        newPartitions.add(p);
      }
      return newPartitions;
    }

    /**
     * Tests input operator partitioning with state modification
     *
     * @throws Exception
     */
    @Test
    public void testInputOperatorPartitioning() throws Exception
    {
      File checkpointDir = new File(TEST_OUTPUT_DIR, "testInputOperatorPartitioning");
      LogicalPlan dag = new LogicalPlan();
      dag.getAttributes().attr(LogicalPlan.APPLICATION_PATH).set(checkpointDir.getPath());

      PartitionableInputOperator input = dag.addOperator("input", new PartitionableInputOperator());
      dag.setAttribute(input, OperatorContext.PARTITION_STATS_HANDLER, PartitionLoadWatch.class.getName());

      StramLocalCluster lc = new StramLocalCluster(dag);
      lc.setHeartbeatMonitoringEnabled(false);
      lc.runAsync();

      List<PTOperator> partitions = assertNumberPartitions(3, lc, dag.getMeta(input));
      Set<String> partProperties = new HashSet<String>();
      for (PTOperator p: partitions) {
        LocalStramChild c = StramTestSupport.waitForActivation(lc, p);
        Map<Integer, Node<?>> nodeMap = c.getNodes();
        Assert.assertEquals("number operators " + nodeMap, 1, nodeMap.size());
        PartitionableInputOperator inputDeployed = (PartitionableInputOperator)nodeMap.get(p.getId()).getOperator();
        Assert.assertNotNull(inputDeployed);
        partProperties.add(inputDeployed.partitionProperty);
        // move to checkpoint to verify that checkpoint state is updated upon repartition
        p.checkpointWindows.add(10L);
        p.setRecoveryCheckpoint(10L);
        OutputStream stream = new HdfsStorageAgent(new Configuration(false), checkpointDir.getPath()).getSaveStream(p.getId(), 10L);
        Node.storeOperator(stream, inputDeployed);
        stream.close();
      }

      Assert.assertEquals("", Sets.newHashSet("partition_0", "partition_1", "partition_2"), partProperties);

      PartitionLoadWatch.loadIndicators.put(partitions.get(0), 1);
      int count = 0;
      long startMillis = System.currentTimeMillis();
      while (count == 0 && startMillis > System.currentTimeMillis() - StramTestSupport.DEFAULT_TIMEOUT_MILLIS) {
        count += lc.dnmgr.processEvents();
      }
      PartitionLoadWatch.loadIndicators.remove(partitions.get(0));

      partitions = assertNumberPartitions(3, lc, dag.getMeta(input));
      partProperties = new HashSet<String>();
      for (PTOperator p: partitions) {
        LocalStramChild c = StramTestSupport.waitForActivation(lc, p);
        Map<Integer, Node<?>> nodeMap = c.getNodes();
        Assert.assertEquals("number operators " + nodeMap, 1, nodeMap.size());
        PartitionableInputOperator inputDeployed = (PartitionableInputOperator)nodeMap.get(p.getId()).getOperator();
        Assert.assertNotNull(inputDeployed);
        partProperties.add(inputDeployed.partitionProperty);
      }
      Assert.assertEquals("", Sets.newHashSet("partition_0_0", "partition_1_1", "partition_2_2"), partProperties);

      lc.shutdown();

    }

  }

}
