package com.datatorrent.stram;

import com.datatorrent.stram.engine.StreamingContainer;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.Thread.sleep;

import com.google.common.collect.Sets;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.stram.StramLocalCluster.LocalStreamingContainer;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.WaitCondition;

public class PartitioningTest
{
  private static final Logger LOG = LoggerFactory.getLogger(PartitioningTest.class);
  private static final File TEST_OUTPUT_DIR = new File("target", PartitioningTest.class.getName());

  @Before
  public void setup() throws IOException
  {
    StreamingContainer.eventloop.start();
  }

  @After
  public void teardown()
  {
    StreamingContainer.eventloop.stop();
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
        Operator.Util.shutdown();
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
    dag.getMeta(collector).getAttributes().put(OperatorContext.INITIAL_PARTITION_COUNT, 2);
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

  public static class PartitionLoadWatch implements StatsListener, java.io.Serializable
  {
    private static final long serialVersionUID = 1L;
    final private static ThreadLocal<Map<Integer, Integer>> loadIndicators = new ThreadLocal<Map<Integer, Integer>>();

    @Override
    public Response processStats(BatchedOperatorStats status)
    {
      Response hbr = null;
      Map<Integer, Integer> m = loadIndicators.get();
      if (m != null) {
        Integer l = m.get(status.getOperatorId());
        if (l != null) {
          hbr = new Response();
          hbr.repartitionRequired = true;
          hbr.loadIndicator = l.intValue();
        }
      }
      return hbr;
    }

    public static void put(PTOperator oper, int load) {
      Map<Integer, Integer> m = loadIndicators.get();
      if (m == null) {
        loadIndicators.set(m = new ConcurrentHashMap<Integer, Integer>());
      }
      m.put(oper.getId(), load);
    }

    public static void remove(PTOperator oper) {
      loadIndicators.get().remove(oper.getId());
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

  //@Ignore
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
    dag.setAttribute(collector, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{new PartitionLoadWatch()}));
    dag.addStream("fromInput", input.output, collector.input);

    CollectorOperator singleCollector = dag.addOperator("singleCollector", new CollectorOperator());
    dag.addStream("toSingleCollector", collector.output, singleCollector.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();

    List<PTOperator> partitions = assertNumberPartitions(2, lc, dag.getMeta(collector));
    Set<PTContainer> containers = Sets.newHashSet();
    for (PTOperator oper : partitions) {
      containers.add(oper.getContainer());
    }
    PTOperator splitPartition = partitions.get(0);
    PartitionLoadWatch.put(splitPartition, 1);
    LOG.debug("Triggered split for {}", splitPartition);

    int count = 0;
    long startMillis = System.currentTimeMillis();
    while (count == 0 && startMillis > System.currentTimeMillis() - StramTestSupport.DEFAULT_TIMEOUT_MILLIS) {
      sleep(20); // yield
      count += lc.dnmgr.processEvents();
    }

    partitions = assertNumberPartitions(3, lc, dag.getMeta(collector));
    Assert.assertTrue("container reused", lc.dnmgr.getPhysicalPlan().getContainers().containsAll(containers));

    for (PTContainer container : lc.dnmgr.getPhysicalPlan().getContainers()) {
      Assert.assertEquals("memory", (int)OperatorContext.MEMORY_MB.defaultValue, container.getRequiredMemoryMB());
    }

    // check deployment
    for (PTOperator p: partitions) {
      StramTestSupport.waitForActivation(lc, p);
    }

    PartitionLoadWatch.remove(splitPartition);

    PTOperator planInput = lc.findByLogicalNode(dag.getMeta(input));
    LocalStreamingContainer c = StramTestSupport.waitForActivation(lc, planInput);
    Map<Integer, Node<?>> nodeMap = c.getNodes();
    Assert.assertEquals("number operators " + nodeMap, 1, nodeMap.size());
    @SuppressWarnings({"unchecked"})
    TestInputOperator<Integer> inputDeployed = (TestInputOperator<Integer>)nodeMap.get(planInput.getId()).getOperator();
    Assert.assertNotNull("" + nodeMap, inputDeployed);

    // add tuple that matches the partition key and check that each partition receives it
    ArrayList<Integer> inputTuples = new ArrayList<Integer>();
    for (PTOperator p: partitions) {
      // default partitioning has one port mapping with a single partition key
      inputTuples.add(p.getPartitionKeys().values().iterator().next().partitions.iterator().next());
    }
    inputDeployed.testTuples = Collections.synchronizedList(new ArrayList<List<Integer>>());
    inputDeployed.testTuples.add(inputTuples);

    for (PTOperator p: partitions) {
      Integer expectedTuple = p.getPartitionKeys().values().iterator().next().partitions.iterator().next();
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

  public static class PartitionableInputOperator extends BaseOperator implements InputOperator, Partitioner<PartitionableInputOperator>
  {
    String partitionProperty = "partition";

    @Override
    public void emitTuples()
    {
    }

    @Override
    public Collection<Partition<PartitionableInputOperator>> definePartitions(Collection<Partition<PartitionableInputOperator>> partitions, int incrementalCapacity)
    {
      List<Partition<PartitionableInputOperator>> newPartitions = new ArrayList<Partition<PartitionableInputOperator>>(3);
      Iterator<? extends Partition<PartitionableInputOperator>> iterator = partitions.iterator();
      Partition<PartitionableInputOperator> templatePartition;
      for (int i = 0; i < 3; i++) {
        PartitionableInputOperator op = new PartitionableInputOperator();
        if (iterator.hasNext()) {
          templatePartition = iterator.next();
          op.partitionProperty = templatePartition.getPartitionedInstance().partitionProperty;
        }
        op.partitionProperty += "_" + i;
        newPartitions.add(new DefaultPartition<PartitionableInputOperator>(op));
      }
      return newPartitions;
    }

    @Override
    public void partitioned(Map<Integer, Partition<PartitionableInputOperator>> partitions)
    {
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
      dag.getAttributes().put(LogicalPlan.APPLICATION_PATH, checkpointDir.getPath());

      PartitionableInputOperator input = dag.addOperator("input", new PartitionableInputOperator());
      dag.setAttribute(input, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{new PartitionLoadWatch()}));

      StramLocalCluster lc = new StramLocalCluster(dag);
      lc.setHeartbeatMonitoringEnabled(false);
      lc.runAsync();

      List<PTOperator> partitions = assertNumberPartitions(3, lc, dag.getMeta(input));
      Set<String> partProperties = new HashSet<String>();
      for (PTOperator p: partitions) {
        LocalStreamingContainer c = StramTestSupport.waitForActivation(lc, p);
        Map<Integer, Node<?>> nodeMap = c.getNodes();
        Assert.assertEquals("number operators " + nodeMap, 1, nodeMap.size());
        PartitionableInputOperator inputDeployed = (PartitionableInputOperator)nodeMap.get(p.getId()).getOperator();
        Assert.assertNotNull(inputDeployed);
        partProperties.add(inputDeployed.partitionProperty);
        // move to checkpoint to verify that checkpoint state is updated upon repartition
        Checkpoint checkpoint = new Checkpoint(10L, 0, 0);
        p.checkpoints.add(checkpoint);
        p.setRecoveryCheckpoint(checkpoint);
        new FSStorageAgent(checkpointDir.getPath(), null).save(inputDeployed, p.getId(), 10L);
      }

      Assert.assertEquals("", Sets.newHashSet("partition_0", "partition_1", "partition_2"), partProperties);

      PartitionLoadWatch.put(partitions.get(0), 1);
      int count = 0;
      long startMillis = System.currentTimeMillis();
      while (count == 0 && startMillis > System.currentTimeMillis() - StramTestSupport.DEFAULT_TIMEOUT_MILLIS) {
        count += lc.dnmgr.processEvents();
      }
      PartitionLoadWatch.remove(partitions.get(0));

      partitions = assertNumberPartitions(3, lc, dag.getMeta(input));
      partProperties = new HashSet<String>();
      for (PTOperator p: partitions) {
        LocalStreamingContainer c = StramTestSupport.waitForActivation(lc, p);
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
