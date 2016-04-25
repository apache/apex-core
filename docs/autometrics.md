Apache Apex AutoMetrics
=======================

# Introduction
Metrics collect various statistical information about a process which can be very useful for diagnosis. Auto Metrics in Apex can help monitor operators in a running application.  The goal of *AutoMetric* API is to enable operator developer to define relevant metrics for an operator in a simple way which the platform collects and reports automatically.

# Specifying AutoMetrics in an Operator
An *AutoMetric* can be any object. It can be of a primitive type - int, long, etc. or a complex one. A field or a `get` method in an operator can be annotated with `@AutoMetric` to specify that its value is a metric. After every application end window, the platform collects the values of these fields/methods in a map and sends it to application master.

<a name="lineReceiver"></a>

```java
public class LineReceiver extends BaseOperator
{
 @AutoMetric
 long length;

 @AutoMetric
 long count;

 public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
 {
   @Override
   public void process(String s)
   {
     length += s.length();
     count++;
   }
 };

 @Override
 public void beginWindow(long windowId)
 {
   length = 0;
   count = 0;
 }
}
```

There are 2 auto-metrics declared in the `LineReceiver`. At the end of each application window, the platform will send a map with 2 entries - `[(length, 100), (count, 10)]` to the application master.

# Aggregating AutoMetrics across Partitions
When an operator is partitioned, it is useful to aggregate the values of auto-metrics across all its partitions every window to get a logical view of these metrics. The application master performs these aggregations using metrics aggregators.

The AutoMetric API helps to achieve this by providing an interface for writing aggregators- `AutoMetric.Aggregator`. Any implementation of `AutoMetric.Aggregator` can be set as an operator attribute - `METRICS_AGGREGATOR` for a particular operator which in turn is used for aggregating physical metrics.

## Default aggregators
[`MetricsAggregator`](https://github.com/apache/apex-core/blob/master/common/src/main/java/com/datatorrent/common/metric/MetricsAggregator.java) is a simple implementation of `AutoMetric.Aggregator` that platform uses as a default for summing up primitive types - int, long, float and double.

`MetricsAggregator` is just a collection of `SingleMetricAggregator`s. There are multiple implementations of `SingleMetricAggregator` that perform sum, min, max, avg which are present in Apex core and Apex malhar.

For the `LineReceiver` operator, the application developer need not specify any aggregator. The platform will automatically inject an instance of `MetricsAggregator` that contains two `LongSumAggregator`s - one for `length` and one for `count`. This aggregator will report sum of length and sum of count across all the partitions of `LineReceiver`.


## Building custom aggregators
Platform cannot perform any meaningful aggregations for non-numeric metrics. In such cases, the operator or application developer can write custom aggregators. Let’s say, if the `LineReceiver` was modified to have a complex metric as shown below.

```java
public class AnotherLineReceiver extends BaseOperator
{
  @AutoMetric
  final LineMetrics lineMetrics = new LineMetrics();

  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {
    @Override
    public void process(String s)
    {
      lineMetrics.length += s.length();
      lineMetrics.count++;
    }
  };

  @Override
  public void beginWindow(long windowId)
  {
    lineMetrics.length = 0;
    lineMetrics.count = 0;
  }

  public static class LineMetrics implements Serializable
  {
    long length;
    long count;

    private static final long serialVersionUID = 201511041908L;
  }
}
```

Below is a custom aggregator that can calculate average line length across all partitions of `AnotherLineReceiver`.

```java
public class AvgLineLengthAggregator implements AutoMetric.Aggregator
{

  Map<String, Object> result = Maps.newHashMap();

  @Override
  public Map<String, Object> aggregate(long l, Collection<AutoMetric.PhysicalMetricsContext> collection)
  {
    long totalLength = 0;
    long totalCount = 0;
    for (AutoMetric.PhysicalMetricsContext pmc : collection) {
      AnotherLineReceiver.LineMetrics lm = (AnotherLineReceiver.LineMetrics)pmc.getMetrics().get("lineMetrics");
      totalLength += lm.length;
      totalCount += lm.count;
    }
    result.put("avgLineLength", totalLength/totalCount);
    return result;
  }
}
```
An instance of above aggregator can be specified as the `METRIC_AGGREGATOR` for `AnotherLineReceiver` while creating the DAG as shown below.

```java
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    ...
    AnotherLineReceiver lineReceiver = dag.addOperator("LineReceiver", new AnotherLineReceiver());
    dag.setAttribute(lineReceiver, Context.OperatorContext.METRICS_AGGREGATOR, new AvgLineLengthAggregator());
    ...
  }
```

# Retrieving AutoMetrics

There are two options for retrieving the AutoMetrics:

* Throught DataTorrent Gateway REST API
* Through REST service on the port of the running STRAM


The Gateway REST API provides a way to retrieve the latest AutoMetrics for each logical operator.  For example:

```
GET /ws/v2/applications/{appid}/logicalPlan/operators/{opName}
{
    ...
    "autoMetrics": {
       "count": "71314",
       "length": "27780706"
    },
    "className": "com.datatorrent.autometric.LineReceiver",
    ...
}
```

# System Metrics
System metrics are standard operator metrics provided by the system.  Examples include:

- processed tuples per second
- emitted tuples per second
- total tuples processed
- total tuples emitted
- latency
- CPU percentage
- failure count
- checkpoint elapsed time

The Gateway REST API provides a way to retrieve the latest values for all of the above for each of the logical operators in the application.

```
GET /ws/v2/applications/{appid}/logicalPlan/operators/{opName}
{
    ...
    "cpuPercentageMA": "{cpuPercentageMA}",
    "failureCount": "{failureCount}",
    "latencyMA": "{latencyMA}",  
    "totalTuplesEmitted": "{totalTuplesEmitted}",
    "totalTuplesProcessed": "{totalTuplesProcessed}",
    "tuplesEmittedPSMA": "{tuplesEmittedPSMA}",
    "tuplesProcessedPSMA": "{tuplesProcessedPSMA}",
    ...
}
```

However, just like AutoMetrics, the Gateway only provides the latest metrics.  For historical metrics, we will need the help of [App Data Tracker](http://docs.datatorrent.com/app_data_tracker/).
