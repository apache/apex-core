/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.stram;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.datatorrent.netlet.util.DTThrowable;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.bus.config.BusConfiguration;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.annotation.Stateless;

import com.datatorrent.bufferserver.auth.AuthManager;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.common.experimental.AppData;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.common.util.FSStorageAgent;
import com.datatorrent.common.util.NumberAggregate;
import com.datatorrent.common.util.Pair;
import com.datatorrent.stram.Journal.Recoverable;
import com.datatorrent.stram.StreamingContainerAgent.ContainerStartRequest;
import com.datatorrent.stram.api.*;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.*;
import com.datatorrent.stram.engine.OperatorResponse;
import com.datatorrent.stram.engine.StreamingContainer;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.plan.logical.LogicalOperatorStatus;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OutputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import com.datatorrent.stram.plan.logical.Operators;
import com.datatorrent.stram.plan.logical.Operators.PortContextPair;
import com.datatorrent.stram.plan.logical.requests.LogicalPlanRequest;
import com.datatorrent.stram.plan.physical.*;
import com.datatorrent.stram.plan.physical.OperatorStatus.PortStatus;
import com.datatorrent.stram.plan.physical.PTOperator.PTInput;
import com.datatorrent.stram.plan.physical.PTOperator.PTOutput;
import com.datatorrent.stram.plan.physical.PTOperator.State;
import com.datatorrent.stram.plan.physical.PhysicalPlan.PlanContext;
import com.datatorrent.stram.util.ConfigUtils;
import com.datatorrent.stram.util.FSJsonLineFile;
import com.datatorrent.stram.util.MovingAverage.MovingAverageLong;
import com.datatorrent.stram.util.SharedPubSubWebSocketClient;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.*;

/**
 * Tracks topology provisioning/allocation to containers<p>
 * <br>
 * The tasks include<br>
 * Provisioning operators one container at a time. Each container gets assigned the operators, streams and its context<br>
 * Monitors run time operations including heartbeat protocol and node status<br>
 * Operator recovery and restart<br>
 * <br>
 *
 * @since 0.3.2
 */
public class StreamingContainerManager implements PlanContext
{
  private final static Logger LOG = LoggerFactory.getLogger(StreamingContainerManager.class);
  public final static String GATEWAY_LOGIN_URL_PATH = "/ws/v2/login";
  public final static String BUILTIN_APPDATA_URL = "builtin";
  public final static String CONTAINERS_INFO_FILENAME_FORMAT = "containers_%d.json";
  public final static String OPERATORS_INFO_FILENAME_FORMAT = "operators_%d.json";
  public final static String APP_META_FILENAME = "meta.json";
  public final static String APP_META_KEY_ATTRIBUTES = "attributes";
  public final static String APP_META_KEY_METRICS = "metrics";
  public static final String EMBEDDABLE_QUERY_NAME_SUFFIX = ".query";

  public final static long LATENCY_WARNING_THRESHOLD_MILLIS = 10 * 60 * 1000; // 10 minutes
  public final static Recoverable SET_OPERATOR_PROPERTY = new SetOperatorProperty();
  public final static Recoverable SET_PHYSICAL_OPERATOR_PROPERTY = new SetPhysicalOperatorProperty();
  public final static int METRIC_QUEUE_SIZE = 1000;

  private final FinalVars vars;
  private final PhysicalPlan plan;
  private final Clock clock;
  private SharedPubSubWebSocketClient wsClient;
  private FSStatsRecorder statsRecorder;
  private FSEventRecorder eventRecorder;
  protected final Map<String, String> containerStopRequests = new ConcurrentHashMap<String, String>();
  protected final ConcurrentLinkedQueue<ContainerStartRequest> containerStartRequests = new ConcurrentLinkedQueue<ContainerStartRequest>();
  protected boolean forcedShutdown = false;
  private final ConcurrentLinkedQueue<Runnable> eventQueue = new ConcurrentLinkedQueue<Runnable>();
  private final AtomicBoolean eventQueueProcessing = new AtomicBoolean();
  private final HashSet<PTContainer> pendingAllocation = Sets.newLinkedHashSet();
  protected String shutdownDiagnosticsMessage = "";
  private long lastResourceRequest = 0;
  private final Map<String, StreamingContainerAgent> containers = new ConcurrentHashMap<String, StreamingContainerAgent>();
  private final List<Pair<PTOperator, Long>> purgeCheckpoints = new ArrayList<Pair<PTOperator, Long>>();
  private final Map<Long, Set<PTOperator>> shutdownOperators = new HashMap<>();
  private CriticalPathInfo criticalPathInfo;
  private final ConcurrentMap<PTOperator, PTOperator> reportStats = Maps.newConcurrentMap();
  private final AtomicBoolean deployChangeInProgress = new AtomicBoolean();
  private int deployChangeCnt;
  private MBassador<StramEvent> eventBus; // event bus for publishing stram events
  final private Journal journal;
  private RecoveryHandler recoveryHandler;
  // window id to node id to end window stats
  private final ConcurrentSkipListMap<Long, Map<Integer, EndWindowStats>> endWindowStatsOperatorMap = new ConcurrentSkipListMap<Long, Map<Integer, EndWindowStats>>();
  private long committedWindowId;
  // (operator id, port name) to timestamp
  private final Map<Pair<Integer, String>, Long> operatorPortLastEndWindowTimestamps = Maps.newConcurrentMap();
  private final Map<Integer, Long> operatorLastEndWindowTimestamps = Maps.newConcurrentMap();
  private long lastStatsTimestamp = System.currentTimeMillis();
  private long currentEndWindowStatsWindowId;
  private long completeEndWindowStatsWindowId;
  private final ConcurrentHashMap<String, MovingAverageLong> rpcLatencies = new ConcurrentHashMap<String, MovingAverageLong>();
  private final AtomicLong nodeToStramRequestIds = new AtomicLong(1);
  private long allocatedMemoryBytes = 0;
  private List<AppDataSource> appDataSources = null;
  private final Cache<Long, Object> commandResponse = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();
  private long lastLatencyWarningTime;
  private transient ExecutorService poolExecutor;
  private FileContext fileContext;

  //logic operator name to a queue of logical metrics. this gets cleared periodically
  private final Map<String, Queue<Pair<Long, Map<String, Object>>>> logicalMetrics = Maps.newConcurrentMap();
  //logical operator name to latest logical metrics.
  private final Map<String, Map<String, Object>> latestLogicalMetrics = Maps.newHashMap();

  //logical operator name to latest counters. exists for backward compatibility.
  private final Map<String, Object> latestLogicalCounters = Maps.newHashMap();

  private final LinkedHashMap<String, ContainerInfo> completedContainers = new LinkedHashMap<String, ContainerInfo>()
  {
    private static final long serialVersionUID = 201405281500L;

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, ContainerInfo> eldest)
    {
      long expireTime = System.currentTimeMillis() - 30 * 60 * 60;
      Iterator<Map.Entry<String, ContainerInfo>> iterator = entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, ContainerInfo> entry = iterator.next();
        if (entry.getValue().finishedTime < expireTime) {
          iterator.remove();
        }
      }
      return false;
    }
  };

  private FSJsonLineFile containerFile;
  private FSJsonLineFile operatorFile;

  private final long startTime = System.currentTimeMillis();

  private static class EndWindowStats
  {
    long emitTimestamp = -1;
    HashMap<String, Long> dequeueTimestamps = new HashMap<String, Long>(); // input port name to end window dequeue time
    Object counters;
    Map<String, Object> metrics;
  }

  public static class CriticalPathInfo
  {
    long latency;
    LinkedList<Integer> path = new LinkedList<Integer>();
  }

  private static class SetOperatorProperty implements Recoverable
  {
    final private String operatorName;
    final private String propertyName;
    final private String propertyValue;

    private SetOperatorProperty()
    {
      this(null, null, null);
    }

    private SetOperatorProperty(String operatorName, String propertyName, String propertyValue)
    {
      this.operatorName = operatorName;
      this.propertyName = propertyName;
      this.propertyValue = propertyValue;
    }

    @Override
    public void read(final Object object, final Input in) throws KryoException
    {
      final StreamingContainerManager scm = (StreamingContainerManager)object;

      final String operatorName = in.readString();
      final String propertyName = in.readString();
      final String propertyValue = in.readString();

      final OperatorMeta logicalOperator = scm.plan.getLogicalPlan().getOperatorMeta(operatorName);
      if (logicalOperator == null) {
        throw new IllegalArgumentException("Unknown operator " + operatorName);
      }

      scm.setOperatorProperty(logicalOperator, propertyName, propertyValue);
    }

    @Override
    public void write(final Output out) throws KryoException
    {
      out.writeString(operatorName);
      out.writeString(propertyName);
      out.writeString(propertyValue);
    }

  }

  private static class SetPhysicalOperatorProperty implements Recoverable
  {
    final private int operatorId;
    final private String propertyName;
    final private String propertyValue;

    private SetPhysicalOperatorProperty()
    {
      this(-1, null, null);
    }

    private SetPhysicalOperatorProperty(int operatorId, String propertyName, String propertyValue)
    {
      this.operatorId = operatorId;
      this.propertyName = propertyName;
      this.propertyValue = propertyValue;
    }

    @Override
    public void read(final Object object, final Input in) throws KryoException
    {
      final StreamingContainerManager scm = (StreamingContainerManager)object;

      final int operatorId = in.readInt();
      final String propertyName = in.readString();
      final String propertyValue = in.readString();

      final PTOperator o = scm.plan.getAllOperators().get(operatorId);
      if (o == null) {
        throw new IllegalArgumentException("Unknown physical operator " + operatorId);
      }
      scm.setPhysicalOperatorProperty(o, propertyName, propertyValue);
    }

    @Override
    public void write(final Output out) throws KryoException
    {
      out.writeInt(operatorId);
      out.writeString(propertyName);
      out.writeString(propertyValue);
    }

  }

  public StreamingContainerManager(LogicalPlan dag, Clock clock)
  {
    this(dag, false, clock);
  }

  public StreamingContainerManager(LogicalPlan dag)
  {
    this(dag, false, new SystemClock());
  }

  public StreamingContainerManager(LogicalPlan dag, boolean enableEventRecording, Clock clock)
  {
    this.clock = clock;
    this.vars = new FinalVars(dag, clock.getTime());
    poolExecutor = Executors.newFixedThreadPool(4);
    // setup prior to plan creation for event recording
    if (enableEventRecording) {
      this.eventBus = new MBassador<StramEvent>(BusConfiguration.Default(1, 1, 1));
    }
    this.plan = new PhysicalPlan(dag, this);
    this.journal = new Journal(this);
    init(enableEventRecording);
  }

  private StreamingContainerManager(CheckpointState checkpointedState, boolean enableEventRecording)
  {
    this.vars = checkpointedState.finals;
    this.clock = new SystemClock();
    poolExecutor = Executors.newFixedThreadPool(4);
    this.plan = checkpointedState.physicalPlan;
    this.eventBus = new MBassador<StramEvent>(BusConfiguration.Default(1, 1, 1));
    this.journal = new Journal(this);
    init(enableEventRecording);
  }

  private void init(boolean enableEventRecording)
  {
    setupWsClient();
    setupRecording(enableEventRecording);
    setupStringCodecs();

    try {
      Path file = new Path(this.vars.appPath);
      URI uri = file.toUri();
      Configuration config = new YarnConfiguration();
      fileContext = uri.getScheme() == null ? FileContext.getFileContext(config) : FileContext.getFileContext(uri, config);
      saveMetaInfo();
      String fileName = String.format(CONTAINERS_INFO_FILENAME_FORMAT, plan.getLogicalPlan().getValue(LogicalPlan.APPLICATION_ATTEMPT_ID));
      this.containerFile = new FSJsonLineFile(fileContext, new Path(this.vars.appPath, fileName), FsPermission.getDefault());
      this.containerFile.append(getAppMasterContainerInfo());
      fileName = String.format(OPERATORS_INFO_FILENAME_FORMAT, plan.getLogicalPlan().getValue(LogicalPlan.APPLICATION_ATTEMPT_ID));
      this.operatorFile = new FSJsonLineFile(fileContext, new Path(this.vars.appPath, fileName), FsPermission.getDefault());
    } catch (IOException ex) {
      throw DTThrowable.wrapIfChecked(ex);
    }
  }

  public Journal getJournal() {
    return journal;
  }

  public final ContainerInfo getAppMasterContainerInfo()
  {
    ContainerInfo ci = new ContainerInfo();
    ci.id = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.toString());
    String nmHost = System.getenv(ApplicationConstants.Environment.NM_HOST.toString());
    String nmPort = System.getenv(ApplicationConstants.Environment.NM_PORT.toString());
    String nmHttpPort = System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.toString());
    ci.state = "ACTIVE";
    ci.jvmName = ManagementFactory.getRuntimeMXBean().getName();
    ci.numOperators = 0;
    YarnConfiguration conf = new YarnConfiguration();

    if (nmHost != null) {
      if (nmPort != null) {
        ci.host = nmHost + ":" + nmPort;
      }
      if (nmHttpPort != null) {
        String nodeHttpAddress = nmHost + ":" + nmHttpPort;
        if (allocatedMemoryBytes == 0) {
          String url = ConfigUtils.getSchemePrefix(conf) + nodeHttpAddress + "/ws/v1/node/containers/" + ci.id;
          WebServicesClient webServicesClient = new WebServicesClient();
          try {
            String content = webServicesClient.process(url, String.class, new WebServicesClient.GetWebServicesHandler<String>());
            JSONObject json = new JSONObject(content);
            int totalMemoryNeededMB = json.getJSONObject("container").getInt("totalMemoryNeededMB");
            if (totalMemoryNeededMB > 0) {
              allocatedMemoryBytes = totalMemoryNeededMB * 1024 * 1024;
            } else {
              LOG.warn("Could not determine the memory allocated for the streaming application master.  Node manager is reporting {} MB from {}", totalMemoryNeededMB, url);
            }
          }
          catch (Exception ex) {
            LOG.warn("Could not determine the memory allocated for the streaming application master", ex);
          }
        }
        ci.containerLogsUrl = ConfigUtils.getSchemePrefix(conf) + nodeHttpAddress + "/node/containerlogs/" + ci.id + "/" + System.getenv(ApplicationConstants.Environment.USER.toString());
        ci.rawContainerLogsUrl = ConfigUtils.getRawContainerLogsUrl(conf, nodeHttpAddress, plan.getLogicalPlan().getAttributes().get(LogicalPlan.APPLICATION_ID), ci.id);
      }
    }
    ci.memoryMBAllocated = (int)(allocatedMemoryBytes / (1024 * 1024));
    ci.memoryMBFree = ((int)(Runtime.getRuntime().freeMemory() / (1024 * 1024)));
    ci.lastHeartbeat = -1;
    ci.startedTime = startTime;
    ci.finishedTime = -1;
    return ci;
  }

  public void updateRPCLatency(String containerId, long latency)
  {
    if (vars.rpcLatencyCompensationSamples > 0) {
      MovingAverageLong latencyMA = rpcLatencies.get(containerId);
      if (latencyMA == null) {
        final MovingAverageLong val = new MovingAverageLong(vars.rpcLatencyCompensationSamples);
        latencyMA = rpcLatencies.putIfAbsent(containerId, val);
        if (latencyMA == null) {
          latencyMA = val;
        }
      }
      latencyMA.add(latency);
    }
  }

  private void setupRecording(boolean enableEventRecording)
  {
    if (this.vars.enableStatsRecording) {
      statsRecorder = new FSStatsRecorder();
      statsRecorder.setBasePath(this.vars.appPath + "/" + LogicalPlan.SUBDIR_STATS);
      statsRecorder.setup();
    }
    if (enableEventRecording) {
      eventRecorder = new FSEventRecorder(plan.getLogicalPlan().getValue(LogicalPlan.APPLICATION_ID));
      eventRecorder.setBasePath(this.vars.appPath + "/" + LogicalPlan.SUBDIR_EVENTS);
      eventRecorder.setWebSocketClient(wsClient);
      eventRecorder.setup();
      eventBus.subscribe(eventRecorder);
    }
  }

  private void setupStringCodecs()
  {
    Map<Class<?>, Class<? extends StringCodec<?>>> codecs = this.plan.getLogicalPlan().getAttributes().get(Context.DAGContext.STRING_CODECS);
    StringCodecs.loadConverters(codecs);
  }

  private void setupWsClient()
  {
    String gatewayAddress = plan.getLogicalPlan().getValue(LogicalPlan.GATEWAY_CONNECT_ADDRESS);
    boolean gatewayUseSsl = plan.getLogicalPlan().getValue(LogicalPlan.GATEWAY_USE_SSL);
    String gatewayUserName = plan.getLogicalPlan().getValue(LogicalPlan.GATEWAY_USER_NAME);
    String gatewayPassword = plan.getLogicalPlan().getValue(LogicalPlan.GATEWAY_PASSWORD);

    if (gatewayAddress != null) {
      try {
        wsClient = new SharedPubSubWebSocketClient((gatewayUseSsl ? "wss://" : "ws://") + gatewayAddress + "/pubsub", 500);
        if (gatewayUserName != null && gatewayPassword != null) {
          wsClient.setLoginUrl((gatewayUseSsl ? "https://" : "http://") + gatewayAddress + GATEWAY_LOGIN_URL_PATH);
          wsClient.setUserName(gatewayUserName);
          wsClient.setPassword(gatewayPassword);
        }
        wsClient.setup();
      }
      catch (Exception ex) {
        LOG.warn("Cannot establish websocket connection to {}", gatewayAddress, ex);
      }
    }
  }

  public void teardown()
  {
    if (eventBus != null) {
      eventBus.shutdown();
    }
    if (eventRecorder != null) {
      eventRecorder.teardown();
    }
    if (statsRecorder != null) {
      statsRecorder.teardown();
    }

    IOUtils.closeQuietly(containerFile);
    IOUtils.closeQuietly(operatorFile);
    if(poolExecutor != null) {
      poolExecutor.shutdown();
    }
  }

  public void subscribeToEvents(Object listener)
  {
    if (eventBus != null) {
      eventBus.subscribe(listener);
    }
  }

  public PhysicalPlan getPhysicalPlan()
  {
    return plan;
  }

  public long getCommittedWindowId()
  {
    return committedWindowId;
  }

  public boolean isGatewayConnected()
  {
    return wsClient != null && wsClient.isConnectionOpen();
  }

  public SharedPubSubWebSocketClient getWsClient()
  {
    return wsClient;
  }

  private String convertAppDataUrl(String url)
  {
    if (BUILTIN_APPDATA_URL.equals(url)) {
      return url;
    }
    /*else if (url != null) {      String messageProxyUrl = this.plan.getLogicalPlan().getAttributes().get(Context.DAGContext.APPLICATION_DATA_MESSAGE_PROXY_URL);
      if (messageProxyUrl != null) {
        StringBuilder convertedUrl = new StringBuilder(messageProxyUrl);
        convertedUrl.append("?url=");
        try {
          convertedUrl.append(URLEncoder.encode(url, "UTF-8"));
          return convertedUrl.toString();
        } catch (UnsupportedEncodingException ex) {
          LOG.warn("URL {} cannot be encoded", url);
        }
      }
    }
     */
    LOG.warn("App Data URL {} cannot be converted for the client.", url);
    return url;
  }

  private final Object appDataSourcesLock = new Object();

  public List<AppDataSource> getAppDataSources()
  {
    synchronized (appDataSourcesLock) {
      if (appDataSources == null) {
        appDataSources = new ArrayList<AppDataSource>();
        operators:
        for (LogicalPlan.OperatorMeta operatorMeta : plan.getLogicalPlan().getAllOperators()) {
          Map<LogicalPlan.InputPortMeta, LogicalPlan.StreamMeta> inputStreams = operatorMeta.getInputStreams();
          Map<LogicalPlan.OutputPortMeta, LogicalPlan.StreamMeta> outputStreams = operatorMeta.getOutputStreams();

          String queryOperatorName = null;
          String queryUrl = null;
          String queryTopic = null;

          boolean hasEmbeddedQuery = false;

          //Discover embeddable query connectors
          if (operatorMeta.getOperator() instanceof AppData.Store<?>) {
            AppData.Store<?> store = (AppData.Store<?>)operatorMeta.getOperator();
            AppData.EmbeddableQueryInfoProvider<?> embeddableQuery = store.getEmbeddableQueryInfoProvider();

            if (embeddableQuery != null) {
              hasEmbeddedQuery = true;
              queryOperatorName = operatorMeta.getName() + EMBEDDABLE_QUERY_NAME_SUFFIX;
              queryUrl = embeddableQuery.getAppDataURL();
              queryTopic = embeddableQuery.getTopic();
            }
          }

          //Discover separate query operators
          LOG.warn("DEBUG: looking at operator {} {}", operatorMeta.getName(), Thread.currentThread().getId());
          for (Map.Entry<LogicalPlan.InputPortMeta, LogicalPlan.StreamMeta> entry : inputStreams.entrySet()) {
            LogicalPlan.InputPortMeta portMeta = entry.getKey();
            if (portMeta.isAppDataQueryPort()) {
              if (queryUrl == null) {
                OperatorMeta queryOperatorMeta = entry.getValue().getSource().getOperatorMeta();
                if (queryOperatorMeta.getOperator() instanceof AppData.ConnectionInfoProvider) {
                  if (!hasEmbeddedQuery) {
                    AppData.ConnectionInfoProvider queryOperator = (AppData.ConnectionInfoProvider)queryOperatorMeta.getOperator();
                    queryOperatorName = queryOperatorMeta.getName();
                    queryUrl = queryOperator.getAppDataURL();
                    queryTopic = queryOperator.getTopic();
                  } else {
                    LOG.warn("An embeddable query connector and the {} query operator were discovered. " +
                             "The query operator will be ignored and the embeddable query connector will be used instead.",
                             operatorMeta.getName());
                  }
                }
              } else {
                LOG.warn("Multiple query ports found in operator {}. Ignoring the App Data Source.", operatorMeta.getName());
                continue operators;
              }
            }
          }

          for (Map.Entry<LogicalPlan.OutputPortMeta, LogicalPlan.StreamMeta> entry : outputStreams.entrySet()) {
            LogicalPlan.OutputPortMeta portMeta = entry.getKey();
            LOG.warn("DEBUG: looking at port {} {}", portMeta.getPortName(), Thread.currentThread().getId());

            if (portMeta.isAppDataResultPort()) {
              AppDataSource appDataSource = new AppDataSource();
              appDataSource.setType(AppDataSource.Type.DAG);
              appDataSource.setOperatorName(operatorMeta.getName());
              appDataSource.setPortName(portMeta.getPortName());

              if (queryOperatorName == null) {
                LOG.warn("There is no query operator for the App Data Source {}.{}. Ignoring the App Data Source.", operatorMeta.getName(), portMeta.getPortName());
                continue;
              }
              appDataSource.setQueryOperatorName(queryOperatorName);
              appDataSource.setQueryTopic(queryTopic);
              appDataSource.setQueryUrl(convertAppDataUrl(queryUrl));
              List<LogicalPlan.InputPortMeta> sinks = entry.getValue().getSinks();
              if (sinks.isEmpty()) {
                LOG.warn("There is no result operator for the App Data Source {}.{}. Ignoring the App Data Source.", operatorMeta.getName(), portMeta.getPortName());
                continue;
              }
              if (sinks.size() > 1) {
                LOG.warn("There are multiple result operators for the App Data Source {}.{}. Ignoring the App Data Source.", operatorMeta.getName(), portMeta.getPortName());
                continue;
              }
              OperatorMeta resultOperatorMeta = sinks.get(0).getOperatorWrapper();
              if (resultOperatorMeta.getOperator() instanceof AppData.ConnectionInfoProvider) {
                AppData.ConnectionInfoProvider resultOperator = (AppData.ConnectionInfoProvider) resultOperatorMeta.getOperator();
                appDataSource.setResultOperatorName(resultOperatorMeta.getName());
                appDataSource.setResultTopic(resultOperator.getTopic());
                appDataSource.setResultUrl(convertAppDataUrl(resultOperator.getAppDataURL()));
                AppData.AppendQueryIdToTopic queryIdAppended = resultOperator.getClass().getAnnotation(AppData.AppendQueryIdToTopic.class);
                if (queryIdAppended != null && queryIdAppended.value()) {
                  appDataSource.setResultAppendQIDTopic(true);
                }
              } else {
                LOG.warn("Result operator for the App Data Source {}.{} does not implement the right interface. Ignoring the App Data Source.", operatorMeta.getName(), portMeta.getPortName());
                continue;
              }
              LOG.warn("DEBUG: Adding appDataSource {} {}", appDataSource.getName(), Thread.currentThread().getId());
              appDataSources.add(appDataSource);
            }
          }
        }
      }
    }
    return appDataSources;
  }

  public Map<String, Map<String, Object>> getLatestLogicalMetrics()
  {
    return latestLogicalMetrics;
  }


  /**
   * Check periodically that deployed containers phone home.
   * Run from the master main loop (single threaded access).
   */
  public void monitorHeartbeat()
  {
    long currentTms = clock.getTime();

    // look for resource allocation timeout
    if (!pendingAllocation.isEmpty()) {
      // look for resource allocation timeout
      if (lastResourceRequest + plan.getLogicalPlan().getValue(LogicalPlan.RESOURCE_ALLOCATION_TIMEOUT_MILLIS) < currentTms) {
        String msg = String.format("Shutdown due to resource allocation timeout (%s ms) waiting for %s containers", currentTms - lastResourceRequest, pendingAllocation.size());
        LOG.warn(msg);
        for (PTContainer c : pendingAllocation) {
          LOG.warn("Waiting for resource: {}m priority: {} {}", c.getRequiredMemoryMB(), c.getResourceRequestPriority(), c);
        }
        shutdownAllContainers(msg);
        this.forcedShutdown = true;
      }
      else {
        for (PTContainer c : pendingAllocation) {
          LOG.debug("Waiting for resource: {}m {}", c.getRequiredMemoryMB(), c);
        }
      }
    }

    // monitor currently deployed containers
    for (StreamingContainerAgent sca : containers.values()) {
      PTContainer c = sca.container;

      if (!pendingAllocation.contains(c) && c.getExternalId() != null) {
        if (sca.lastHeartbeatMillis == 0) {
          //LOG.debug("{} {} {}", c.getExternalId(), currentTms - sca.createdMillis, this.vars.heartbeatTimeoutMillis);
          // container allocated but process was either not launched or is not able to phone home
          if (currentTms - sca.createdMillis > 2 * this.vars.heartbeatTimeoutMillis) {
            LOG.info("Container {}@{} startup timeout ({} ms).", c.getExternalId(), c.host, currentTms - sca.createdMillis);
            containerStopRequests.put(c.getExternalId(), c.getExternalId());
          }
        }
        else {
          if (currentTms - sca.lastHeartbeatMillis > this.vars.heartbeatTimeoutMillis) {
            if (!isApplicationIdle()) {
              // request stop (kill) as process may still be hanging around (would have been detected by Yarn otherwise)
              LOG.info("Container {}@{} heartbeat timeout ({} ms).", c.getExternalId(), c.host, currentTms - sca.lastHeartbeatMillis);
              containerStopRequests.put(c.getExternalId(), c.getExternalId());
            }
          }
        }
      }
    }

    // events that may modify the plan
    processEvents();

    committedWindowId = updateCheckpoints(false);
    calculateEndWindowStats();
    if (this.vars.enableStatsRecording) {
      recordStats(currentTms);
    }
  }

  private void recordStats(long currentTms)
  {
    try {
      statsRecorder.recordContainers(containers, currentTms);
      statsRecorder.recordOperators(getOperatorInfoList(), currentTms);
    }
    catch (Exception ex) {
      LOG.warn("Exception caught when recording stats", ex);
    }
  }

  private void calculateEndWindowStats()
  {
    if (!endWindowStatsOperatorMap.isEmpty()) {
      Set<Integer> allCurrentOperators = plan.getAllOperators().keySet();

      if (endWindowStatsOperatorMap.size() > this.vars.maxWindowsBehindForStats) {
        LOG.warn("Some operators are behind for more than {} windows! Trimming the end window stats map", this.vars.maxWindowsBehindForStats);
        while (endWindowStatsOperatorMap.size() > this.vars.maxWindowsBehindForStats) {
          LOG.debug("Removing incomplete end window stats for window id {}. Collected operator set: {}. Complete set: {}",
            endWindowStatsOperatorMap.firstKey(),
            endWindowStatsOperatorMap.get(endWindowStatsOperatorMap.firstKey()).keySet(), allCurrentOperators);
          endWindowStatsOperatorMap.remove(endWindowStatsOperatorMap.firstKey());
        }
      }
      //logicalMetrics.clear();
      int numOperators = allCurrentOperators.size();
      Long windowId = endWindowStatsOperatorMap.firstKey();
      while (windowId != null) {
        Map<Integer, EndWindowStats> endWindowStatsMap = endWindowStatsOperatorMap.get(windowId);
        Set<Integer> endWindowStatsOperators = endWindowStatsMap.keySet();

        aggregateMetrics(windowId, endWindowStatsMap);

        if (allCurrentOperators.containsAll(endWindowStatsOperators)) {
          if (endWindowStatsMap.size() < numOperators) {
            if (windowId < completeEndWindowStatsWindowId) {
              LOG.debug("Disregarding stale end window stats for window {}", windowId);
              endWindowStatsOperatorMap.remove(windowId);
            }
            else {
              break;
            }
          }
          else {
            // collected data from all operators for this window id.  start latency calculation
            List<OperatorMeta> rootOperatorMetas = plan.getLogicalPlan().getRootOperators();
            Set<PTOperator> endWindowStatsVisited = new HashSet<PTOperator>();
            Set<PTOperator> leafOperators = new HashSet<PTOperator>();
            for (OperatorMeta root : rootOperatorMetas) {
              List<PTOperator> rootOperators = plan.getOperators(root);
              for (PTOperator rootOperator : rootOperators) {
                // DFS for visiting the operators for latency calculation
                LOG.debug("Calculating latency starting from operator {}", rootOperator.getId());
                calculateLatency(rootOperator, endWindowStatsMap, endWindowStatsVisited, leafOperators);
              }
            }
            CriticalPathInfo cpi = new CriticalPathInfo();
            //LOG.debug("Finding critical path...");
            cpi.latency = findCriticalPath(endWindowStatsMap, leafOperators, cpi.path);
            criticalPathInfo = cpi;
            endWindowStatsOperatorMap.remove(windowId);
            currentEndWindowStatsWindowId = windowId;
          }
        }
        else {
          // the old stats contains operators that do not exist any more
          // this is probably right after a partition happens.
          LOG.debug("Stats for non-existent operators detected. Disregarding end window stats for window {}", windowId);
          endWindowStatsOperatorMap.remove(windowId);
        }
        windowId = endWindowStatsOperatorMap.higherKey(windowId);
      }
    }
  }

  private void aggregateMetrics(long windowId, Map<Integer, EndWindowStats> endWindowStatsMap)
  {
    Collection<OperatorMeta> logicalOperators = getLogicalPlan().getAllOperators();
    //for backward compatibility
    for (OperatorMeta operatorMeta : logicalOperators) {
      Context.CountersAggregator aggregator = operatorMeta.getValue(OperatorContext.COUNTERS_AGGREGATOR);
      if (aggregator == null) {
        continue;
      }
      Collection<PTOperator> physicalOperators = plan.getAllOperators(operatorMeta);
      List<Object> counters = Lists.newArrayList();
      for (PTOperator operator : physicalOperators) {
        EndWindowStats stats = endWindowStatsMap.get(operator.getId());
        if (stats != null && stats.counters != null) {
          counters.add(stats.counters);
        }
      }
      if (counters.size() > 0) {
        Object aggregate = aggregator.aggregate(counters);
        latestLogicalCounters.put(operatorMeta.getName(), aggregate);
      }
    }

    for (OperatorMeta operatorMeta : logicalOperators) {
      AutoMetric.Aggregator aggregator = operatorMeta.getMetricAggregatorMeta() != null ?
        operatorMeta.getMetricAggregatorMeta().getAggregator() : null;
      if (aggregator == null) {
        continue;
      }
      Collection<PTOperator> physicalOperators = plan.getAllOperators(operatorMeta);
      List<AutoMetric.PhysicalMetricsContext> metricPool = Lists.newArrayList();

      for (PTOperator operator : physicalOperators) {
        EndWindowStats stats = endWindowStatsMap.get(operator.getId());
        if (stats != null && stats.metrics != null) {
          PhysicalMetricsContextImpl physicalMetrics = new PhysicalMetricsContextImpl(operator.getId(), stats.metrics);
          metricPool.add(physicalMetrics);
        }
      }
      Map<String, Object> lm = aggregator.aggregate(windowId, metricPool);

      if (lm != null && lm.size() > 0) {
        Queue<Pair<Long, Map<String, Object>>> windowMetrics = logicalMetrics.get(operatorMeta.getName());
        if (windowMetrics == null) {
          windowMetrics = new LinkedBlockingQueue<Pair<Long, Map<String, Object>>>(METRIC_QUEUE_SIZE)
          {
            @Override
            public boolean add(Pair<Long, Map<String, Object>> longMapPair)
            {
              if (remainingCapacity() <= 1) {
                remove();
              }
              return super.add(longMapPair);
            }
          };
          logicalMetrics.put(operatorMeta.getName(), windowMetrics);
        }
        LOG.debug("Adding to logical metrics for {}", operatorMeta.getName());
        windowMetrics.add(new Pair<Long, Map<String, Object>>(windowId, lm));
        Map<String, Object> oldValue = latestLogicalMetrics.put(operatorMeta.getName(), lm);
        if (oldValue == null) {
          try {
            saveMetaInfo();
          } catch (IOException ex) {
            LOG.error("Cannot save application meta info to DFS. App data sources will not be available.", ex);
          }
        }
      }
    }
  }

  /**
   * This method is for saving meta information about this application in HDFS -- the meta information that generally
   * does not change across multiple attempts
   */
  private void saveMetaInfo() throws IOException
  {
    Path file = new Path(this.vars.appPath, APP_META_FILENAME + "." + System.nanoTime());
    try (FSDataOutputStream os = fileContext.create(file, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE), Options.CreateOpts.CreateParent.createParent())) {      JSONObject top = new JSONObject();
      JSONObject attributes = new JSONObject();
      for (Map.Entry<Attribute<?>, Object> entry : this.plan.getLogicalPlan().getAttributes().entrySet()) {
        attributes.put(entry.getKey().getSimpleName(), entry.getValue());
      }
      JSONObject autoMetrics = new JSONObject();
      for (Map.Entry<String, Map<String, Object>> entry : latestLogicalMetrics.entrySet()) {
        autoMetrics.put(entry.getKey(), new JSONArray(entry.getValue().keySet()));
      }
      top.put(APP_META_KEY_ATTRIBUTES, attributes);
      top.put(APP_META_KEY_METRICS, autoMetrics);
      os.write(top.toString().getBytes());
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
    Path origPath = new Path(this.vars.appPath, APP_META_FILENAME);
    fileContext.rename(file, origPath, Options.Rename.OVERWRITE);
  }

  public Queue<Pair<Long, Map<String, Object>>> getWindowMetrics(String operatorName)
  {
    return logicalMetrics.get(operatorName);
  }

  private void calculateLatency(PTOperator oper, Map<Integer, EndWindowStats> endWindowStatsMap, Set<PTOperator> endWindowStatsVisited, Set<PTOperator> leafOperators)
  {
    endWindowStatsVisited.add(oper);
    OperatorStatus operatorStatus = oper.stats;

    EndWindowStats endWindowStats = endWindowStatsMap.get(oper.getId());
    if (endWindowStats == null) {
      LOG.info("End window stats is null for operator {}, probably a new operator after partitioning", oper);
      return;
    }

    // find the maximum end window emit time from all input ports
    long upstreamMaxEmitTimestamp = -1;
    PTOperator upstreamMaxEmitTimestampOperator = null;
    for (PTOperator.PTInput input : oper.getInputs()) {
      if (null != input.source.source) {
        PTOperator upstreamOp = input.source.source;
        EndWindowStats upstreamEndWindowStats = endWindowStatsMap.get(upstreamOp.getId());
        if (upstreamEndWindowStats == null) {
          LOG.info("End window stats is null for operator {}", oper);
          return;
        }
        long adjustedEndWindowEmitTimestamp = upstreamEndWindowStats.emitTimestamp;
        MovingAverageLong rpcLatency = rpcLatencies.get(upstreamOp.getContainer().getExternalId());
        if (rpcLatency != null) {
          adjustedEndWindowEmitTimestamp += rpcLatency.getAvg();
        }
        if (adjustedEndWindowEmitTimestamp > upstreamMaxEmitTimestamp) {
          upstreamMaxEmitTimestamp = adjustedEndWindowEmitTimestamp;
          upstreamMaxEmitTimestampOperator = upstreamOp;
        }
      }
    }

    if (upstreamMaxEmitTimestamp > 0) {
      long adjustedEndWindowEmitTimestamp = endWindowStats.emitTimestamp;
      MovingAverageLong rpcLatency = rpcLatencies.get(oper.getContainer().getExternalId());
      if (rpcLatency != null) {
        adjustedEndWindowEmitTimestamp += rpcLatency.getAvg();
      }
      if (upstreamMaxEmitTimestamp <= adjustedEndWindowEmitTimestamp) {
        LOG.debug("Adding {} to latency MA for {}", adjustedEndWindowEmitTimestamp - upstreamMaxEmitTimestamp, oper);
        operatorStatus.latencyMA.add(adjustedEndWindowEmitTimestamp - upstreamMaxEmitTimestamp);
      } else {
        operatorStatus.latencyMA.add(0);
        if (lastLatencyWarningTime < System.currentTimeMillis() - LATENCY_WARNING_THRESHOLD_MILLIS) {
          LOG.warn("Latency calculation for this operator may not be correct because upstream end window timestamp is greater than this operator's end window timestamp: {} ({}) > {} ({}). Please verify that the system clocks are in sync in your cluster. You can also try tweaking the RPC_LATENCY_COMPENSATION_SAMPLES application attribute (currently set to {}).",
                  upstreamMaxEmitTimestamp, upstreamMaxEmitTimestampOperator, adjustedEndWindowEmitTimestamp, oper, this.vars.rpcLatencyCompensationSamples);
          lastLatencyWarningTime = System.currentTimeMillis();
        }
      }
    }

    if (oper.getOutputs().isEmpty()) {
      // it is a leaf operator
      leafOperators.add(oper);
    }
    else {
      for (PTOperator.PTOutput output : oper.getOutputs()) {
        for (PTOperator.PTInput input : output.sinks) {
          if (input.target != null) {
            PTOperator downStreamOp = input.target;
            if (!endWindowStatsVisited.contains(downStreamOp)) {
              calculateLatency(downStreamOp, endWindowStatsMap, endWindowStatsVisited, leafOperators);
            }
          }
        }
      }
    }
  }
  /*
   * returns cumulative latency
   */
  private long findCriticalPath(Map<Integer, EndWindowStats> endWindowStatsMap, Set<PTOperator> operators, LinkedList<Integer> criticalPath)
  {
    long maxEndWindowTimestamp = 0;
    PTOperator maxOperator = null;
    for (PTOperator operator : operators) {
      EndWindowStats endWindowStats = endWindowStatsMap.get(operator.getId());
      if (maxEndWindowTimestamp < endWindowStats.emitTimestamp) {
        maxEndWindowTimestamp = endWindowStats.emitTimestamp;
        maxOperator = operator;
      }
    }
    if (maxOperator == null) {
      return 0;
    }
    criticalPath.addFirst(maxOperator.getId());
    OperatorStatus operatorStatus = maxOperator.stats;

    operators.clear();
    if (maxOperator.getInputs() == null || maxOperator.getInputs().isEmpty()) {
      return operatorStatus.latencyMA.getAvg();
    }
    for (PTOperator.PTInput input : maxOperator.getInputs()) {
      if (null != input.source.source) {
        operators.add(input.source.source);
      }
    }
    return operatorStatus.latencyMA.getAvg() + findCriticalPath(endWindowStatsMap, operators, criticalPath);
  }

  public int processEvents()
  {
    for (PTOperator o : reportStats.keySet()) {
      List<OperatorStats> stats = o.stats.listenerStats.poll();
      if (stats != null) {
        // append into single list
        List<OperatorStats> moreStats;
        while ((moreStats = o.stats.listenerStats.poll()) != null) {
          stats.addAll(moreStats);
        }
      }
      o.stats.lastWindowedStats = stats;
      if (o.stats.lastWindowedStats != null) {
        // call listeners only with non empty window list
        if (o.statsListeners != null) {
          plan.onStatusUpdate(o);
        }
      }
      reportStats.remove(o);
    }

    if (!this.shutdownOperators.isEmpty()) {
      synchronized (this.shutdownOperators) {
        Iterator<Map.Entry<Long, Set<PTOperator>>> it = shutdownOperators.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry<Long, Set<PTOperator>> windowAndOpers = it.next();
          if (windowAndOpers.getKey().longValue() > this.committedWindowId) {
            // wait until window is committed
            continue;
          } else {
            LOG.info("Removing inactive operators at window {} {}", Codec.getStringWindowId(windowAndOpers.getKey()), windowAndOpers.getValue());
            for (PTOperator oper : windowAndOpers.getValue()) {
              plan.removeTerminatedPartition(oper);
            }
            it.remove();
          }
        }
      }
    }

    if (!eventQueue.isEmpty()) {
      for (PTOperator oper : plan.getAllOperators().values()) {
        if (oper.getState() != PTOperator.State.ACTIVE) {
          LOG.debug("Skipping plan updates due to inactive operator {} {}", oper, oper.getState());
          return 0;
        }
      }
    }

    int count = 0;
    Runnable command;
    while ((command = this.eventQueue.poll()) != null) {
      eventQueueProcessing.set(true);
      try {
        command.run();
        count++;
      }
      catch (Exception e) {
        // TODO: handle error
        LOG.error("Failed to execute {}", command, e);
      }
      eventQueueProcessing.set(false);
    }

    if (count > 0) {
      try {
        checkpoint();
      }
      catch (Exception e) {
        throw new RuntimeException("Failed to checkpoint state.", e);
      }
    }

    return count;
  }

  /**
   * Schedule container restart. Called by Stram after a container was terminated
   * and requires recovery (killed externally, or after heartbeat timeout). <br>
   * Recovery will resolve affected operators (within the container and
   * everything downstream with respective recovery checkpoint states).
   * Dependent operators will be undeployed and buffer server connections reset prior to
   * redeploy to recovery checkpoint.
   *
   * @param containerId
   */
  public void scheduleContainerRestart(String containerId)
  {
    StreamingContainerAgent cs = this.getContainerAgent(containerId);
    if (cs == null || cs.shutdownRequested) {
      // the container is no longer used / was released by us
      return;
    }

    LOG.info("Initiating recovery for {}@{}", containerId, cs.container.host);

    cs.container.setState(PTContainer.State.KILLED);
    cs.container.bufferServerAddress = null;
    cs.container.setResourceRequestPriority(-1);
    cs.container.setAllocatedMemoryMB(0);
    cs.container.setAllocatedVCores(0);

    // resolve dependencies
    UpdateCheckpointsContext ctx = new UpdateCheckpointsContext(clock);
    for (PTOperator oper : cs.container.getOperators()) {
      updateRecoveryCheckpoints(oper, ctx);
    }
    includeLocalUpstreamOperators(ctx);

    // redeploy cycle for all affected operators
    LOG.info("Affected operators {}", ctx.visited);
    deploy(Collections.<PTContainer>emptySet(), ctx.visited, Sets.newHashSet(cs.container), ctx.visited);
  }

  /**
   * Transitively add operators that are container local to the dependency set.
   * (All downstream operators were traversed during checkpoint update.)
   *
   * @param ctx
   */
  private void includeLocalUpstreamOperators(UpdateCheckpointsContext ctx)
  {
    Set<PTOperator> newOperators = Sets.newHashSet();
    // repeat until no more local upstream operators are found
    do {
      newOperators.clear();
      for (PTOperator oper : ctx.visited) {
        for (PTInput input : oper.getInputs()) {
          if (input.source.source.getContainer() == oper.getContainer()) {
            if (!ctx.visited.contains(input.source.source)) {
              newOperators.add(input.source.source);
            }
          }
        }
      }
      if (!newOperators.isEmpty()) {
        for (PTOperator oper : newOperators) {
          updateRecoveryCheckpoints(oper, ctx);
        }
      }
    } while (!newOperators.isEmpty());
  }

  public void removeContainerAgent(String containerId)
  {
    LOG.debug("Removing container agent {}", containerId);
    StreamingContainerAgent containerAgent = containers.remove(containerId);
    if (containerAgent != null) {
      // record operator stop for this container
      for (PTOperator oper : containerAgent.container.getOperators()) {
        StramEvent ev = new StramEvent.StopOperatorEvent(oper.getName(), oper.getId(), containerId);
        recordEventAsync(ev);
      }
      containerAgent.container.setFinishedTime(System.currentTimeMillis());
      containerAgent.container.setState(PTContainer.State.KILLED);
      completedContainers.put(containerId, containerAgent.getContainerInfo());
    }
  }

  public Collection<ContainerInfo> getCompletedContainerInfo()
  {
    return Collections.unmodifiableCollection(completedContainers.values());
  }

  public static class ContainerResource
  {
    public final String containerId;
    public final String host;
    public final int memoryMB;
    public final int vCores;
    public final int priority;
    public final String nodeHttpAddress;

    public ContainerResource(int priority, String containerId, String host, int memoryMB, int vCores, String nodeHttpAddress)
    {
      this.containerId = containerId;
      this.host = host;
      this.memoryMB = memoryMB;
      this.vCores = vCores;
      this.priority = priority;
      this.nodeHttpAddress = nodeHttpAddress;

    }

    /**
     * @return String
     */
    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("containerId", this.containerId)
        .append("host", this.host)
        .append("memoryMB", this.memoryMB)
        .toString();
    }

  }

  /**
   * Assign operators to allocated container resource.
   *
   * @param resource
   * @param bufferServerAddr
   * @return streaming container agent
   */
  public StreamingContainerAgent assignContainer(ContainerResource resource, InetSocketAddress bufferServerAddr)
  {
    PTContainer container = null;
    // match container waiting for resource
    for (PTContainer c : pendingAllocation) {
      if (c.getState() == PTContainer.State.NEW || c.getState() == PTContainer.State.KILLED) {
        if (c.getResourceRequestPriority() == resource.priority) {
          container = c;
          break;
        }
      }
    }

    if (container == null) {
      LOG.debug("No container matching allocated resource {}", resource);
      LOG.debug("Containers waiting for allocation {}", pendingAllocation);
      return null;
    }

    pendingAllocation.remove(container);
    container.setState(PTContainer.State.ALLOCATED);
    if (container.getExternalId() != null) {
      LOG.info("Removing container agent {}", container.getExternalId());
      this.containers.remove(container.getExternalId());
    }
    container.setExternalId(resource.containerId);
    container.host = resource.host;
    container.bufferServerAddress = bufferServerAddr;
    if (UserGroupInformation.isSecurityEnabled()) {
      byte[] token = AuthManager.generateToken();
      container.setBufferServerToken(token);
    }
    container.nodeHttpAddress = resource.nodeHttpAddress;
    container.setAllocatedMemoryMB(resource.memoryMB);
    container.setAllocatedVCores(resource.vCores);
    container.setStartedTime(-1);
    container.setFinishedTime(-1);
    writeJournal(container.getSetContainerState());

    StreamingContainerAgent sca = new StreamingContainerAgent(container, newStreamingContainerContext(container), this);
    containers.put(resource.containerId, sca);
    LOG.debug("Assigned container {} priority {}", resource.containerId, resource.priority);
    return sca;
  }

  private StreamingContainerContext newStreamingContainerContext(PTContainer container)
  {
    try {
      int bufferServerMemory = 0;
      Iterator<PTOperator> operatorIterator = container.getOperators().iterator();

      while (operatorIterator.hasNext()) {
        bufferServerMemory += operatorIterator.next().getBufferServerMemory();
      }
      LOG.debug("Buffer Server Memory {}", bufferServerMemory);

      // the logical plan is not to be serialized via RPC, clone attributes only
      StreamingContainerContext scc = new StreamingContainerContext(plan.getLogicalPlan().getAttributes().clone(), null);
      scc.attributes.put(ContainerContext.IDENTIFIER, container.getExternalId());
      scc.attributes.put(ContainerContext.BUFFER_SERVER_MB, bufferServerMemory);
      scc.attributes.put(ContainerContext.BUFFER_SERVER_TOKEN, container.getBufferServerToken());
      scc.startWindowMillis = this.vars.windowStartMillis;
      return scc;
    }
    catch (CloneNotSupportedException ex) {
      throw new RuntimeException("Cannot clone DAG attributes", ex);
    }
  }

  public StreamingContainerAgent getContainerAgent(String containerId)
  {
    StreamingContainerAgent cs = containers.get(containerId);
    if (cs == null) {
      LOG.warn("Trying to get unknown container {}", containerId);
    }
    return cs;
  }

  public Collection<StreamingContainerAgent> getContainerAgents()
  {
    return this.containers.values();
  }

  private void processOperatorDeployStatus(final PTOperator oper, OperatorHeartbeat ohb, StreamingContainerAgent sca)
  {
    OperatorHeartbeat.DeployState ds = null;
    if (ohb != null) {
      ds = ohb.getState();
    }

    LOG.debug("heartbeat {} {}/{} {}", oper, oper.getState(), ds, oper.getContainer().getExternalId());

    switch (oper.getState()) {
      case ACTIVE:
        // Commented out the warning below because it's expected when the operator does something
        // quickly and goes out of commission, it will report SHUTDOWN correcly whereas this code
        // is incorrectly expecting ACTIVE to be reported.
        //LOG.warn("status out of sync {} expected {} remote {}", oper, oper.getState(), ds);
        // operator expected active, check remote status
        if (ds == null) {
          sca.deployOpers.add(oper);
        }
        else {
          switch (ds) {
            case SHUTDOWN:
              // schedule operator deactivation against the windowId
              // will be processed once window is committed and all dependent operators completed processing
              long windowId = oper.stats.currentWindowId.get();
              if (ohb.windowStats != null && !ohb.windowStats.isEmpty()) {
                windowId = ohb.windowStats.get(ohb.windowStats.size()-1).windowId;
              }
              LOG.debug("Operator {} deactivated at window {}", oper, windowId);
              synchronized (this.shutdownOperators) {
                Set<PTOperator> deactivatedOpers = this.shutdownOperators.get(windowId);
                if (deactivatedOpers == null) {
                  this.shutdownOperators.put(windowId, deactivatedOpers = new HashSet<>());
                }
                deactivatedOpers.add(oper);
              }
              sca.undeployOpers.add(oper.getId());
              // record operator stop event
              recordEventAsync(new StramEvent.StopOperatorEvent(oper.getName(), oper.getId(), oper.getContainer().getExternalId()));
              break;
            case FAILED:
              processOperatorFailure(oper);
              sca.undeployOpers.add(oper.getId());
              recordEventAsync(new StramEvent.StopOperatorEvent(oper.getName(), oper.getId(), oper.getContainer().getExternalId()));
              break;
            case ACTIVE:
              break;
          }
        }
        break;
      case PENDING_UNDEPLOY:
        if (ds == null) {
          // operator no longer deployed in container
          recordEventAsync(new StramEvent.StopOperatorEvent(oper.getName(), oper.getId(), oper.getContainer().getExternalId()));
          oper.setState(State.PENDING_DEPLOY);
          sca.deployOpers.add(oper);
        }
        else {
          // operator is currently deployed, request undeploy
          sca.undeployOpers.add(oper.getId());
        }
        break;
      case PENDING_DEPLOY:
        if (ds == null) {
          // operator to be deployed
          sca.deployOpers.add(oper);
        }
        else {
          // operator was deployed in container
          PTContainer container = oper.getContainer();
          LOG.debug("{} marking deployed: {} remote status {}", container.getExternalId(), oper, ds);
          oper.setState(PTOperator.State.ACTIVE);
          oper.stats.lastHeartbeat = null; // reset on redeploy
          oper.stats.lastWindowIdChangeTms = clock.getTime();
          recordEventAsync(new StramEvent.StartOperatorEvent(oper.getName(), oper.getId(), container.getExternalId()));
        }
        break;
      default:
        //LOG.warn("Unhandled operator state {} {} remote {}", oper, oper.getState(), ds);
        if (ds != null) {
          // operator was removed and needs to be undeployed from container
          sca.undeployOpers.add(oper.getId());
          recordEventAsync(new StramEvent.StopOperatorEvent(oper.getName(), oper.getId(), oper.getContainer().getExternalId()));
        }
    }
  }

  private void processOperatorFailure(PTOperator oper)
  {
    // count failure transitions *->FAILED, applies to initialization as well as intermittent failures
    if (oper.getState() == PTOperator.State.ACTIVE) {
      oper.setState(PTOperator.State.INACTIVE);
      oper.failureCount++;
      oper.getOperatorMeta().getStatus().failureCount++;
      LOG.warn("Operator failure: {} count: {}", oper, oper.failureCount);
      Integer maxAttempts = oper.getOperatorMeta().getValue(OperatorContext.RECOVERY_ATTEMPTS);
      if (maxAttempts == null || oper.failureCount <= maxAttempts) {
        // restart entire container in attempt to recover operator
        // in the future a more sophisticated recovery strategy could
        // involve initial redeploy attempt(s) of affected operator in
        // existing container or sandbox container for just the operator
        LOG.error("Initiating container restart after operator failure {}", oper);
        containerStopRequests.put(oper.getContainer().getExternalId(), oper.getContainer().getExternalId());
      }
      else {
        String msg = String.format("Shutdown after reaching failure threshold for %s", oper);
        LOG.warn(msg);
        shutdownAllContainers(msg);
        forcedShutdown = true;
      }
    }
    else {
      // should not get here
      LOG.warn("Failed operator {} {} {} to be undeployed by container", oper, oper.getState());
    }
  }

  /**
   * process the heartbeat from each container.
   * called by the RPC thread for each container. (i.e. called by multiple threads)
   *
   * @param heartbeat
   * @return heartbeat response
   */
  @SuppressWarnings("StatementWithEmptyBody")
  public ContainerHeartbeatResponse processHeartbeat(ContainerHeartbeat heartbeat)
  {
    long currentTimeMillis = clock.getTime();

    final StreamingContainerAgent sca = this.containers.get(heartbeat.getContainerId());
    if (sca == null || sca.container.getState() == PTContainer.State.KILLED) {
      // could be orphaned container that was replaced and needs to terminate
      LOG.error("Unknown container {}", heartbeat.getContainerId());
      ContainerHeartbeatResponse response = new ContainerHeartbeatResponse();
      response.shutdown = true;
      return response;
    }

    //LOG.debug("{} {} {}", new Object[]{sca.container.containerId, sca.container.bufferServerAddress, sca.container.getState()});
    if (sca.container.getState() == PTContainer.State.ALLOCATED) {
      // capture dynamically assigned address from container
      if (sca.container.bufferServerAddress == null && heartbeat.bufferServerHost != null) {
        sca.container.bufferServerAddress = InetSocketAddress.createUnresolved(heartbeat.bufferServerHost, heartbeat.bufferServerPort);
        LOG.info("Container {} buffer server: {}", sca.container.getExternalId(), sca.container.bufferServerAddress);
      }
      final long containerStartTime = System.currentTimeMillis();
      sca.container.setState(PTContainer.State.ACTIVE);
      sca.container.setStartedTime(containerStartTime);
      sca.container.setFinishedTime(-1);
      sca.jvmName = heartbeat.jvmName;
      poolExecutor.submit(new Runnable()
      {
        @Override
        public void run()
        {
          try {
            containerFile.append(sca.getContainerInfo());
          } catch (IOException ex) {
            LOG.warn("Cannot write to container file");
          }
          for (PTOperator ptOp : sca.container.getOperators()) {
            try {
              JSONObject operatorInfo = new JSONObject();
              operatorInfo.put("name", ptOp.getName());
              operatorInfo.put("id", ptOp.getId());
              operatorInfo.put("container", sca.container.getExternalId());
              operatorInfo.put("startTime", containerStartTime);
              operatorFile.append(operatorInfo);
            } catch (IOException | JSONException ex) {
              LOG.warn("Cannot write to operator file: ", ex);
            }
          }
        }
      });
    }

    if (heartbeat.restartRequested) {
      LOG.error("Container {} restart request", sca.container.getExternalId());
      containerStopRequests.put(sca.container.getExternalId(), sca.container.getExternalId());
    }

    sca.memoryMBFree = heartbeat.memoryMBFree;
    sca.gcCollectionCount = heartbeat.gcCollectionCount;
    sca.gcCollectionTime = heartbeat.gcCollectionTime;

    sca.undeployOpers.clear();
    sca.deployOpers.clear();
    if (!this.deployChangeInProgress.get()) {
      sca.deployCnt = this.deployChangeCnt;
    }
    Set<Integer> reportedOperators = Sets.newHashSetWithExpectedSize(sca.container.getOperators().size());

    boolean containerIdle = true;

    for (OperatorHeartbeat shb : heartbeat.getContainerStats().operators) {

      long maxEndWindowTimestamp = 0;

      reportedOperators.add(shb.nodeId);
      PTOperator oper = this.plan.getAllOperators().get(shb.getNodeId());

      if (oper == null) {
        LOG.info("Heartbeat for unknown operator {} (container {})", shb.getNodeId(), heartbeat.getContainerId());
        sca.undeployOpers.add(shb.nodeId);
        continue;
      }

      if (shb.requestResponse != null) {
        for (StatsListener.OperatorResponse obj : shb.requestResponse) {
          if (obj instanceof OperatorResponse) {      // This is to identify platform requests
            commandResponse.put((Long) obj.getResponseId(), obj.getResponse());
            LOG.debug(" Got back the response {} for the request {}", obj, obj.getResponseId());
          }
          else {       // This is to identify user requests
            if (oper.stats.operatorResponses == null) {
              oper.stats.operatorResponses = new ArrayList<StatsListener.OperatorResponse>();
            }
            oper.stats.operatorResponses.add(obj);
          }
        }
      }

      //LOG.debug("heartbeat {} {}/{} {}", oper, oper.getState(), shb.getState(), oper.getContainer().getExternalId());
      if (!(oper.getState() == PTOperator.State.ACTIVE && shb.getState() == OperatorHeartbeat.DeployState.ACTIVE)) {
        // deploy state may require synchronization
        processOperatorDeployStatus(oper, shb, sca);
      }

      oper.stats.lastHeartbeat = shb;
      List<ContainerStats.OperatorStats> statsList = shb.getOperatorStatsContainer();
      if (!oper.stats.isIdle()) {
        containerIdle = false;
      }
      if (!statsList.isEmpty()) {
        long tuplesProcessed = 0;
        long tuplesEmitted = 0;
        long totalCpuTimeUsed = 0;
        int statCount = 0;
        long maxDequeueTimestamp = -1;
        oper.stats.recordingId = null;

        final OperatorStatus status = oper.stats;
        status.statsRevs.checkout();

        for (Map.Entry<String, PortStatus> entry : status.inputPortStatusList.entrySet()) {
          entry.getValue().recordingId = null;
        }
        for (Map.Entry<String, PortStatus> entry : status.outputPortStatusList.entrySet()) {
          entry.getValue().recordingId = null;
        }
        for (ContainerStats.OperatorStats stats : statsList) {

          /* report checkpoint-ed WindowId status of the operator */
          if (stats.checkpoint instanceof Checkpoint) {
            if (oper.getRecentCheckpoint() == null || oper.getRecentCheckpoint().windowId < stats.checkpoint.getWindowId()) {
              addCheckpoint(oper, (Checkpoint) stats.checkpoint);
              if (stats.checkpointStats != null) {
                status.checkpointStats = stats.checkpointStats;
                status.checkpointTimeMA.add(stats.checkpointStats.checkpointTime);
              }
              oper.failureCount = 0;
            }
          }

          oper.stats.recordingId = stats.recordingId;

          /* report all the other stuff */

          // calculate the stats related to end window
          EndWindowStats endWindowStats = new EndWindowStats(); // end window stats for a particular window id for a particular node
          Collection<ContainerStats.OperatorStats.PortStats> ports = stats.inputPorts;
          if (ports != null) {
            Set<String> currentInputPortSet = Sets.newHashSetWithExpectedSize(ports.size());
            for (ContainerStats.OperatorStats.PortStats s : ports) {
              currentInputPortSet.add(s.id);
              PortStatus ps = status.inputPortStatusList.get(s.id);
              if (ps == null) {
                ps = status.new PortStatus();
                ps.portName = s.id;
                status.inputPortStatusList.put(s.id, ps);
              }
              ps.totalTuples += s.tupleCount;
              ps.recordingId = s.recordingId;

              tuplesProcessed += s.tupleCount;
              endWindowStats.dequeueTimestamps.put(s.id, s.endWindowTimestamp);

              Pair<Integer, String> operatorPortName = new Pair<Integer, String>(oper.getId(), s.id);
              long lastEndWindowTimestamp = operatorPortLastEndWindowTimestamps.containsKey(operatorPortName) ? operatorPortLastEndWindowTimestamps.get(operatorPortName) : lastStatsTimestamp;
              long portElapsedMillis = Math.max(s.endWindowTimestamp - lastEndWindowTimestamp, 0);
              //LOG.debug("=== PROCESSED TUPLE COUNT for {}: {}, {}, {}, {}", operatorPortName, s.tupleCount, portElapsedMillis, operatorPortLastEndWindowTimestamps.get(operatorPortName), lastStatsTimestamp);
              ps.tuplesPMSMA.add(s.tupleCount, portElapsedMillis);
              ps.bufferServerBytesPMSMA.add(s.bufferServerBytes, portElapsedMillis);
              ps.queueSizeMA.add(s.queueSize);

              operatorPortLastEndWindowTimestamps.put(operatorPortName, s.endWindowTimestamp);
              if (maxEndWindowTimestamp < s.endWindowTimestamp) {
                maxEndWindowTimestamp = s.endWindowTimestamp;
              }
              if (s.endWindowTimestamp > maxDequeueTimestamp) {
                maxDequeueTimestamp = s.endWindowTimestamp;
              }
            }
            // need to remove dead ports, for unifiers
            Iterator<Map.Entry<String, PortStatus>> it = status.inputPortStatusList.entrySet().iterator();
            while (it.hasNext()) {
              Map.Entry<String, PortStatus> entry = it.next();
              if (!currentInputPortSet.contains(entry.getKey())) {
                it.remove();
              }
            }
          }

          ports = stats.outputPorts;
          if (ports != null) {
            Set<String> currentOutputPortSet = Sets.newHashSetWithExpectedSize(ports.size());
            for (ContainerStats.OperatorStats.PortStats s : ports) {
              currentOutputPortSet.add(s.id);
              PortStatus ps = status.outputPortStatusList.get(s.id);
              if (ps == null) {
                ps = status.new PortStatus();
                ps.portName = s.id;
                status.outputPortStatusList.put(s.id, ps);
              }
              ps.totalTuples += s.tupleCount;
              ps.recordingId = s.recordingId;

              tuplesEmitted += s.tupleCount;
              Pair<Integer, String> operatorPortName = new Pair<Integer, String>(oper.getId(), s.id);

              long lastEndWindowTimestamp = operatorPortLastEndWindowTimestamps.containsKey(operatorPortName) ? operatorPortLastEndWindowTimestamps.get(operatorPortName) : lastStatsTimestamp;
              long portElapsedMillis = Math.max(s.endWindowTimestamp - lastEndWindowTimestamp, 0);
              //LOG.debug("=== EMITTED TUPLE COUNT for {}: {}, {}, {}, {}", operatorPortName, s.tupleCount, portElapsedMillis, operatorPortLastEndWindowTimestamps.get(operatorPortName), lastStatsTimestamp);
              ps.tuplesPMSMA.add(s.tupleCount, portElapsedMillis);
              ps.bufferServerBytesPMSMA.add(s.bufferServerBytes, portElapsedMillis);
              operatorPortLastEndWindowTimestamps.put(operatorPortName, s.endWindowTimestamp);
              if (maxEndWindowTimestamp < s.endWindowTimestamp) {
                maxEndWindowTimestamp = s.endWindowTimestamp;
              }
            }
            if (ports.size() > 0) {
              endWindowStats.emitTimestamp = ports.iterator().next().endWindowTimestamp;
            }
            // need to remove dead ports, for unifiers
            Iterator<Map.Entry<String, PortStatus>> it = status.outputPortStatusList.entrySet().iterator();
            while (it.hasNext()) {
              Map.Entry<String, PortStatus> entry = it.next();
              if (!currentOutputPortSet.contains(entry.getKey())) {
                it.remove();
              }
            }
          }

          // for output operator, just take the maximum dequeue time for emit timestamp.
          // (we don't know the latency for output operators because they don't emit tuples)
          if (endWindowStats.emitTimestamp < 0) {
            endWindowStats.emitTimestamp = maxDequeueTimestamp;
          }

          if (status.currentWindowId.get() != stats.windowId) {
            status.lastWindowIdChangeTms = currentTimeMillis;
            status.currentWindowId.set(stats.windowId);
          }
          totalCpuTimeUsed += stats.cpuTimeUsed;
          statCount++;

          if (oper.getOperatorMeta().getValue(OperatorContext.COUNTERS_AGGREGATOR) != null) {
            endWindowStats.counters = stats.counters;
          }
          if (oper.getOperatorMeta().getMetricAggregatorMeta() != null &&
            oper.getOperatorMeta().getMetricAggregatorMeta().getAggregator() != null) {
            endWindowStats.metrics = stats.metrics;
          }

          if (stats.windowId > currentEndWindowStatsWindowId) {
            Map<Integer, EndWindowStats> endWindowStatsMap = endWindowStatsOperatorMap.get(stats.windowId);
            if (endWindowStatsMap == null) {
              endWindowStatsOperatorMap.putIfAbsent(stats.windowId, new ConcurrentSkipListMap<Integer, EndWindowStats>());
              endWindowStatsMap = endWindowStatsOperatorMap.get(stats.windowId);
            }
            endWindowStatsMap.put(shb.getNodeId(), endWindowStats);

            Set<Integer> allCurrentOperators = plan.getAllOperators().keySet();
            int numOperators = plan.getAllOperators().size();
            if (allCurrentOperators.containsAll(endWindowStatsMap.keySet()) && endWindowStatsMap.size() == numOperators) {
              completeEndWindowStatsWindowId = stats.windowId;
            }
          }
        }

        status.totalTuplesProcessed.add(tuplesProcessed);
        status.totalTuplesEmitted.add(tuplesEmitted);
        OperatorMeta logicalOperator = oper.getOperatorMeta();
        LogicalOperatorStatus logicalStatus = logicalOperator.getStatus();
        if (!oper.isUnifier()) {
          logicalStatus.totalTuplesProcessed += tuplesProcessed;
          logicalStatus.totalTuplesEmitted += tuplesEmitted;
        }
        long lastMaxEndWindowTimestamp = operatorLastEndWindowTimestamps.containsKey(oper.getId()) ? operatorLastEndWindowTimestamps.get(oper.getId()) : lastStatsTimestamp;
        if (maxEndWindowTimestamp >= lastMaxEndWindowTimestamp) {
          double tuplesProcessedPMSMA = 0.0;
          double tuplesEmittedPMSMA = 0.0;
          if (statCount != 0) {
            //LOG.debug("CPU for {}: {} / {} - {}", oper.getId(), totalCpuTimeUsed, maxEndWindowTimestamp, lastMaxEndWindowTimestamp);
            status.cpuNanosPMSMA.add(totalCpuTimeUsed, maxEndWindowTimestamp - lastMaxEndWindowTimestamp);
          }

          for (PortStatus ps : status.inputPortStatusList.values()) {
            tuplesProcessedPMSMA += ps.tuplesPMSMA.getAvg();
          }
          for (PortStatus ps : status.outputPortStatusList.values()) {
            tuplesEmittedPMSMA += ps.tuplesPMSMA.getAvg();
          }
          status.tuplesProcessedPSMA.set(Math.round(tuplesProcessedPMSMA * 1000));
          status.tuplesEmittedPSMA.set(Math.round(tuplesEmittedPMSMA * 1000));
        }
        else {
          //LOG.warn("This timestamp for {} is lower than the previous!! {} < {}", oper.getId(), maxEndWindowTimestamp, lastMaxEndWindowTimestamp);
        }
        operatorLastEndWindowTimestamps.put(oper.getId(), maxEndWindowTimestamp);
        status.listenerStats.add(statsList);
        this.reportStats.put(oper, oper);

        status.statsRevs.commit();
      }
      if (lastStatsTimestamp < maxEndWindowTimestamp) {
        lastStatsTimestamp = maxEndWindowTimestamp;
      }
    }

    sca.lastHeartbeatMillis = currentTimeMillis;

    for (PTOperator oper : sca.container.getOperators()) {
      if (!reportedOperators.contains(oper.getId())) {
        processOperatorDeployStatus(oper, null, sca);
      }
    }

    ContainerHeartbeatResponse rsp = getHeartbeatResponse(sca);

    if (containerIdle && isApplicationIdle()) {
      LOG.info("requesting idle shutdown for container {}", heartbeat.getContainerId());
      rsp.shutdown = true;
    }
    else {
      if (sca.shutdownRequested) {
        LOG.info("requesting shutdown for container {}", heartbeat.getContainerId());
        rsp.shutdown = true;
      }
    }

    List<StramToNodeRequest> requests = rsp.nodeRequests != null ? rsp.nodeRequests : new ArrayList<StramToNodeRequest>();
    ConcurrentLinkedQueue<StramToNodeRequest> operatorRequests = sca.getOperatorRequests();
    while (true) {
      StramToNodeRequest r = operatorRequests.poll();
      if (r == null) {
        break;
      }
      requests.add(r);
    }
    rsp.nodeRequests = requests;
    rsp.committedWindowId = committedWindowId;
    return rsp;
  }

  private ContainerHeartbeatResponse getHeartbeatResponse(StreamingContainerAgent sca)
  {
    ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
    if (this.deployChangeInProgress.get() || sca.deployCnt != this.deployChangeCnt) {
      LOG.debug("{} deferred requests due to concurrent plan change.", sca.container.toIdStateString());
      rsp.hasPendingRequests = true;
      return rsp;
    }

    if (!sca.undeployOpers.isEmpty()) {
      rsp.undeployRequest = Lists.newArrayList(sca.undeployOpers);
      rsp.hasPendingRequests = (!sca.deployOpers.isEmpty());
      return rsp;
    }

    Set<PTOperator> deployOperators = sca.deployOpers;
    if (!deployOperators.isEmpty()) {
      // deploy once all containers are running and no undeploy operations are pending.
      for (PTContainer c : getPhysicalPlan().getContainers()) {
        if (c.getState() != PTContainer.State.ACTIVE) {
          LOG.debug("{} waiting for container activation {}", sca.container.toIdStateString(), c.toIdStateString());
          rsp.hasPendingRequests = true;
          return rsp;
        }
        for (PTOperator oper : c.getOperators()) {
          if (oper.getState() == PTOperator.State.PENDING_UNDEPLOY) {
            LOG.debug("{} waiting for undeploy {} {}", sca.container.toIdStateString(), c.toIdStateString(), oper);
            rsp.hasPendingRequests = true;
            return rsp;
          }
        }
      }

      LOG.debug("{} deployable operators: {}", sca.container.toIdStateString(), deployOperators);
      List<OperatorDeployInfo> deployList = sca.getDeployInfoList(deployOperators);
      if (deployList != null && !deployList.isEmpty()) {
        rsp.deployRequest = deployList;
        rsp.nodeRequests = Lists.newArrayList();
        for (PTOperator o : deployOperators) {
          rsp.nodeRequests.addAll(o.deployRequests);
        }
      }
      rsp.hasPendingRequests = false;
      return rsp;
    }
    return rsp;
  }

  private boolean isApplicationIdle()
  {
    if (eventQueueProcessing.get()) {
      return false;
    }
    for (StreamingContainerAgent sca : this.containers.values()) {
      if (sca.hasPendingWork()) {
        // container may have no active operators but deploy request pending
        return false;
      }
      for (PTOperator oper : sca.container.getOperators()) {
        if (!oper.stats.isIdle()) {
          return false;
        }
      }
    }
    return true;
  }

  @SuppressWarnings("StatementWithEmptyBody")
  void addCheckpoint(PTOperator node, Checkpoint checkpoint)
  {
    synchronized (node.checkpoints) {
      if (!node.checkpoints.isEmpty()) {
        Checkpoint lastCheckpoint = node.checkpoints.getLast();
        // skip unless checkpoint moves
        if (lastCheckpoint.windowId != checkpoint.windowId) {
          if (lastCheckpoint.windowId > checkpoint.windowId) {
            // list needs to have max windowId last
            LOG.warn("Out of sequence checkpoint {} last {} (operator {})", checkpoint, lastCheckpoint, node);
            ListIterator<Checkpoint> li = node.checkpoints.listIterator();
            while (li.hasNext() && li.next().windowId < checkpoint.windowId) {
              //continue;
            }
            if (li.previous().windowId != checkpoint.windowId) {
              li.add(checkpoint);
            }
          }
          else {
            node.checkpoints.add(checkpoint);
          }
        }
      }
      else {
        node.checkpoints.add(checkpoint);
      }
    }
  }

  public static class UpdateCheckpointsContext
  {
    public final MutableLong committedWindowId = new MutableLong(Long.MAX_VALUE);
    public final Set<PTOperator> visited = new LinkedHashSet<PTOperator>();
    public final Set<PTOperator> blocked = new LinkedHashSet<PTOperator>();
    public final long currentTms;
    public final boolean recovery;

    public UpdateCheckpointsContext(Clock clock)
    {
      this.currentTms = clock.getTime();
      this.recovery = false;
    }

    public UpdateCheckpointsContext(Clock clock, boolean recovery)
    {
      this.currentTms = clock.getTime();
      this.recovery = recovery;
    }

  }

  /**
   * Compute checkpoints required for a given operator instance to be recovered.
   * This is done by looking at checkpoints available for downstream dependencies first,
   * and then selecting the most recent available checkpoint that is smaller than downstream.
   *
   * @param operator Operator instance for which to find recovery checkpoint
   * @param ctx      Context into which to collect traversal info
   */
  public void updateRecoveryCheckpoints(PTOperator operator, UpdateCheckpointsContext ctx)
  {
    if (operator.getRecoveryCheckpoint().windowId < ctx.committedWindowId.longValue()) {
      ctx.committedWindowId.setValue(operator.getRecoveryCheckpoint().windowId);
    }

    if (operator.getState() == PTOperator.State.ACTIVE && (ctx.currentTms - operator.stats.lastWindowIdChangeTms) > operator.stats.windowProcessingTimeoutMillis) {
      // if the checkpoint is ahead, then it is not blocked but waiting for activation (state-less recovery, at-most-once)
      if (ctx.committedWindowId.longValue() >= operator.getRecoveryCheckpoint().windowId) {
        ctx.blocked.add(operator);
      }
    }

    long maxCheckpoint = operator.getRecentCheckpoint().windowId;
    if (ctx.recovery && maxCheckpoint == Stateless.WINDOW_ID && operator.isOperatorStateLess()) {
      long currentWindowId = WindowGenerator.getWindowId(ctx.currentTms, this.vars.windowStartMillis, this.getLogicalPlan().getValue(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS));
      maxCheckpoint = currentWindowId;
    }

    // DFS downstream operators
    for (PTOperator.PTOutput out : operator.getOutputs()) {
      for (PTOperator.PTInput sink : out.sinks) {
        PTOperator sinkOperator = sink.target;
        if (!ctx.visited.contains(sinkOperator)) {
          // downstream traversal
          updateRecoveryCheckpoints(sinkOperator, ctx);
        }
        // recovery window id cannot move backwards
        // when dynamically adding new operators
        if (sinkOperator.getRecoveryCheckpoint().windowId >= operator.getRecoveryCheckpoint().windowId) {
          maxCheckpoint = Math.min(maxCheckpoint, sinkOperator.getRecoveryCheckpoint().windowId);
        }

        if (ctx.blocked.contains(sinkOperator)) {
          if (sinkOperator.stats.getCurrentWindowId() == operator.stats.getCurrentWindowId()) {
            // downstream operator is blocked by this operator
            ctx.blocked.remove(sinkOperator);
          }
        }
      }
    }

    // checkpoint frozen during deployment
    if (ctx.recovery || operator.getState() != PTOperator.State.PENDING_DEPLOY) {
      // remove previous checkpoints
      Checkpoint c1 = Checkpoint.INITIAL_CHECKPOINT;
      synchronized (operator.checkpoints) {
        if (!operator.checkpoints.isEmpty() && (operator.checkpoints.getFirst()).windowId <= maxCheckpoint) {
          c1 = operator.checkpoints.getFirst();
          Checkpoint c2;
          while (operator.checkpoints.size() > 1 && ((c2 = operator.checkpoints.get(1)).windowId) <= maxCheckpoint) {
            operator.checkpoints.removeFirst();
            //LOG.debug("Checkpoint to delete: operator={} windowId={}", operator.getName(), c1);
            this.purgeCheckpoints.add(new Pair<PTOperator, Long>(operator, c1.windowId));
            c1 = c2;
          }
        }
        else {
          if (ctx.recovery && operator.checkpoints.isEmpty() && operator.isOperatorStateLess()) {
            LOG.debug("Adding checkpoint for stateless operator {} {}", operator, Codec.getStringWindowId(maxCheckpoint));
            c1 = operator.addCheckpoint(maxCheckpoint, this.vars.windowStartMillis);
          }
        }
      }
      //LOG.debug("Operator {} checkpoints: commit {} recent {}", new Object[] {operator.getName(), c1, operator.checkpoints});
      operator.setRecoveryCheckpoint(c1);
    }
    else {
      LOG.debug("Skipping checkpoint update {} during {}", operator, operator.getState());
    }

    ctx.visited.add(operator);
  }

  public long windowIdToMillis(long windowId)
  {
    int widthMillis = plan.getLogicalPlan().getValue(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS);
    return WindowGenerator.getWindowMillis(windowId, this.vars.windowStartMillis, widthMillis);
  }

  public long getWindowStartMillis()
  {
    return this.vars.windowStartMillis;
  }

  /**
   * Visit all operators to update current checkpoint based on updated downstream state.
   * Purge older checkpoints that are no longer needed.
   */
  private long updateCheckpoints(boolean recovery)
  {
    UpdateCheckpointsContext ctx = new UpdateCheckpointsContext(clock, recovery);
    for (OperatorMeta logicalOperator : plan.getLogicalPlan().getRootOperators()) {
      //LOG.debug("Updating checkpoints for operator {}", logicalOperator.getName());
      List<PTOperator> operators = plan.getOperators(logicalOperator);
      if (operators != null) {
        for (PTOperator operator : operators) {
          updateRecoveryCheckpoints(operator, ctx);
        }
      }
    }
    purgeCheckpoints();

    for (PTOperator oper : ctx.blocked) {
      String containerId = oper.getContainer().getExternalId();
      if (containerId != null) {
        LOG.info("Blocked operator {} container {} time {}ms", oper, oper.getContainer().toIdStateString(), ctx.currentTms - oper.stats.lastWindowIdChangeTms);
        this.containerStopRequests.put(containerId, containerId);
      }
    }
    return ctx.committedWindowId.longValue();
  }

  private BufferServerController getBufferServerClient(PTOperator operator)
  {
    BufferServerController bsc = new BufferServerController(operator.getLogicalId());
    bsc.setToken(operator.getContainer().getBufferServerToken());
    InetSocketAddress address = operator.getContainer().bufferServerAddress;
    StreamingContainer.eventloop.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bsc);
    return bsc;
  }

  private void purgeCheckpoints()
  {
    for (Pair<PTOperator, Long> p : purgeCheckpoints) {
      final PTOperator operator = p.getFirst();
      if (!operator.isOperatorStateLess()) {
        final long windowId = p.getSecond();
        Runnable r = new Runnable()
        {
          @Override
          public void run()
          {
            try {
              operator.getOperatorMeta().getValue(OperatorContext.STORAGE_AGENT).delete(operator.getId(), windowId);
            }
            catch (IOException ex) {
              LOG.error("Failed to purge checkpoint for operator {} for windowId {}", operator, windowId, ex);
            }
          }
        };
        poolExecutor.submit(r);
      }
      // delete stream state when using buffer server
      for (PTOperator.PTOutput out : operator.getOutputs()) {
        if (!out.isDownStreamInline()) {
          if (operator.getContainer().bufferServerAddress == null) {
            // address should be null only for a new container, in which case there should not be a purge request
            // TODO: logging added to find out how we got here
            LOG.warn("purge request w/o buffer server address source {} container {} checkpoints {}",
              out, operator.getContainer(), operator.checkpoints);
            continue;
          }

          for (InputPortMeta ipm : out.logicalStream.getSinks()) {
            StreamCodec<?> streamCodecInfo = StreamingContainerAgent.getStreamCodec(ipm);
            Integer codecId = plan.getStreamCodecIdentifier(streamCodecInfo);
            // following needs to match the concat logic in StreamingContainer
            String sourceIdentifier = Integer.toString(operator.getId()).concat(Component.CONCAT_SEPARATOR).concat(out.portName).concat(Component.CONCAT_SEPARATOR).concat(codecId.toString());
            // delete everything from buffer server prior to new checkpoint
            BufferServerController bsc = getBufferServerClient(operator);
            try {
              bsc.purge(null, sourceIdentifier, operator.checkpoints.getFirst().windowId - 1);
            }
            catch (RuntimeException re) {
              LOG.warn("Failed to purge {} {}", bsc.addr, sourceIdentifier, re);
            }
          }
        }
      }
    }
    purgeCheckpoints.clear();
  }

  /**
   * Mark all containers for shutdown, next container heartbeat response
   * will propagate the shutdown request. This is controlled soft shutdown.
   * If containers don't respond, the application can be forcefully terminated
   * via yarn using forceKillApplication.
   *
   * @param message
   */
  public void shutdownAllContainers(String message)
  {
    this.shutdownDiagnosticsMessage = message;
    LOG.info("Initiating application shutdown: {}", message);
    for (StreamingContainerAgent cs : this.containers.values()) {
      cs.shutdownRequested = true;
    }
  }

  private Map<PTContainer, List<PTOperator>> groupByContainer(Collection<PTOperator> operators)
  {
    Map<PTContainer, List<PTOperator>> m = new HashMap<PTContainer, List<PTOperator>>();
    for (PTOperator node : operators) {
      List<PTOperator> nodes = m.get(node.getContainer());
      if (nodes == null) {
        nodes = new ArrayList<PTOperator>();
        m.put(node.getContainer(), nodes);
      }
      nodes.add(node);
    }
    return m;
  }

  private void requestContainer(PTContainer c)
  {
    ContainerStartRequest dr = new ContainerStartRequest(c);
    containerStartRequests.add(dr);
    pendingAllocation.add(dr.container);
    lastResourceRequest = System.currentTimeMillis();
    for (PTOperator operator : c.getOperators()) {
      operator.setState(PTOperator.State.INACTIVE);
    }
  }

  @Override
  public void deploy(Set<PTContainer> releaseContainers, Collection<PTOperator> undeploy, Set<PTContainer> startContainers, Collection<PTOperator> deploy)
  {
    try {
      this.deployChangeInProgress.set(true);

      Map<PTContainer, List<PTOperator>> undeployGroups = groupByContainer(undeploy);

      // stop affected operators (exclude new/failed containers)
      // order does not matter, remove all operators in each container in one sweep
      for (Map.Entry<PTContainer, List<PTOperator>> e : undeployGroups.entrySet()) {
        // container may already be in failed or pending deploy state, notified by RM or timed out
        PTContainer c = e.getKey();
        if (!startContainers.contains(c) && !releaseContainers.contains(c) && c.getState() != PTContainer.State.KILLED) {
          LOG.debug("scheduling undeploy {} {}", e.getKey().getExternalId(), e.getValue());
          for (PTOperator oper : e.getValue()) {
            oper.setState(PTOperator.State.PENDING_UNDEPLOY);
          }
        }
      }

      // start new containers
      for (PTContainer c : startContainers) {
        requestContainer(c);
      }

      // (re)deploy affected operators
      // can happen in parallel after buffer server for recovered publishers is reset
      Map<PTContainer, List<PTOperator>> deployGroups = groupByContainer(deploy);
      for (Map.Entry<PTContainer, List<PTOperator>> e : deployGroups.entrySet()) {
        if (!startContainers.contains(e.getKey())) {
          // to reset publishers, clean buffer server past checkpoint so subscribers don't read stale data (including end of stream)
          for (PTOperator operator : e.getValue()) {
            for (PTOperator.PTOutput out : operator.getOutputs()) {
              if (!out.isDownStreamInline()) {
                for (InputPortMeta ipm : out.logicalStream.getSinks()) {
                  StreamCodec<?> streamCodecInfo = StreamingContainerAgent.getStreamCodec(ipm);
                  Integer codecId = plan.getStreamCodecIdentifier(streamCodecInfo);
                  // following needs to match the concat logic in StreamingContainer
                  String sourceIdentifier = Integer.toString(operator.getId()).concat(Component.CONCAT_SEPARATOR).concat(out.portName).concat(Component.CONCAT_SEPARATOR).concat(codecId.toString());
                  if (operator.getContainer().getState() == PTContainer.State.ACTIVE) {
                    // TODO: unit test - find way to mock this when testing rest of logic
                    if (operator.getContainer().bufferServerAddress.getPort() != 0) {
                      BufferServerController bsc = getBufferServerClient(operator);
                      // reset publisher (stale operator may still write data until disconnected)
                      // ensures new subscriber starting to read from checkpoint will wait until publisher redeploy cycle is complete
                      try {
                        bsc.reset(null, sourceIdentifier, 0);
                      }
                      catch (Exception ex) {
                        LOG.error("Failed to reset buffer server {} {}", sourceIdentifier, ex);
                      }
                    }
                  }
                }
              }
            }
          }
        }

        // add to operators that we expect to deploy
        LOG.debug("scheduling deploy {} {}", e.getKey().getExternalId(), e.getValue());
        for (PTOperator oper : e.getValue()) {
          // operator will be deployed after it has been undeployed, if still referenced by the container
          if (oper.getState() != PTOperator.State.PENDING_UNDEPLOY) {
            oper.setState(PTOperator.State.PENDING_DEPLOY);
          }
        }
      }

      // stop containers that are no longer used
      for (PTContainer c : releaseContainers) {
        if (c.getExternalId() == null) {
          continue;
        }
        StreamingContainerAgent sca = containers.get(c.getExternalId());
        if (sca != null) {
          LOG.debug("Container marked for shutdown: {}", c);
          // container already removed from plan
          // TODO: monitor soft shutdown
          sca.shutdownRequested = true;
        }
      }

    }
    finally {
      this.deployChangeCnt++;
      this.deployChangeInProgress.set(false);
    }
  }

  @Override
  public void recordEventAsync(StramEvent ev)
  {
    if (eventBus != null) {
      eventBus.publishAsync(ev);
    }
  }

  @Override
  public void dispatch(Runnable r)
  {
    this.eventQueue.add(r);
  }

  public OperatorInfo getOperatorInfo(int operatorId)
  {
    PTOperator o = this.plan.getAllOperators().get(operatorId);
    return o == null ? null : fillPhysicalOperatorInfo(o);
  }

  public List<OperatorInfo> getOperatorInfoList()
  {
    List<OperatorInfo> infoList = new ArrayList<OperatorInfo>();

    for (PTContainer container : this.plan.getContainers()) {
      for (PTOperator operator : container.getOperators()) {
        infoList.add(fillPhysicalOperatorInfo(operator));
      }
    }
    return infoList;
  }

  public LogicalOperatorInfo getLogicalOperatorInfo(String operatorName)
  {
    OperatorMeta operatorMeta = getLogicalPlan().getOperatorMeta(operatorName);
    if (operatorMeta == null) {
      return null;
    }
    return fillLogicalOperatorInfo(operatorMeta);
  }

  public List<LogicalOperatorInfo> getLogicalOperatorInfoList()
  {
    List<LogicalOperatorInfo> infoList = new ArrayList<LogicalOperatorInfo>();
    Collection<OperatorMeta> allOperators = getLogicalPlan().getAllOperators();
    for (OperatorMeta operatorMeta : allOperators) {
      infoList.add(fillLogicalOperatorInfo(operatorMeta));
    }
    return infoList;
  }

  public OperatorAggregationInfo getOperatorAggregationInfo(String operatorName)
  {
    OperatorMeta operatorMeta = getLogicalPlan().getOperatorMeta(operatorName);
    if (operatorMeta == null) {
      return null;
    }
    return fillOperatorAggregationInfo(operatorMeta);
  }

  public static long toWsWindowId(long windowId)
  {
    // until console handles -1
    return windowId < 0 ? 0 : windowId;
  }

  private OperatorInfo fillPhysicalOperatorInfo(PTOperator operator)
  {
    OperatorInfo oi = new OperatorInfo();
    oi.container = operator.getContainer().getExternalId();
    oi.host = operator.getContainer().host;
    oi.id = Integer.toString(operator.getId());
    oi.name = operator.getName();
    oi.className = operator.getOperatorMeta().getOperator().getClass().getName();
    oi.status = operator.getState().toString();
    if (operator.isUnifier()) {
      oi.unifierClass = operator.getUnifierClass().getName();
    }
    oi.logicalName = operator.getOperatorMeta().getName();

    OperatorStatus os = operator.stats;
    oi.recordingId = os.recordingId;
    oi.totalTuplesProcessed = os.totalTuplesProcessed.get();
    oi.totalTuplesEmitted = os.totalTuplesEmitted.get();
    oi.tuplesProcessedPSMA = os.tuplesProcessedPSMA.get();
    oi.tuplesEmittedPSMA = os.tuplesEmittedPSMA.get();
    oi.cpuPercentageMA = os.cpuNanosPMSMA.getAvg() / 10000;
    oi.latencyMA = os.latencyMA.getAvg();
    oi.failureCount = operator.failureCount;
    oi.recoveryWindowId = toWsWindowId(operator.getRecoveryCheckpoint().windowId);
    oi.currentWindowId = toWsWindowId(os.currentWindowId.get());
    if (os.lastHeartbeat != null) {
      oi.lastHeartbeat = os.lastHeartbeat.getGeneratedTms();
    }
    if (os.checkpointStats != null) {
      oi.checkpointTime = os.checkpointStats.checkpointTime;
      oi.checkpointStartTime = os.checkpointStats.checkpointStartTime;
    }
    oi.checkpointTimeMA = os.checkpointTimeMA.getAvg();
    for (PortStatus ps : os.inputPortStatusList.values()) {
      PortInfo pinfo = new PortInfo();
      pinfo.name = ps.portName;
      pinfo.type = "input";
      pinfo.totalTuples = ps.totalTuples;
      pinfo.tuplesPSMA = Math.round(ps.tuplesPMSMA.getAvg() * 1000);
      pinfo.bufferServerBytesPSMA = Math.round(ps.bufferServerBytesPMSMA.getAvg() * 1000);
      pinfo.queueSizeMA = ps.queueSizeMA.getAvg();
      pinfo.recordingId = ps.recordingId;
      oi.addPort(pinfo);
    }
    for (PortStatus ps : os.outputPortStatusList.values()) {
      PortInfo pinfo = new PortInfo();
      pinfo.name = ps.portName;
      pinfo.type = "output";
      pinfo.totalTuples = ps.totalTuples;
      pinfo.tuplesPSMA = Math.round(ps.tuplesPMSMA.getAvg() * 1000);
      pinfo.bufferServerBytesPSMA = Math.round(ps.bufferServerBytesPMSMA.getAvg() * 1000);
      pinfo.recordingId = ps.recordingId;
      oi.addPort(pinfo);
    }
    oi.counters = os.getLastWindowedStats().size() > 0 ?
      os.getLastWindowedStats().get(os.getLastWindowedStats().size() - 1).counters : null;

    oi.metrics = os.getLastWindowedStats().size() > 0 ?
      os.getLastWindowedStats().get(os.getLastWindowedStats().size() - 1).metrics : null;
    return oi;
  }

  private LogicalOperatorInfo fillLogicalOperatorInfo(OperatorMeta operator)
  {
    LogicalOperatorInfo loi = new LogicalOperatorInfo();
    loi.name = operator.getName();
    loi.className = operator.getOperator().getClass().getName();
    loi.totalTuplesEmitted = operator.getStatus().totalTuplesEmitted;
    loi.totalTuplesProcessed = operator.getStatus().totalTuplesProcessed;
    loi.failureCount = operator.getStatus().failureCount;
    loi.status = new HashMap<String, MutableInt>();
    loi.partitions = new TreeSet<Integer>();
    loi.unifiers = new TreeSet<Integer>();
    loi.containerIds = new TreeSet<String>();
    loi.hosts = new TreeSet<String>();
    Collection<PTOperator> physicalOperators = getPhysicalPlan().getAllOperators(operator);
    NumberAggregate.LongAggregate checkpointTimeAggregate = new NumberAggregate.LongAggregate();
    for (PTOperator physicalOperator : physicalOperators) {
      OperatorStatus os = physicalOperator.stats;
      if (physicalOperator.isUnifier()) {
        loi.unifiers.add(physicalOperator.getId());
      }
      else {
        loi.partitions.add(physicalOperator.getId());

        // exclude unifier, not sure if we should include it in the future
        loi.tuplesEmittedPSMA += os.tuplesEmittedPSMA.get();
        loi.tuplesProcessedPSMA += os.tuplesProcessedPSMA.get();

        // calculate maximum latency for all partitions
        long latency = calculateLatency(physicalOperator);
        if (latency > loi.latencyMA) {
          loi.latencyMA = latency;
        }
        checkpointTimeAggregate.addNumber(os.checkpointTimeMA.getAvg());
      }
      loi.cpuPercentageMA += os.cpuNanosPMSMA.getAvg() / 10000;
      if (os.lastHeartbeat != null && (loi.lastHeartbeat == 0 || loi.lastHeartbeat > os.lastHeartbeat.getGeneratedTms())) {
        loi.lastHeartbeat = os.lastHeartbeat.getGeneratedTms();
      }
      long currentWindowId = toWsWindowId(os.currentWindowId.get());
      if (loi.currentWindowId == 0 || loi.currentWindowId > currentWindowId) {
        loi.currentWindowId = currentWindowId;
      }
      MutableInt count = loi.status.get(physicalOperator.getState().toString());
      if (count == null) {
        count = new MutableInt();
        loi.status.put(physicalOperator.getState().toString(), count);
      }
      count.increment();
      if (physicalOperator.getRecoveryCheckpoint() != null) {
        long recoveryWindowId = toWsWindowId(physicalOperator.getRecoveryCheckpoint().windowId);
        if (loi.recoveryWindowId == 0 || loi.recoveryWindowId > recoveryWindowId) {
          loi.recoveryWindowId = recoveryWindowId;
        }
      }
      PTContainer container = physicalOperator.getContainer();
      if (container != null) {
        String externalId = container.getExternalId();
        if (externalId != null) {
          loi.containerIds.add(externalId);
          loi.hosts.add(container.host);
        }
      }
    }
    loi.checkpointTimeMA = checkpointTimeAggregate.getAvg().longValue();
    loi.counters = latestLogicalCounters.get(operator.getName());
    loi.autoMetrics = latestLogicalMetrics.get(operator.getName());
    return loi;
  }

  private OperatorAggregationInfo fillOperatorAggregationInfo(OperatorMeta operator)
  {
    OperatorAggregationInfo oai = new OperatorAggregationInfo();

    Collection<PTOperator> physicalOperators = getPhysicalPlan().getAllOperators(operator);
    if (physicalOperators.isEmpty()) {
      return null;
    }
    oai.name = operator.getName();

    for (PTOperator physicalOperator : physicalOperators) {
      if (!physicalOperator.isUnifier()) {
        OperatorStatus os = physicalOperator.stats;
        oai.latencyMA.addNumber(os.latencyMA.getAvg());
        oai.cpuPercentageMA.addNumber(os.cpuNanosPMSMA.getAvg() / 10000);
        oai.tuplesEmittedPSMA.addNumber(os.tuplesEmittedPSMA.get());
        oai.tuplesProcessedPSMA.addNumber(os.tuplesProcessedPSMA.get());
        oai.currentWindowId.addNumber(os.currentWindowId.get());
        oai.recoveryWindowId.addNumber(toWsWindowId(physicalOperator.getRecoveryCheckpoint().windowId));
        if (os.lastHeartbeat != null) {
          oai.lastHeartbeat.addNumber(os.lastHeartbeat.getGeneratedTms());
        }
        oai.checkpointTime.addNumber(os.checkpointTimeMA.getAvg());
      }
    }
    return oai;
  }

  private long calculateLatency(PTOperator operator)
  {
    long latency = operator.stats.latencyMA.getAvg();
    long maxUnifierLatency = 0;
    for (PTOutput output : operator.getOutputs()) {
      for (PTInput input : output.sinks) {
        if (input.target.isUnifier()) {
          long thisUnifierLatency = calculateLatency(input.target);
          if (maxUnifierLatency < thisUnifierLatency) {
            maxUnifierLatency = thisUnifierLatency;
          }
        }
      }
    }
    return latency + maxUnifierLatency;
  }

  public List<StreamInfo> getStreamInfoList()
  {
    List<StreamInfo> infoList = new ArrayList<StreamInfo>();

    for (PTContainer container : this.plan.getContainers()) {
      for (PTOperator operator : container.getOperators()) {
        List<PTOutput> outputs = operator.getOutputs();
        for (PTOutput output : outputs) {
          StreamInfo si = new StreamInfo();
          si.logicalName = output.logicalStream.getName();
          si.source.operatorId = String.valueOf(operator.getId());
          si.source.portName = output.portName;
          si.locality = output.logicalStream.getLocality();
          for (PTInput input : output.sinks) {
            StreamInfo.Port p = new StreamInfo.Port();
            p.operatorId = String.valueOf(input.target.getId());
            if (input.target.isUnifier()) {
              p.portName = StreamingContainer.getUnifierInputPortName(input.portName, operator.getId(), output.portName);
            }
            else {
              p.portName = input.portName;
            }
            si.sinks.add(p);
          }
          infoList.add(si);
        }
      }
    }
    return infoList;
  }

  private static class RecordingRequestFilter implements Predicate<StramToNodeRequest>
  {
    final static Set<StramToNodeRequest.RequestType> MATCH_TYPES = Sets.newHashSet(StramToNodeRequest.RequestType.START_RECORDING, StramToNodeRequest.RequestType.STOP_RECORDING, StramToNodeRequest.RequestType.SYNC_RECORDING);

    @Override
    public boolean apply(@Nullable StramToNodeRequest input)
    {
      return input != null && MATCH_TYPES.contains(input.getRequestType());
    }

  }

  private class SetOperatorPropertyRequestFilter implements Predicate<StramToNodeRequest>
  {
    final String propertyKey;

    SetOperatorPropertyRequestFilter(String key)
    {
      this.propertyKey = key;
    }

    @Override
    public boolean apply(@Nullable StramToNodeRequest input)
    {
      if (input == null) {
        return false;
      }
      if (input instanceof StramToNodeSetPropertyRequest) {
        return ((StramToNodeSetPropertyRequest)input).getPropertyKey().equals(propertyKey);
      }
      return false;
    }

  }

  private void updateOnDeployRequests(PTOperator p, Predicate<StramToNodeRequest> superseded, StramToNodeRequest newRequest)
  {
    // filter existing requests
    List<StramToNodeRequest> cloneRequests = new ArrayList<StramToNodeRequest>(p.deployRequests.size());
    for (StramToNodeRequest existingRequest : p.deployRequests) {
      if (!superseded.apply(existingRequest)) {
        cloneRequests.add(existingRequest);
      }
    }
    // add new request, if any
    if (newRequest != null) {
      cloneRequests.add(newRequest);
    }
    p.deployRequests = Collections.unmodifiableList(cloneRequests);
  }

  private StreamingContainerAgent getContainerAgentFromOperatorId(int operatorId)
  {
    PTOperator oper = plan.getAllOperators().get(operatorId);
    if (oper != null) {
      StreamingContainerAgent sca = containers.get(oper.getContainer().getExternalId());
      if (sca != null) {
        return sca;
      }
    }
    // throw exception that propagates to web client
    throw new NotFoundException("Operator ID " + operatorId + " not found");
  }

  public void startRecording(String id, int operId, String portName, long numWindows)
  {
    StreamingContainerAgent sca = getContainerAgentFromOperatorId(operId);
    StramToNodeStartRecordingRequest request = new StramToNodeStartRecordingRequest();
    request.setOperatorId(operId);
    if (!StringUtils.isBlank(portName)) {
      request.setPortName(portName);
    }
    request.setNumWindows(numWindows);
    request.setId(id);
    sca.addOperatorRequest(request);
    PTOperator operator = plan.getAllOperators().get(operId);
    if (operator != null) {
      // restart on deploy
      updateOnDeployRequests(operator, new RecordingRequestFilter(), request);
    }
  }

  public void stopRecording(int operId, String portName)
  {
    StreamingContainerAgent sca = getContainerAgentFromOperatorId(operId);
    StramToNodeRequest request = new StramToNodeRequest();
    request.setOperatorId(operId);
    if (!StringUtils.isBlank(portName)) {
      request.setPortName(portName);
    }
    request.setRequestType(StramToNodeRequest.RequestType.STOP_RECORDING);
    sca.addOperatorRequest(request);
    PTOperator operator = plan.getAllOperators().get(operId);
    if (operator != null) {
      // no stop on deploy, but remove existing start
      updateOnDeployRequests(operator, new RecordingRequestFilter(), null);
    }
  }

  public void syncStats()
  {
    statsRecorder.requestSync();
  }

  public void syncEvents()
  {
    eventRecorder.requestSync();
  }

  public void stopContainer(String containerId)
  {
    this.containerStopRequests.put(containerId, containerId);
  }

  public Recoverable getSetOperatorProperty(String operatorName, String propertyName, String propertyValue) {
    return new SetOperatorProperty(operatorName, propertyName, propertyValue);
  }

  public Recoverable getSetPhysicalOperatorProperty(int operatorId, String propertyName, String propertyValue) {
    return new SetPhysicalOperatorProperty(operatorId, propertyName, propertyValue);
  }

  public void setOperatorProperty(String operatorName, String propertyName, String propertyValue)
  {
    OperatorMeta logicalOperator = plan.getLogicalPlan().getOperatorMeta(operatorName);
    if (logicalOperator == null) {
      throw new IllegalArgumentException("Unknown operator " + operatorName);
    }

    writeJournal(new SetOperatorProperty(operatorName, propertyName, propertyValue));

    setOperatorProperty(logicalOperator, propertyName, propertyValue);
  }

  private void setOperatorProperty(OperatorMeta logicalOperator, String propertyName, String propertyValue)
  {
    Map<String, String> properties = Collections.singletonMap(propertyName, propertyValue);
    LogicalPlanConfiguration.setOperatorProperties(logicalOperator.getOperator(), properties);

    List<PTOperator> operators = plan.getOperators(logicalOperator);
    for (PTOperator o : operators) {
      StramToNodeSetPropertyRequest request = new StramToNodeSetPropertyRequest();
      request.setOperatorId(o.getId());
      request.setPropertyKey(propertyName);
      request.setPropertyValue(propertyValue);
      addOperatorRequest(o, request);
      // re-apply to checkpointed state on deploy
      updateOnDeployRequests(o, new SetOperatorPropertyRequestFilter(propertyName), request);
    }
    // should probably not record it here because it's better to get confirmation from the operators first.
    // but right now, the operators do not give confirmation for the requests.  so record it here for now.
    recordEventAsync(new StramEvent.SetOperatorPropertyEvent(logicalOperator.getName(), propertyName, propertyValue));
  }

  /**
   * Set property on a physical operator. The property change is applied asynchronously on the deployed operator.
   *
   * @param operatorId
   * @param propertyName
   * @param propertyValue
   */
  public void setPhysicalOperatorProperty(int operatorId, String propertyName, String propertyValue)
  {
    PTOperator o = this.plan.getAllOperators().get(operatorId);
    if (o == null) {
      return;
    }
    writeJournal(new SetPhysicalOperatorProperty(operatorId, propertyName, propertyValue));
    setPhysicalOperatorProperty(o, propertyName, propertyValue);
  }

  private void setPhysicalOperatorProperty(PTOperator o, String propertyName, String propertyValue)
  {
    String operatorName = o.getName();
    StramToNodeSetPropertyRequest request = new StramToNodeSetPropertyRequest();
    request.setOperatorId(o.getId());
    request.setPropertyKey(propertyName);
    request.setPropertyValue(propertyValue);
    addOperatorRequest(o, request);
    updateOnDeployRequests(o, new SetOperatorPropertyRequestFilter(propertyName), request);

    // should probably not record it here because it's better to get confirmation from the operators first.
    // but right now, the operators do not give confirmation for the requests. so record it here for now.
    recordEventAsync(new StramEvent.SetPhysicalOperatorPropertyEvent(operatorName, o.getId(), propertyName, propertyValue));
  }

  @Override
  public void addOperatorRequest(PTOperator oper, StramToNodeRequest request)
  {
    StreamingContainerAgent sca = getContainerAgent(oper.getContainer().getExternalId());
    // yarn may not assigned resource to the container yet
    if (sca != null) {
      sca.addOperatorRequest(request);
    }
  }

  /**
   * Send requests to change logger levels to all containers
   *
   * @param changedLoggers loggers that were changed.
   */
  public void setLoggersLevel(Map<String, String> changedLoggers)
  {
    LOG.debug("change logger request");
    StramToNodeChangeLoggersRequest request = new StramToNodeChangeLoggersRequest();
    request.setTargetChanges(changedLoggers);
    for (StreamingContainerAgent stramChildAgent : containers.values()) {
      stramChildAgent.addOperatorRequest(request);
    }
  }

  public FutureTask<Object> getPhysicalOperatorProperty(int operatorId, String propertyName, long waitTime)
  {
    PTOperator o = this.plan.getAllOperators().get(operatorId);
    StramToNodeGetPropertyRequest request = new StramToNodeGetPropertyRequest();
    request.setOperatorId(operatorId);
    request.setPropertyName(propertyName);
    addOperatorRequest(o, request);
    RequestHandler task = new RequestHandler();
    task.requestId = nodeToStramRequestIds.incrementAndGet();
    task.waitTime = waitTime;
    request.requestId = task.requestId;
    FutureTask<Object> future = new FutureTask<Object>(task);
    dispatch(future);
    return future;
  }

  public Attribute.AttributeMap getApplicationAttributes()
  {
    LogicalPlan lp = getLogicalPlan();
    try {
      return lp.getAttributes().clone();
    }
    catch (CloneNotSupportedException ex) {
      throw new RuntimeException("Cannot clone DAG attributes", ex);
    }
  }

  public Attribute.AttributeMap getOperatorAttributes(String operatorId)
  {
    OperatorMeta logicalOperator = plan.getLogicalPlan().getOperatorMeta(operatorId);
    if (logicalOperator == null) {
      throw new IllegalArgumentException("Invalid operatorId " + operatorId);
    }
    try {
      return logicalOperator.getAttributes().clone();
    }
    catch (CloneNotSupportedException ex) {
      throw new RuntimeException("Cannot clone operator attributes", ex);
    }
  }

  public Map<String, Object> getPortAttributes(String operatorId, String portName)
  {
    OperatorMeta logicalOperator = plan.getLogicalPlan().getOperatorMeta(operatorId);
    if (logicalOperator == null) {
      throw new IllegalArgumentException("Invalid operatorId " + operatorId);
    }

    Operators.PortMappingDescriptor portMap = new Operators.PortMappingDescriptor();
    Operators.describe(logicalOperator.getOperator(), portMap);
    PortContextPair<InputPort<?>> inputPort = portMap.inputPorts.get(portName);
    if (inputPort != null) {
      HashMap<String, Object> portAttributeMap = new HashMap<String, Object>();
      InputPortMeta portMeta = logicalOperator.getMeta(inputPort.component);
      Map<Attribute<Object>, Object> rawAttributes = Attribute.AttributeMap.AttributeInitializer.getAllAttributes(portMeta, Context.PortContext.class);
      for (Map.Entry<Attribute<Object>, Object> attEntry : rawAttributes.entrySet()) {
        portAttributeMap.put(attEntry.getKey().getSimpleName(), attEntry.getValue());
      }
      return portAttributeMap;
    }
    else {
      PortContextPair<OutputPort<?>> outputPort = portMap.outputPorts.get(portName);
      if (outputPort != null) {
        HashMap<String, Object> portAttributeMap = new HashMap<String, Object>();
        OutputPortMeta portMeta = logicalOperator.getMeta(outputPort.component);
        Map<Attribute<Object>, Object> rawAttributes = Attribute.AttributeMap.AttributeInitializer.getAllAttributes(portMeta, Context.PortContext.class);
        for (Map.Entry<Attribute<Object>, Object> attEntry : rawAttributes.entrySet()) {
          portAttributeMap.put(attEntry.getKey().getSimpleName(), attEntry.getValue());
        }
        return portAttributeMap;
      }
      throw new IllegalArgumentException("Invalid port name " + portName);
    }
  }

  public LogicalPlan getLogicalPlan()
  {
    return plan.getLogicalPlan();
  }

  /**
   * Asynchronously process the logical, physical plan and execution layer changes.
   * Caller can use the returned future to block until processing is complete.
   *
   * @param requests
   * @return future
   * @throws Exception
   */
  public FutureTask<Object> logicalPlanModification(List<LogicalPlanRequest> requests) throws Exception
  {
    // delegate processing to dispatch thread
    FutureTask<Object> future = new FutureTask<Object>(new LogicalPlanChangeRunnable(requests));
    dispatch(future);
    //LOG.info("Scheduled plan changes: {}", requests);
    return future;
  }

  private class LogicalPlanChangeRunnable implements java.util.concurrent.Callable<Object>
  {
    final List<LogicalPlanRequest> requests;

    private LogicalPlanChangeRunnable(List<LogicalPlanRequest> requests)
    {
      this.requests = requests;
    }

    @Override
    public Object call() throws Exception
    {
      // clone logical plan, for dry run and validation
      LOG.info("Begin plan changes: {}", requests);
      LogicalPlan lp = plan.getLogicalPlan();
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      LogicalPlan.write(lp, bos);
      bos.flush();
      ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
      lp = LogicalPlan.read(bis);

      PlanModifier pm = new PlanModifier(lp);
      for (LogicalPlanRequest request : requests) {
        LOG.debug("Dry run plan change: {}", request);
        request.execute(pm);
      }

      lp.validate();

      // perform changes on live plan
      pm = new PlanModifier(plan);
      for (LogicalPlanRequest request : requests) {
        request.execute(pm);

        // record an event for the request.  however, we should probably record these when we get a confirmation.
        recordEventAsync(new StramEvent.ChangeLogicalPlanEvent(request));
      }
      pm.applyChanges(StreamingContainerManager.this);
      LOG.info("Plan changes applied: {}", requests);
      return null;
    }

  }

  public CriticalPathInfo getCriticalPathInfo()
  {
    return criticalPathInfo;
  }

  private void checkpoint() throws IOException
  {
    if (recoveryHandler != null) {
      LOG.debug("Checkpointing state");
      DataOutputStream out = recoveryHandler.rotateLog();
      journal.setOutputStream(out);
      // checkpoint the state
      CheckpointState cs = new CheckpointState();
      cs.finals = this.vars;
      cs.physicalPlan = this.plan;
      recoveryHandler.save(cs);
    }
  }

  @Override
  public void writeJournal(Recoverable operation)
  {
    try {
      if (journal != null) {
        journal.write(operation);
      }
    }
    catch (Exception e) {
      throw new IllegalStateException("Failed to write to journal " + operation, e);
    }
  }

  /**
   * Get the instance for the given application. If the application directory contains a checkpoint, the state will be restored.
   *
   * @param rh
   * @param dag
   * @param enableEventRecording
   * @return instance of {@link StreamingContainerManager}
   * @throws IOException
   */
  public static StreamingContainerManager getInstance(RecoveryHandler rh, LogicalPlan dag, boolean enableEventRecording) throws IOException
  {
    try {
      CheckpointState checkpointedState = (CheckpointState) rh.restore();
      StreamingContainerManager scm;
      if (checkpointedState == null) {
        scm = new StreamingContainerManager(dag, enableEventRecording, new SystemClock());
      }
      else {
        // find better way to support final transient members
        PhysicalPlan plan = checkpointedState.physicalPlan;
        plan.getLogicalPlan().setAttribute(LogicalPlan.APPLICATION_ATTEMPT_ID, dag.getAttributes().get(LogicalPlan.APPLICATION_ATTEMPT_ID));
        scm = new StreamingContainerManager(checkpointedState, enableEventRecording);
        for (Field f : plan.getClass().getDeclaredFields()) {
          if (f.getType() == PlanContext.class) {
            f.setAccessible(true);
            try {
              f.set(plan, scm);
            }
            catch (Exception e) {
              throw new RuntimeException("Failed to set " + f, e);
            }
            f.setAccessible(false);
          }
        }
        DataInputStream logStream = rh.getLog();
        scm.journal.replay(logStream);
        logStream.close();

        // restore checkpoint info
        plan.syncCheckpoints(scm.vars.windowStartMillis, scm.clock.getTime());
        scm.committedWindowId = scm.updateCheckpoints(true);

        // at this point the physical plan has been fully restored
        // populate container agents for existing containers
        for (PTContainer c : plan.getContainers()) {
          if (c.getExternalId() != null) {
            LOG.debug("Restore container agent {} for {}", c.getExternalId(), c);
            StreamingContainerAgent sca = new StreamingContainerAgent(c, scm.newStreamingContainerContext(c), scm);
            scm.containers.put(c.getExternalId(), sca);
          }
          else {
            LOG.debug("Requesting new resource for {}", c.toIdStateString());
            scm.requestContainer(c);
          }
        }
      }
      scm.recoveryHandler = rh;
      scm.checkpoint();
      return scm;
    }
    catch (IOException e) {
      throw new IllegalStateException("Failed to read checkpointed state", e);
    }
  }

  private static class FinalVars implements java.io.Serializable
  {
    private static final long serialVersionUID = 3827310557521807024L;
    private final long windowStartMillis;
    private final int heartbeatTimeoutMillis;
    private final String appPath;
    private final int maxWindowsBehindForStats;
    private final boolean enableStatsRecording;
    private final int rpcLatencyCompensationSamples;

    private FinalVars(LogicalPlan dag, long tms)
    {
      Attribute.AttributeMap attributes = dag.getAttributes();
      /* try to align to it to please eyes. */
      windowStartMillis = tms - (tms % 1000);

      if (attributes.get(LogicalPlan.APPLICATION_PATH) == null) {
        throw new IllegalArgumentException("Not set: " + LogicalPlan.APPLICATION_PATH);
      }

      this.appPath = attributes.get(LogicalPlan.APPLICATION_PATH);

      if (attributes.get(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS) == null) {
        attributes.put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 500);
      }
      if (attributes.get(LogicalPlan.CHECKPOINT_WINDOW_COUNT) == null) {
        attributes.put(LogicalPlan.CHECKPOINT_WINDOW_COUNT, 30000 / attributes.get(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS));
      }

      this.heartbeatTimeoutMillis = dag.getValue(LogicalPlan.HEARTBEAT_TIMEOUT_MILLIS);
      this.maxWindowsBehindForStats = dag.getValue(LogicalPlan.STATS_MAX_ALLOWABLE_WINDOWS_LAG);
      this.enableStatsRecording = dag.getValue(LogicalPlan.ENABLE_STATS_RECORDING);
      this.rpcLatencyCompensationSamples = dag.getValue(LogicalPlan.RPC_LATENCY_COMPENSATION_SAMPLES);
    }

    private FinalVars(FinalVars other, LogicalPlan dag)
    {
      this.windowStartMillis = other.windowStartMillis;
      this.heartbeatTimeoutMillis = other.heartbeatTimeoutMillis;
      this.maxWindowsBehindForStats = other.maxWindowsBehindForStats;
      this.enableStatsRecording = other.enableStatsRecording;
      this.appPath = dag.getValue(LogicalPlan.APPLICATION_PATH);
      this.rpcLatencyCompensationSamples = other.rpcLatencyCompensationSamples;
    }

  }

  /**
   * The state that can be saved and used to recover the manager.
   */
  static class CheckpointState implements Serializable
  {
    private static final long serialVersionUID = 3827310557521807024L;
    private FinalVars finals;
    private PhysicalPlan physicalPlan;

    /**
     * Modify previously saved state to allow for re-launch of application.
     */
    public void setApplicationId(LogicalPlan newApp, Configuration conf)
    {
      LogicalPlan lp = physicalPlan.getLogicalPlan();
      String appId = newApp.getValue(LogicalPlan.APPLICATION_ID);
      String oldAppId = lp.getValue(LogicalPlan.APPLICATION_ID);
      if (oldAppId == null) {
        throw new AssertionError("Missing original application id");
      }

      lp.setAttribute(LogicalPlan.APPLICATION_ID, appId);
      lp.setAttribute(LogicalPlan.APPLICATION_PATH, newApp.assertAppPath());
      lp.setAttribute(LogicalPlan.LIBRARY_JARS, newApp.getValue(LogicalPlan.LIBRARY_JARS));
      lp.setAttribute(LogicalPlan.ARCHIVES, newApp.getValue(LogicalPlan.ARCHIVES));

      this.finals = new FinalVars(finals, lp);
      StorageAgent sa = lp.getValue(OperatorContext.STORAGE_AGENT);
      if(sa instanceof AsyncFSStorageAgent){
        // replace the default storage agent, if present
        AsyncFSStorageAgent fssa = (AsyncFSStorageAgent) sa;
        if (fssa.path.contains(oldAppId)) {
          fssa = new AsyncFSStorageAgent(fssa.path.replace(oldAppId, appId), conf);
          lp.setAttribute(OperatorContext.STORAGE_AGENT, fssa);
        }
      } else if (sa instanceof FSStorageAgent) {
        // replace the default storage agent, if present
        FSStorageAgent fssa = (FSStorageAgent) sa;
        if (fssa.path.contains(oldAppId)) {
          fssa = new FSStorageAgent(fssa.path.replace(oldAppId, appId), conf);
          lp.setAttribute(OperatorContext.STORAGE_AGENT, fssa);
        }
      }
    }

  }

  public interface RecoveryHandler
  {
    /**
     * Save snapshot.
     *
     * @param state
     * @throws IOException
     */
    void save(Object state) throws IOException;

    /**
     * Restore snapshot. Must get/apply log after restore.
     *
     * @return snapshot
     * @throws IOException
     */
    Object restore() throws IOException;

    /**
     * Backup log. Call before save.
     *
     * @return output stream
     * @throws IOException
     */
    DataOutputStream rotateLog() throws IOException;

    /**
     * Get input stream for log. Call after restore.
     *
     * @return input stream
     * @throws IOException
     */
    DataInputStream getLog() throws IOException;

  }

  private class RequestHandler implements Callable<Object>
  {
    /*
     * The unique requestId of the request
     */
    public long requestId;
    /*
     * The maximum time this thread will wait for the response
     */
    public long waitTime = 5000;

    @Override
    @SuppressWarnings("SleepWhileInLoop")
    public Object call() throws Exception
    {
      Object obj;
      long expiryTime = System.currentTimeMillis() + waitTime;
      while ((obj = commandResponse.getIfPresent(requestId)) == null && expiryTime > System.currentTimeMillis()) {
        Thread.sleep(100);
        LOG.debug("Polling for a response to request with Id {}", requestId);
      }
      if(obj != null) {
        commandResponse.invalidate(requestId);
        return obj;
      }
      return null;
    }
  }

  @VisibleForTesting
  protected Collection<Pair<Long, Map<String, Object>>> getLogicalMetrics(String operatorName)
  {
    if (logicalMetrics.get(operatorName) != null) {
      return Collections.unmodifiableCollection(logicalMetrics.get(operatorName));
    }
    return null;
  }

  @VisibleForTesting
  protected Object getLogicalCounter(String operatorName)
  {
    return latestLogicalCounters.get(operatorName);
  }

}
