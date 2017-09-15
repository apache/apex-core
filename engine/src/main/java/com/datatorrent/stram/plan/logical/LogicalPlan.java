/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.plan.logical;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.ValidationException;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.collect.Sets;

import com.datatorrent.api.AffinityRule;
import com.datatorrent.api.AffinityRulesSet;
import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.DAG;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Module;
import com.datatorrent.api.Module.ProxyInputPort;
import com.datatorrent.api.Module.ProxyOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.StringCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.experimental.AppData;
import com.datatorrent.common.metric.MetricsAggregator;
import com.datatorrent.common.metric.SingleMetricAggregator;
import com.datatorrent.common.metric.sum.DoubleSumAggregator;
import com.datatorrent.common.metric.sum.LongSumAggregator;
import com.datatorrent.common.util.FSStorageAgent;
import com.datatorrent.common.util.Pair;
import com.datatorrent.stram.engine.DefaultUnifier;
import com.datatorrent.stram.engine.Slider;

import static com.datatorrent.api.Context.PortContext.STREAM_CODEC;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * DAG contains the logical declarations of operators and streams.
 * <p>
 * Operators have ports that are connected through streams. Ports can be
 * mandatory or optional with respect to their need to connect a stream to it.
 * Each port can be connected to a single stream only. A stream has to be
 * connected to one output port and can go to multiple input ports.
 * <p>
 * The DAG will be serialized and deployed to the cluster, where it is translated
 * into the physical plan.
 *
 * @since 0.3.2
 */
public class LogicalPlan implements Serializable, DAG
{
  /**
   * Attribute of input port.
   * This is a read-only attribute to query whether the input port is connected to a DelayOperator
   * This is for iterative processing.
   */
  public static final Attribute<Boolean> IS_CONNECTED_TO_DELAY_OPERATOR = new Attribute<>(false);
  @SuppressWarnings("FieldNameHidesFieldInSuperclass")
  private static final long serialVersionUID = -2099729915606048704L;
  private static final Logger LOG = LoggerFactory.getLogger(LogicalPlan.class);
  // The name under which the application master expects its configuration.
  public static final String SER_FILE_NAME = "dt-conf.ser";
  public static final String LAUNCH_CONFIG_FILE_NAME = "dt-launch-config.xml";
  private static final transient AtomicInteger logicalOperatorSequencer = new AtomicInteger();
  public static final String MODULE_NAMESPACE_SEPARATOR = "$";

  /**
   * Constant
   * <code>SUBDIR_CHECKPOINTS="checkpoints"</code>
   */
  public static String SUBDIR_CHECKPOINTS = "checkpoints";
  /**
   * Constant
   * <code>SUBDIR_STATS="stats"</code>
   */
  public static String SUBDIR_STATS = "stats";
  /**
   * Constant
   * <code>SUBDIR_EVENTS="events"</code>
   */
  public static String SUBDIR_EVENTS = "events";

  /**
   * A flag to specify whether to use the fast publisher or not. This attribute was moved
   * from DAGContext. This can be here till the fast publisher is fully tested and working as desired.
   * Then it can be moved back to DAGContext.
   */
  public static Attribute<Boolean> FAST_PUBLISHER_SUBSCRIBER = new Attribute<>(false);
  public static Attribute<Long> HDFS_TOKEN_RENEWAL_INTERVAL = new Attribute<>(86400000L);
  public static Attribute<Long> HDFS_TOKEN_LIFE_TIME = new Attribute<>(604800000L);
  public static Attribute<Long> RM_TOKEN_RENEWAL_INTERVAL = new Attribute<>(YarnConfiguration.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
  public static Attribute<Long> RM_TOKEN_LIFE_TIME = new Attribute<>(YarnConfiguration.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);
  public static Attribute<String> PRINCIPAL = new Attribute<>(null, StringCodec.String2String.getInstance());
  public static Attribute<String> KEY_TAB_FILE = new Attribute<>((String)null, StringCodec.String2String.getInstance());
  public static Attribute<Double> TOKEN_REFRESH_ANTICIPATORY_FACTOR = new Attribute<>(0.7);
  /**
   * Comma separated list of archives to be deployed with the application.
   * The launcher will include the archives into the final set of resources
   * that are made available through the distributed file system to application master
   * and child containers.
   */
  public static Attribute<String> ARCHIVES = new Attribute<>(StringCodec.String2String.getInstance());
  /**
   * Comma separated list of files to be deployed with the application. The launcher will include the files into the
   * final set of resources that are made available through the distributed file system to application master and child
   * containers.
   */
  public static Attribute<String> FILES = new Attribute<>(StringCodec.String2String.getInstance());
  /**
   * The maximum number of containers (excluding the application master) that the application is allowed to request.
   * If the DAG plan requires less containers, remaining count won't be allocated from the resource manager.
   * Example: DAG with several operators and all streams container local would require one container,
   * only one container will be requested from the resource manager.
   */
  public static Attribute<Integer> CONTAINERS_MAX_COUNT = new Attribute<>(Integer.MAX_VALUE);

  /**
   * The application attempt ID from YARN
   */
  public static Attribute<Integer> APPLICATION_ATTEMPT_ID = new Attribute<>(1);

  static {
    Attribute.AttributeMap.AttributeInitializer.initialize(LogicalPlan.class);
  }

  private final Map<String, StreamMeta> streams = new HashMap<>();
  private final Map<String, OperatorMeta> operators = new HashMap<>();
  public final Map<String, ModuleMeta> modules = new LinkedHashMap<>();
  private final List<OperatorMeta> rootOperators = new ArrayList<>();
  private final List<OperatorMeta> leafOperators = new ArrayList<>();
  private final Attribute.AttributeMap attributes = new DefaultAttributeMap();

  @Override
  public Attribute.AttributeMap getAttributes()
  {
    return attributes;
  }

  @Override
  public <T> T getValue(Attribute<T> key)
  {
    T val = attributes.get(key);
    if (val == null) {
      return key.defaultValue;
    }

    return val;
  }

  public LogicalPlan()
  {
  }

  @Override
  public void setCounters(Object counters)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void sendMetrics(Collection<String> metricNames)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public final class InputPortMeta implements DAG.InputPortMeta, Serializable
  {
    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 1L;
    private OperatorMeta operatorMeta;
    private String fieldName;
    private InputPortFieldAnnotation portAnnotation;
    private AppData.QueryPort adqAnnotation;
    private final Attribute.AttributeMap attributes = new DefaultAttributeMap();
    //This is null when port is not hidden
    private Class<?> classDeclaringHiddenPort;

    @Override
    public OperatorMeta getOperatorMeta()
    {
      return operatorMeta;
    }

    public String getPortName()
    {
      return fieldName;
    }

    @Override
    public InputPort<?> getPort()
    {
      for (Map.Entry<InputPort<?>, InputPortMeta> e : operatorMeta.getPortMapping().inPortMap.entrySet()) {
        if (e.getValue() == this) {
          return e.getKey();
        }
      }
      throw new AssertionError("Cannot find the port object for " + this);
    }

    public boolean isAppDataQueryPort()
    {
      return adqAnnotation != null;
    }

    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
          .append("operator", this.operatorMeta)
          .append("portAnnotation", this.portAnnotation)
          .append("adqAnnotation", this.adqAnnotation)
          .append("field", this.fieldName)
          .toString();
    }

    @Override
    public Attribute.AttributeMap getAttributes()
    {
      return attributes;
    }

    @Override
    public <T> T getValue(Attribute<T> key)
    {
      T attr = attributes.get(key);
      if (attr == null) {
        return key.defaultValue;
      }

      return attr;
    }

    @Override
    public void setCounters(Object counters)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void sendMetrics(Collection<String> metricNames)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public StreamCodec<?> getStreamCodec()
    {
      return attributes.get(STREAM_CODEC);
    }

    void setStreamCodec(StreamCodec<?> streamCodec)
    {
      if (streamCodec != null) {
        StreamCodec<?> oldStreamCodec = attributes.put(STREAM_CODEC, streamCodec);
        if (oldStreamCodec != null && oldStreamCodec != streamCodec) { // once input port codec is set, it is not expected that it will be changed.
          LOG.warn("Operator {} input port {} stream codec was changed from {} to {}", getOperatorMeta().getName(), getPortName(), oldStreamCodec, streamCodec);
        }
      }
    }
  }

  public final class OutputPortMeta implements DAG.OutputPortMeta, Serializable
  {
    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 201412091633L;
    private OperatorMeta operatorMeta;
    private OperatorMeta unifierMeta;
    private OperatorMeta sliderMeta;
    private String fieldName;
    private OutputPortFieldAnnotation portAnnotation;
    private AppData.ResultPort adrAnnotation;
    private final DefaultAttributeMap attributes;
    //This is null when port is not hidden
    private Class<?> classDeclaringHiddenPort;

    public OutputPortMeta()
    {
      this.attributes = new DefaultAttributeMap();
    }

    @Override
    public OperatorMeta getOperatorMeta()
    {
      return operatorMeta;
    }

    @Override
    public OperatorMeta getUnifierMeta()
    {
      if (unifierMeta == null) {
        unifierMeta = new OperatorMeta(operatorMeta.getName() + '.' + fieldName + "#unifier",  getUnifier());
      }

      return unifierMeta;
    }

    public OperatorMeta getSlidingUnifier(int numberOfBuckets, int slidingApplicationWindowCount, int numberOfSlidingWindows)
    {
      if (sliderMeta == null) {
        @SuppressWarnings("unchecked")
        Slider slider = new Slider((Unifier<Object>)getUnifier(), numberOfBuckets, numberOfSlidingWindows);
        try {
          sliderMeta = new OperatorMeta(operatorMeta.getName() + '.' + fieldName + "#slider", slider, getUnifierMeta().attributes.clone());
        } catch (CloneNotSupportedException ex) {
          throw new RuntimeException(ex);
        }
        sliderMeta.getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, slidingApplicationWindowCount);
      }
      return sliderMeta;
    }

    public String getPortName()
    {
      return fieldName;
    }

    @Override
    public OutputPort<?> getPort()
    {
      for (Map.Entry<OutputPort<?>, OutputPortMeta> e : operatorMeta.getPortMapping().outPortMap.entrySet()) {
        if (e.getValue() == this) {
          return e.getKey();
        }
      }
      throw new AssertionError("Cannot find the port object for " + this);
    }

    public Operator.Unifier<?> getUnifier()
    {
      for (Map.Entry<OutputPort<?>, OutputPortMeta> e : operatorMeta.getPortMapping().outPortMap.entrySet()) {
        if (e.getValue() == this) {
          Unifier<?> unifier = e.getKey().getUnifier();
          if (unifier == null) {
            break;
          }
          LOG.debug("User supplied unifier is {}", unifier);
          return unifier;
        }
      }

      LOG.debug("Using default unifier for {}", this);
      return new DefaultUnifier();
    }

    @Override
    public Attribute.AttributeMap getAttributes()
    {
      return attributes;
    }

    @Override
    public <T> T getValue(Attribute<T> key)
    {
      T attr = attributes.get(key);
      if (attr == null) {
        return key.defaultValue;
      }

      return attr;
    }

    public boolean isAppDataResultPort()
    {
      return adrAnnotation != null;
    }

    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
          .append("operator", this.operatorMeta)
          .append("portAnnotation", this.portAnnotation)
          .append("adrAnnotation", this.adrAnnotation)
          .append("field", this.fieldName)
          .toString();
    }

    @Override
    public void setCounters(Object counters)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void sendMetrics(Collection<String> metricNames)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

  }

  /**
   * Representation of streams in the logical layer. Instances are created through {@link LogicalPlan#addStream}.
   */
  public final class StreamMeta implements DAG.StreamMeta, Serializable
  {
    private static final long serialVersionUID = 1L;
    private Locality locality;
    private final List<InputPortMeta> sinks = new ArrayList<>();
    private OutputPortMeta source;
    private final String id;
    private OperatorMeta persistOperatorForStream;
    private InputPortMeta persistOperatorInputPort;
    private Set<InputPortMeta> enableSinksForPersisting;
    private String persistOperatorName;
    public Map<InputPortMeta, OperatorMeta> sinkSpecificPersistOperatorMap;
    public Map<InputPortMeta, InputPortMeta> sinkSpecificPersistInputPortMap;

    private StreamMeta(String id)
    {
      this.id = id;
      enableSinksForPersisting = new HashSet<>();
      sinkSpecificPersistOperatorMap = new HashMap<>();
      sinkSpecificPersistInputPortMap = new HashMap<>();
    }

    @Override
    public String getName()
    {
      return id;
    }

    @Override
    public Locality getLocality()
    {
      return this.locality;
    }

    @Override
    public StreamMeta setLocality(Locality locality)
    {
      this.locality = locality;
      return this;
    }

    public OutputPortMeta getSource()
    {
      return source;
    }

    @Override
    public StreamMeta setSource(Operator.OutputPort<?> port)
    {
      if (port instanceof ProxyOutputPort) {
        proxySource = port;
        return this;
      }
      OutputPortMeta portMeta = assertGetPortMeta(port);
      OperatorMeta om = portMeta.getOperatorMeta();
      if (om.outputStreams.containsKey(portMeta)) {
        String msg = String.format("Operator %s already connected to %s", om.name, om.outputStreams.get(portMeta).id);
        throw new IllegalArgumentException(msg);
      }
      this.source = portMeta;
      om.outputStreams.put(portMeta, this);
      return this;
    }

    @Override
    public Collection<InputPortMeta> getSinks()
    {
      return sinks;
    }

    @Override
    public StreamMeta addSink(Operator.InputPort<?> port)
    {
      if (port instanceof ProxyInputPort) {
        proxySinks.add(port);
        return this;
      }
      InputPortMeta portMeta = assertGetPortMeta(port);
      OperatorMeta om = portMeta.getOperatorMeta();
      String portName = portMeta.getPortName();
      if (om.inputStreams.containsKey(portMeta)) {
        throw new IllegalArgumentException(String.format("Port %s already connected to stream %s", portName, om.inputStreams.get(portMeta)));
      }

      /*
      finalizeValidate(portMeta);
      */

      sinks.add(portMeta);
      om.inputStreams.put(portMeta, this);
      rootOperators.remove(portMeta.operatorMeta);
      if (this.source != null && !(this.source.getOperatorMeta().getOperator() instanceof Operator.DelayOperator)) {
        leafOperators.remove(this.source.getOperatorMeta());
      }
      return this;
    }

    public void remove()
    {
      for (InputPortMeta ipm : this.sinks) {
        ipm.getOperatorMeta().inputStreams.remove(ipm);
        if (ipm.getOperatorMeta().inputStreams.isEmpty()) {
          rootOperators.add(ipm.getOperatorMeta());
        }
      }
      // Remove persist operator for at stream level if present:
      if (getPersistOperator() != null) {
        removeOperator(getPersistOperator().getOperator());
      }

      // Remove persist operators added for specific sinks :
      if (!sinkSpecificPersistOperatorMap.isEmpty()) {
        for (Entry<InputPortMeta, OperatorMeta> entry : sinkSpecificPersistOperatorMap.entrySet()) {
          removeOperator(entry.getValue().getOperator());
        }
        sinkSpecificPersistOperatorMap.clear();
        sinkSpecificPersistInputPortMap.clear();
      }
      this.sinks.clear();
      if (this.source != null) {
        this.source.getOperatorMeta().outputStreams.remove(this.source);
        if (this.source.getOperatorMeta().outputStreams.isEmpty() &&
            !(this.source.getOperatorMeta().getOperator() instanceof Operator.DelayOperator)) {
          leafOperators.remove(this.source.getOperatorMeta());
        }
      }
      this.source = null;
      streams.remove(this.id);
    }

    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
          .append("id", this.id)
          .toString();
    }

    @Override
    public int hashCode()
    {
      int hash = 7;
      hash = 31 * hash + (this.locality != null ? this.locality.hashCode() : 0);
      hash = 31 * hash + (this.source != null ? this.source.hashCode() : 0);
      hash = 31 * hash + (this.id != null ? this.id.hashCode() : 0);
      return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final StreamMeta other = (StreamMeta)obj;
      if (this.locality != other.locality) {
        return false;
      }
      if (this.sinks != other.sinks && (this.sinks == null || !this.sinks.equals(other.sinks))) {
        return false;
      }
      if (this.source != other.source && (this.source == null || !this.source.equals(other.source))) {
        return false;
      }
      return !((this.id == null) ? (other.id != null) : !this.id.equals(other.id));
    }

    @Override
    public StreamMeta persistUsing(String name, Operator persistOperator, InputPort<?> port)
    {
      persistOperatorName = name;
      enablePersistingForSinksAddedSoFar(persistOperator);
      OperatorMeta persistOpMeta = createPersistOperatorMeta(persistOperator);
      if (!persistOpMeta.getPortMapping().inPortMap.containsKey(port)) {
        String msg = String.format("Port argument %s does not belong to persist operator passed %s", port, persistOperator);
        throw new IllegalArgumentException(msg);
      }

      setPersistOperatorInputPort(persistOpMeta.getPortMapping().inPortMap.get(port));

      return this;
    }

    @Override
    public StreamMeta persistUsing(String name, Operator persistOperator)
    {
      persistOperatorName = name;
      enablePersistingForSinksAddedSoFar(persistOperator);
      OperatorMeta persistOpMeta = createPersistOperatorMeta(persistOperator);
      InputPortMeta port = persistOpMeta.getPortMapping().inPortMap.values().iterator().next();
      setPersistOperatorInputPort(port);
      return this;
    }

    private void enablePersistingForSinksAddedSoFar(Operator persistOperator)
    {
      for (InputPortMeta portMeta : getSinks()) {
        enableSinksForPersisting.add(portMeta);
      }
    }

    private OperatorMeta createPersistOperatorMeta(Operator persistOperator)
    {
      addOperator(persistOperatorName, persistOperator);
      OperatorMeta persistOpMeta = getOperatorMeta(persistOperatorName);
      setPersistOperator(persistOpMeta);
      if (persistOpMeta.getPortMapping().inPortMap.isEmpty()) {
        String msg = String.format("Persist operator passed %s has no input ports to connect", persistOperator);
        throw new IllegalArgumentException(msg);
      }
      Map<InputPort<?>, InputPortMeta> inputPortMap = persistOpMeta.getPortMapping().inPortMap;
      int nonOptionalInputPortCount = 0;
      for (InputPortMeta inputPort : inputPortMap.values()) {
        if (inputPort.portAnnotation == null || !inputPort.portAnnotation.optional()) {
          // By default input port is non-optional unless specified
          nonOptionalInputPortCount++;
        }
      }

      if (nonOptionalInputPortCount > 1) {
        String msg = String.format("Persist operator %s has more than 1 non optional input port", persistOperator);
        throw new IllegalArgumentException(msg);
      }

      Map<OutputPort<?>, OutputPortMeta> outputPortMap = persistOpMeta.getPortMapping().outPortMap;
      for (OutputPortMeta outPort : outputPortMap.values()) {
        if (outPort.portAnnotation != null && !outPort.portAnnotation.optional()) {
          // By default output port is optional unless specified
          String msg = String.format("Persist operator %s has non optional output port %s", persistOperator, outPort.fieldName);
          throw new IllegalArgumentException(msg);
        }
      }
      return persistOpMeta;
    }

    public OperatorMeta getPersistOperator()
    {
      return persistOperatorForStream;
    }

    private void setPersistOperator(OperatorMeta persistOperator)
    {
      this.persistOperatorForStream = persistOperator;
    }

    public InputPortMeta getPersistOperatorInputPort()
    {
      return persistOperatorInputPort;
    }

    private void setPersistOperatorInputPort(InputPortMeta inport)
    {
      this.addSink(inport.getPort());
      this.persistOperatorInputPort = inport;
    }

    public Set<InputPortMeta> getSinksToPersist()
    {
      return enableSinksForPersisting;
    }

    private String getPersistOperatorName(Operator operator)
    {
      return id + "_persister";
    }

    private String getPersistOperatorName(InputPort<?> sinkToPersist)
    {
      InputPortMeta portMeta = assertGetPortMeta(sinkToPersist);
      OperatorMeta operatorMeta = portMeta.getOperatorMeta();
      return id + "_" + operatorMeta.getName() + "_persister";
    }

    @Override
    public StreamMeta persistUsing(String name, Operator persistOperator, InputPort<?> port, InputPort<?> sinkToPersist)
    {
      // When persist Stream is invoked for a specific sink, persist operator can directly be added
      String persistOperatorName = name;
      addOperator(persistOperatorName, persistOperator);
      addSink(port);
      InputPortMeta sinkPortMeta = assertGetPortMeta(sinkToPersist);
      addStreamCodec(sinkPortMeta, port);
      updateSinkSpecificPersistOperatorMap(sinkPortMeta, persistOperatorName, port);
      return this;
    }

    private void addStreamCodec(InputPortMeta sinkToPersistPortMeta, InputPort<?> port)
    {
      StreamCodec<Object> inputStreamCodec = (StreamCodec<Object>)sinkToPersistPortMeta.getStreamCodec();
      if (inputStreamCodec != null) {
        Map<InputPortMeta, StreamCodec<Object>> codecs = new HashMap<>();
        codecs.put(sinkToPersistPortMeta, inputStreamCodec);
        InputPortMeta persistOperatorPortMeta = assertGetPortMeta(port);
        StreamCodec<Object> specifiedCodecForPersistOperator = (StreamCodec<Object>)persistOperatorPortMeta.getStreamCodec();
        StreamCodecWrapperForPersistance<Object> codec = new StreamCodecWrapperForPersistance<>(codecs, specifiedCodecForPersistOperator);
        persistOperatorPortMeta.setStreamCodec(codec);
      }
    }

    private void updateSinkSpecificPersistOperatorMap(InputPortMeta sinkToPersistPortMeta, String persistOperatorName, InputPort<?> persistOperatorInPort)
    {
      OperatorMeta persistOpMeta = operators.get(persistOperatorName);
      this.sinkSpecificPersistOperatorMap.put(sinkToPersistPortMeta, persistOpMeta);
      this.sinkSpecificPersistInputPortMap.put(sinkToPersistPortMeta, persistOpMeta.getMeta(persistOperatorInPort));
    }

    public void resetStreamPersistanceOnSinkRemoval(InputPortMeta sinkBeingRemoved)
    {
     /*
      * If persistStream was enabled for the entire stream and the operator
      * to be removed was the only one enabled for persisting, Remove the persist operator
      */
      if (enableSinksForPersisting.contains(sinkBeingRemoved)) {
        enableSinksForPersisting.remove(sinkBeingRemoved);
        if (enableSinksForPersisting.isEmpty()) {
          removeOperator(getPersistOperator().getOperator());
          setPersistOperator(null);
        }
      }

      // If persisting was added specific to this sink, remove the persist operator
      if (sinkSpecificPersistInputPortMap.containsKey(sinkBeingRemoved)) {
        sinkSpecificPersistInputPortMap.remove(sinkBeingRemoved);
      }
      if (sinkSpecificPersistOperatorMap.containsKey(sinkBeingRemoved)) {
        OperatorMeta persistOpMeta = sinkSpecificPersistOperatorMap.get(sinkBeingRemoved);
        sinkSpecificPersistOperatorMap.remove(sinkBeingRemoved);
        removeOperator(persistOpMeta.getOperator());
      }
    }

    private OutputPort<?> proxySource = null;
    private List<InputPort<?>> proxySinks = new ArrayList<>();

    /**
     * Go over each Proxy port and find out the actual port connected to the ProxyPort
     * and update StreamMeta.
     */
    private void resolvePorts()
    {
      if (proxySource != null && proxySource instanceof ProxyOutputPort) {
        OutputPort<?> outputPort = proxySource;
        while (outputPort instanceof ProxyOutputPort) {
          outputPort = ((ProxyOutputPort<?>)outputPort).get();
        }
        setSource(outputPort);
      }

      for (InputPort<?> inputPort : proxySinks) {
        while (inputPort instanceof ProxyInputPort) {
          inputPort = ((ProxyInputPort<?>)inputPort).get();
        }
        addSink(inputPort);
      }

      proxySource = null;
      proxySinks.clear();
    }
  }

  /**
   * Operator meta object.
   */
  public class OperatorMeta implements DAG.OperatorMeta, Serializable
  {
    private final LinkedHashMap<InputPortMeta, StreamMeta> inputStreams = new LinkedHashMap<>();
    private final LinkedHashMap<OutputPortMeta, StreamMeta> outputStreams = new LinkedHashMap<>();
    private final Attribute.AttributeMap attributes;
    @SuppressWarnings("unused")
    private final int id;
    @NotNull
    private final String name;
    private final OperatorAnnotation operatorAnnotation;
    private final LogicalOperatorStatus status;
    private transient Integer nindex; // for cycle detection
    private transient Integer lowlink; // for cycle detection
    private transient GenericOperator operator;
    private MetricAggregatorMeta metricAggregatorMeta;
    private String moduleName;  // Name of the module which has this operator. null if this is a top level operator.

    /*
     * Used for  OIO validation,
     *  value null => node not visited yet
     *  other value => represents the root oio node for this node
     */
    private transient Integer oioRoot = null;
    private ClassUtils genricOperator;

    private OperatorMeta(String name, GenericOperator operator)
    {
      this(name, operator, new DefaultAttributeMap());
    }

    private OperatorMeta(String name, GenericOperator operator, Attribute.AttributeMap attributeMap)
    {
      LOG.debug("Initializing {} as {}", name, operator.getClass().getName());
      this.operatorAnnotation = operator.getClass().getAnnotation(OperatorAnnotation.class);
      this.name = name;
      this.operator = operator;
      this.id = logicalOperatorSequencer.decrementAndGet();
      this.status = new LogicalOperatorStatus(name);
      this.attributes = attributeMap;
    }

    @Override
    public String getName()
    {
      return name;
    }

    @Override
    public Attribute.AttributeMap getAttributes()
    {
      return attributes;
    }

    @Override
    public <T> T getValue(Attribute<T> key)
    {
      T attr = attributes.get(key);
      if (attr == null) {
        attr = LogicalPlan.this.getValue(key);
      }
      if (attr == null) {
        return key.defaultValue;
      }
      return attr;
    }

    public LogicalOperatorStatus getStatus()
    {
      return status;
    }

    private void writeObject(ObjectOutputStream out) throws IOException
    {
      //getValue2(OperatorContext.STORAGE_AGENT).save(operator, id, Checkpoint.STATELESS_CHECKPOINT_WINDOW_ID);
      out.defaultWriteObject();
      FSStorageAgent.store(out, operator);
    }

    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException
    {
      input.defaultReadObject();
      // TODO: not working because - we don't have the storage agent in parent attribuet map
      //operator = (Operator)getValue2(OperatorContext.STORAGE_AGENT).load(id, Checkpoint.STATELESS_CHECKPOINT_WINDOW_ID);
      operator = (GenericOperator)FSStorageAgent.retrieve(input);
    }

    @Override
    public void setCounters(Object counters)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void sendMetrics(Collection<String> metricNames)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public MetricAggregatorMeta getMetricAggregatorMeta()
    {
      return metricAggregatorMeta;
    }

    public String getModuleName()
    {
      return moduleName;
    }

    public void setModuleName(String moduleName)
    {
      this.moduleName = moduleName;
    }

    protected void populateAggregatorMeta()
    {
      AutoMetric.Aggregator aggregator = getValue(OperatorContext.METRICS_AGGREGATOR);
      if (aggregator == null && operator instanceof AutoMetric.Aggregator) {
        aggregator = new MetricAggregatorMeta.MetricsAggregatorProxy(this);
      }
      if (aggregator == null) {
        MetricsAggregator defAggregator = null;
        Set<String> metricNames = Sets.newHashSet();

        for (Field field : ReflectionUtils.getDeclaredFieldsIncludingInherited(operator.getClass())) {

          if (field.isAnnotationPresent(AutoMetric.class)) {
            metricNames.add(field.getName());

            if (field.getType() == int.class || field.getType() == Integer.class ||
                field.getType() == long.class || field.getType() == Long.class) {
              if (defAggregator == null) {
                defAggregator = new MetricsAggregator();
              }
              defAggregator.addAggregators(field.getName(), new SingleMetricAggregator[]{new LongSumAggregator()});
            } else if (field.getType() == float.class || field.getType() == Float.class ||
                field.getType() == double.class || field.getType() == Double.class) {
              if (defAggregator == null) {
                defAggregator = new MetricsAggregator();
              }
              defAggregator.addAggregators(field.getName(), new SingleMetricAggregator[]{new DoubleSumAggregator()});
            }
          }
        }

        try {
          for (PropertyDescriptor pd : Introspector.getBeanInfo(operator.getClass()).getPropertyDescriptors()) {
            Method readMethod = pd.getReadMethod();
            if (readMethod != null) {
              AutoMetric rfa = readMethod.getAnnotation(AutoMetric.class);
              if (rfa != null) {
                String propName = pd.getName();
                if (metricNames.contains(propName)) {
                  continue;
                }

                if (readMethod.getReturnType() == int.class || readMethod.getReturnType() == Integer.class ||
                    readMethod.getReturnType() == long.class || readMethod.getReturnType() == Long.class) {

                  if (defAggregator == null) {
                    defAggregator = new MetricsAggregator();
                  }
                  defAggregator.addAggregators(propName, new SingleMetricAggregator[]{new LongSumAggregator()});

                } else if (readMethod.getReturnType() == float.class || readMethod.getReturnType() == Float.class ||
                    readMethod.getReturnType() == double.class || readMethod.getReturnType() == Double.class) {

                  if (defAggregator == null) {
                    defAggregator = new MetricsAggregator();
                  }
                  defAggregator.addAggregators(propName, new SingleMetricAggregator[]{new DoubleSumAggregator()});
                }
              }
            }
          }
        } catch (IntrospectionException e) {
          throw new RuntimeException("finding methods", e);
        }

        if (defAggregator != null) {
          aggregator = defAggregator;
        }
      }
      this.metricAggregatorMeta = new MetricAggregatorMeta(aggregator,
          getValue(OperatorContext.METRICS_DIMENSIONS_SCHEME));
    }

    /**
     * Copy attribute from source attributeMap to destination attributeMap.
     *
     * @param dest  destination attribute map.
     * @param source source attribute map.
     */
    private void copyAttributes(AttributeMap dest, AttributeMap source)
    {
      for (Entry<Attribute<?>, ?> a : source.entrySet()) {
        dest.put((Attribute<Object>)a.getKey(), a.getValue());
      }
    }

    /**
     * Copy attribute of operator and port from provided operatorMeta. This function requires
     * operatorMeta argument is for the same operator.
     *
     * @param operatorMeta copy attribute from this OperatorMeta to the object.
     */
    private void copyAttributesFrom(OperatorMeta operatorMeta)
    {
      if (operator != operatorMeta.getOperator()) {
        throw new IllegalArgumentException("Operator meta is not for the same operator ");
      }

      // copy operator attributes
      copyAttributes(attributes, operatorMeta.getAttributes());

      // copy Input port attributes
      for (Map.Entry<InputPort<?>, InputPortMeta> entry : operatorMeta.getPortMapping().inPortMap.entrySet()) {
        copyAttributes(getPortMapping().inPortMap.get(entry.getKey()).attributes, entry.getValue().attributes);
      }

      // copy Output port attributes
      for (Map.Entry<OutputPort<?>, OutputPortMeta> entry : operatorMeta.getPortMapping().outPortMap.entrySet()) {
        copyAttributes(getPortMapping().outPortMap.get(entry.getKey()).attributes, entry.getValue().attributes);

        // copy Unifier attributes
        copyAttributes(getPortMapping().outPortMap.get(entry.getKey()).getUnifierMeta().attributes,
            entry.getValue().getUnifierMeta().attributes);
      }
    }


    private class PortMapping implements Operators.OperatorDescriptor
    {
      private final Map<Operator.InputPort<?>, InputPortMeta> inPortMap = new HashMap<>();
      private final Map<Operator.OutputPort<?>, OutputPortMeta> outPortMap = new HashMap<>();
      private final Map<String, Object> portNameMap = new HashMap<>();

      @Override
      public void addInputPort(InputPort<?> portObject, Field field, InputPortFieldAnnotation portAnnotation, AppData.QueryPort adqAnnotation)
      {
        if (!OperatorMeta.this.inputStreams.isEmpty()) {
          for (Map.Entry<LogicalPlan.InputPortMeta, LogicalPlan.StreamMeta> e : OperatorMeta.this.inputStreams.entrySet()) {
            LogicalPlan.InputPortMeta pm = e.getKey();
            if (pm.operatorMeta == OperatorMeta.this && pm.fieldName.equals(field.getName())) {
              //LOG.debug("Found existing port meta for: " + field);
              inPortMap.put(portObject, pm);
              markInputPortIfHidden(pm.getPortName(), pm, field.getDeclaringClass());
              return;
            }
          }
        }
        InputPortMeta metaPort = new InputPortMeta();
        metaPort.operatorMeta = OperatorMeta.this;
        metaPort.fieldName = field.getName();
        metaPort.portAnnotation = portAnnotation;
        metaPort.adqAnnotation = adqAnnotation;
        inPortMap.put(portObject, metaPort);
        markInputPortIfHidden(metaPort.getPortName(), metaPort, field.getDeclaringClass());
        if (metaPort.getStreamCodec() == null) {
          metaPort.setStreamCodec(portObject.getStreamCodec());
        } else if (portObject.getStreamCodec() != null) {
          LOG.info("Operator {} input port {} attribute {} overrides codec {} with {} codec", metaPort.getOperatorMeta().getName(),
              metaPort.getPortName(), STREAM_CODEC.getSimpleName(), portObject.getStreamCodec(), metaPort.getStreamCodec());
        }
      }

      @Override
      public void addOutputPort(OutputPort<?> portObject, Field field, OutputPortFieldAnnotation portAnnotation, AppData.ResultPort adrAnnotation)
      {
        if (!OperatorMeta.this.outputStreams.isEmpty()) {
          for (Map.Entry<LogicalPlan.OutputPortMeta, LogicalPlan.StreamMeta> e : OperatorMeta.this.outputStreams.entrySet()) {
            LogicalPlan.OutputPortMeta pm = e.getKey();
            if (pm.operatorMeta == OperatorMeta.this && pm.fieldName.equals(field.getName())) {
              //LOG.debug("Found existing port meta for: " + field);
              outPortMap.put(portObject, pm);
              markOutputPortIfHidden(pm.getPortName(), pm, field.getDeclaringClass());
              return;
            }
          }
        }
        OutputPortMeta metaPort = new OutputPortMeta();
        metaPort.operatorMeta = OperatorMeta.this;
        metaPort.fieldName = field.getName();
        metaPort.portAnnotation = portAnnotation;
        metaPort.adrAnnotation = adrAnnotation;
        outPortMap.put(portObject, metaPort);
        markOutputPortIfHidden(metaPort.getPortName(), metaPort, field.getDeclaringClass());
      }

      private void markOutputPortIfHidden(String portName, OutputPortMeta portMeta, Class<?> declaringClass)
      {
        if (!portNameMap.containsKey(portName)) {
          portNameMap.put(portName, portMeta);
        } else {
          // make the port optional
          portMeta.classDeclaringHiddenPort = declaringClass;
        }

      }

      private void markInputPortIfHidden(String portName, InputPortMeta portMeta, Class<?> declaringClass)
      {
        if (!portNameMap.containsKey(portName)) {
          portNameMap.put(portName, portMeta);
        } else {
          // make the port optional
          portMeta.classDeclaringHiddenPort = declaringClass;
        }
      }
    }

    /**
     * Ports objects are transient, we keep a lazy initialized mapping
     */
    private transient PortMapping portMapping = null;

    private PortMapping getPortMapping()
    {
      if (this.portMapping == null) {
        this.portMapping = new PortMapping();
        Operators.describe(this.getOperator(), portMapping);
      }
      return portMapping;
    }

    @Override
    public OutputPortMeta getMeta(Operator.OutputPort<?> port)
    {
      return getPortMapping().outPortMap.get(port);
    }

    @Override
    public OperatorAnnotation getOperatorAnnotation()
    {
      return operatorAnnotation;
    }

    @Override
    public InputPortMeta getMeta(Operator.InputPort<?> port)
    {
      return getPortMapping().inPortMap.get(port);
    }

    @Override
    public Map<InputPortMeta, StreamMeta> getInputStreams()
    {
      return this.inputStreams;
    }

    public Map<OutputPortMeta, StreamMeta> getOutputStreams()
    {
      return this.outputStreams;
    }

    @Override
    public Operator getOperator()
    {
      return (Operator)operator;
    }

    public GenericOperator getGenericOperator()
    {
      return operator;
    }

    public LogicalPlan getDAG()
    {
      return LogicalPlan.this;
    }

    @Override
    public String toString()
    {
      return "OperatorMeta{" + "name=" + name + ", operator=" + operator + ", attributes=" + attributes + '}';
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof OperatorMeta)) {
        return false;
      }

      OperatorMeta that = (OperatorMeta)o;

      if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) {
        return false;
      }
      if (!name.equals(that.name)) {
        return false;
      }
      if (operatorAnnotation != null ? !operatorAnnotation.equals(that.operatorAnnotation) : that.operatorAnnotation != null) {
        return false;
      }
      return !(operator != null ? !operator.equals(that.operator) : that.operator != null);
    }

    @Override
    public int hashCode()
    {
      return name.hashCode();
    }

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 201401091635L;
  }

  @Override
  public <T extends Operator> T addOperator(@Nonnull String name, Class<T> clazz)
  {
    T instance;
    try {
      instance = clazz.newInstance();
    } catch (Exception ex) {
      throw new IllegalArgumentException(ex);
    }
    addOperator(name, instance);
    return instance;
  }

  @Override
  public <T extends Operator> T addOperator(@Nonnull String name, T operator)
  {
    checkArgument(!isNullOrEmpty(name), "operator name is null or empty");

    if (operators.containsKey(name)) {
      if (operators.get(name).operator == operator) {
        return operator;
      }
      throw new IllegalArgumentException("duplicate operator name: " + operators.get(name));
    }

    // Avoid name conflict with module.
    if (modules.containsKey(name)) {
      throw new IllegalArgumentException("duplicate operator name: " + operators.get(name));
    }
    OperatorMeta decl = new OperatorMeta(name, operator);
    rootOperators.add(decl); // will be removed when a sink is added to an input port for this operator
    leafOperators.add(decl); // will be removed when a sink is added to an output port for this operator
    operators.put(name, decl);
    return operator;
  }

  /**
   * Module meta object.
   */
  public final class ModuleMeta extends OperatorMeta implements Serializable
  {
    private ModuleMeta parent;
    private LogicalPlan dag = null;
    private transient String fullName;
    //type-casted reference to the module.
    private transient Module module;
    private transient boolean flattened = false;

    private ModuleMeta(String name, Module module)
    {
      super(name, module);
      this.module = module;
      LOG.debug("Initializing {} as {}", name, module.getClass().getName());
      this.dag = new LogicalPlan();
    }

    public LogicalPlan getDag()
    {
      return dag;
    }

    /**
     * Expand the module and add its operator to the parentDAG. After this method finishes the module is expanded fully
     * with all its submodules also expanded. The parentDAG contains the operator added by all the modules.
     *
     * @param parentDAG parent dag to populate with operators from this and inner modules.
     * @param conf      configuration object.
     */
    public void flattenModule(LogicalPlan parentDAG, Configuration conf)
    {
      if (flattened) {
        return;
      }

      module.populateDAG(dag, conf);
      for (ModuleMeta subModuleMeta : dag.getAllModules()) {
        subModuleMeta.setParent(this);
        subModuleMeta.flattenModule(dag, conf);
      }
      dag.applyStreamLinks();
      parentDAG.addDAGToCurrentDAG(this);
      flattened = true;
    }

    /**
     * Return full name of the module. If this is a inner module, i.e module inside of module this method will traverse
     * till the top level module, and construct the name by concatenating name of modules in the chain in reverse order
     * separated by MODULE_NAMESPACE_SEPARATO.
     *
     * For example If there is module M1, which adds another module M2 in the DAG. Then the full name of the module M2
     * is ("M1" ++ MODULE_NAMESPACE_SEPARATO + "M2")
     *
     * @return full name of the module.
     */
    public String getFullName()
    {
      if (fullName != null) {
        return fullName;
      }

      if (parent == null) {
        fullName = getName();
      } else {
        fullName = parent.getFullName() + MODULE_NAMESPACE_SEPARATOR + getName();
      }
      return fullName;
    }

    private void setParent(ModuleMeta meta)
    {
      this.parent = meta;
    }

    private static final long serialVersionUID = 7562277769188329223L;
  }

  @Override
  public <T extends Module> T addModule(@Nonnull String name, T module)
  {
    checkArgument(!isNullOrEmpty(name), "module name is null or empty");

    if (modules.containsKey(name)) {
      if (modules.get(name).module == module) {
        return module;
      }
      throw new IllegalArgumentException("duplicate module name: " + modules.get(name));
    }
    if (operators.containsKey(name)) {
      throw new IllegalArgumentException("duplicate module name: " + modules.get(name));
    }

    ModuleMeta meta = new ModuleMeta(name, module);
    modules.put(name, meta);
    return module;
  }

  @Override
  public <T extends Module> T addModule(@Nonnull String name, Class<T> clazz)
  {
    T instance;
    try {
      instance = clazz.newInstance();
    } catch (Exception ex) {
      throw new IllegalArgumentException(ex);
    }
    addModule(name, instance);
    return instance;
  }

  public void removeOperator(Operator operator)
  {
    OperatorMeta om = getMeta(operator);
    if (om == null) {
      return;
    }

    Map<InputPortMeta, StreamMeta> inputStreams = om.getInputStreams();
    for (Map.Entry<InputPortMeta, StreamMeta> e : inputStreams.entrySet()) {
      StreamMeta stream = e.getValue();
      if (e.getKey().getOperatorMeta() == om) {
        stream.sinks.remove(e.getKey());
      }
      // If persistStream was enabled for stream, reset stream when sink removed
      stream.resetStreamPersistanceOnSinkRemoval(e.getKey());
    }
    this.operators.remove(om.getName());
    rootOperators.remove(om);
    leafOperators.remove(om);
  }

  @Override
  public StreamMeta addStream(@Nonnull String id)
  {
    checkArgument(!isNullOrEmpty(id),"stream id is null or empty");
    StreamMeta s = new StreamMeta(id);
    StreamMeta o = streams.put(id, s);
    if (o == null) {
      return s;
    }

    throw new IllegalArgumentException("duplicate stream id: " + o);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> StreamMeta addStream(@Nonnull String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T>... sinks)
  {
    StreamMeta s = addStream(id);
    s.setSource(source);
    for (Operator.InputPort<?> sink : sinks) {
      s.addSink(sink);
    }
    return s;
  }

  /**
   * This will be called once the Logical Dag is expanded, and proxy input and proxy output ports are populated
   * with actual ports they refer to.
   */
  public void applyStreamLinks()
  {
    for (StreamMeta smeta : streams.values()) {
      smeta.resolvePorts();
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void addDAGToCurrentDAG(ModuleMeta moduleMeta)
  {
    LogicalPlan subDag = moduleMeta.getDag();
    String subDAGName = moduleMeta.getName();
    String name;
    for (OperatorMeta operatorMeta : subDag.getAllOperators()) {
      name = subDAGName + MODULE_NAMESPACE_SEPARATOR + operatorMeta.getName();
      Operator op = this.addOperator(name, operatorMeta.getOperator());
      OperatorMeta operatorMetaNew = this.getMeta(op);
      operatorMetaNew.copyAttributesFrom(operatorMeta);
      operatorMetaNew.setModuleName(operatorMeta.getModuleName() == null ? subDAGName :
          subDAGName + MODULE_NAMESPACE_SEPARATOR + operatorMeta.getModuleName());
    }

    for (StreamMeta streamMeta : subDag.getAllStreams()) {
      OutputPortMeta sourceMeta = streamMeta.getSource();
      List<InputPort<?>> ports = new LinkedList<>();
      for (InputPortMeta inputPortMeta : streamMeta.getSinks()) {
        ports.add(inputPortMeta.getPort());
      }
      InputPort[] inputPorts = ports.toArray(new InputPort[]{});

      name = subDAGName + MODULE_NAMESPACE_SEPARATOR + streamMeta.getName();
      StreamMeta streamMetaNew = this.addStream(name, sourceMeta.getPort(), inputPorts);
      streamMetaNew.setLocality(streamMeta.getLocality());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1)
  {
    @SuppressWarnings("rawtypes")
    InputPort[] ports = new Operator.InputPort[]{sink1};
    return addStream(id, source, ports);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1, Operator.InputPort<? super T> sink2)
  {
    @SuppressWarnings("rawtypes")
    InputPort[] ports = new Operator.InputPort[] {sink1, sink2};
    return addStream(id, source, ports);
  }

  public StreamMeta getStream(String id)
  {
    return this.streams.get(id);
  }

  /**
   * Set attribute for the operator. For valid attributes, see {
   *
   * @param operator
   * @return AttributeMap
   */
  public Attribute.AttributeMap getContextAttributes(Operator operator)
  {
    return getMeta(operator).attributes;
  }

  @Override
  public <T> void setAttribute(Attribute<T> key, T value)
  {
    this.getAttributes().put(key, value);
  }

  @Override
  public <T> void setAttribute(Operator operator, Attribute<T> key, T value)
  {
    setOperatorAttribute(operator, key, value);
  }

  @Override
  public <T> void setOperatorAttribute(Operator operator, Attribute<T> key, T value)
  {
    this.getMeta(operator).attributes.put(key, value);
  }

  private OutputPortMeta assertGetPortMeta(Operator.OutputPort<?> port)
  {
    for (OperatorMeta o : getAllOperators()) {
      OutputPortMeta opm = o.getPortMapping().outPortMap.get(port);
      if (opm != null) {
        return opm;
      }
    }
    throw new IllegalArgumentException("Port is not associated to any operator in the DAG: " + port);
  }

  private InputPortMeta assertGetPortMeta(Operator.InputPort<?> port)
  {
    for (OperatorMeta o : getAllOperators()) {
      InputPortMeta opm = o.getPortMapping().inPortMap.get(port);
      if (opm != null) {
        return opm;
      }
    }
    throw new IllegalArgumentException("Port is not associated to any operator in the DAG: " + port);
  }

  @Override
  public <T> void setOutputPortAttribute(Operator.OutputPort<?> port, Attribute<T> key, T value)
  {
    assertGetPortMeta(port).attributes.put(key, value);
  }

  @Override
  public <T> void setUnifierAttribute(Operator.OutputPort<?> port, Attribute<T> key, T value)
  {
    assertGetPortMeta(port).getUnifierMeta().attributes.put(key, value);
  }

  @Override
  public <T> void setInputPortAttribute(Operator.InputPort<?> port, Attribute<T> key, T value)
  {
    assertGetPortMeta(port).attributes.put(key, value);
  }

  public List<OperatorMeta> getRootOperators()
  {
    return Collections.unmodifiableList(this.rootOperators);
  }

  @Override
  public List<OperatorMeta> getRootOperatorsMeta()
  {
    return getRootOperators();
  }

  public List<OperatorMeta> getLeafOperators()
  {
    return Collections.unmodifiableList(this.leafOperators);
  }

  public Collection<OperatorMeta> getAllOperators()
  {
    return Collections.unmodifiableCollection(this.operators.values());
  }

  @Override
  public Collection<OperatorMeta> getAllOperatorsMeta()
  {
    return getAllOperators();
  }

  public Collection<ModuleMeta> getAllModules()
  {
    return Collections.unmodifiableCollection(this.modules.values());
  }

  public Collection<StreamMeta> getAllStreams()
  {
    return Collections.unmodifiableCollection(this.streams.values());
  }

  @Override
  public Collection<StreamMeta> getAllStreamsMeta()
  {
    return getAllStreams();
  }

  @Override
  public OperatorMeta getOperatorMeta(String operatorName)
  {
    return this.operators.get(operatorName);
  }

  @Override
  public OperatorMeta getMeta(Operator operator)
  {
    // TODO: cache mapping
    for (OperatorMeta o: getAllOperators()) {
      if (o.operator == operator) {
        return o;
      }
    }
    throw new IllegalArgumentException("Operator not associated with the DAG: " + operator);
  }

  public int getMaxContainerCount()
  {
    return this.getValue(CONTAINERS_MAX_COUNT);
  }

  public boolean isDebug()
  {
    return this.getValue(DEBUG);
  }

  public int getMasterMemoryMB()
  {
    return this.getValue(MASTER_MEMORY_MB);
  }

  public String getMasterJVMOptions()
  {
    return this.getValue(CONTAINER_JVM_OPTIONS);
  }

  public String assertAppPath()
  {
    String path = getAttributes().get(LogicalPlan.APPLICATION_PATH);
    if (path == null) {
      throw new AssertionError("Missing " + LogicalPlan.APPLICATION_PATH);
    }
    return path;
  }

  /**
   * Class dependencies for the topology. Used to determine jar file dependencies.
   *
   * @return Set<String>
   */
  public Set<String> getClassNames()
  {
    Set<String> classNames = new HashSet<>();
    for (OperatorMeta n: this.operators.values()) {
      String className = n.getOperator().getClass().getName();
      if (className != null) {
        classNames.add(className);
      }
    }
    for (StreamMeta n: this.streams.values()) {
      for (InputPortMeta sink : n.getSinks()) {
        StreamCodec<?> streamCodec = sink.getStreamCodec();
        if (streamCodec != null) {
          classNames.add(streamCodec.getClass().getName());
        }
      }
    }
    return classNames;
  }

  public static class ValidationContext
  {
    public int nodeIndex = 0;
    public Stack<OperatorMeta> stack = new Stack<>();
    public Stack<OperatorMeta> path = new Stack<>();
    public List<Set<OperatorMeta>> stronglyConnected = new ArrayList<>();
    public OperatorMeta invalidLoopAt;
    public List<Set<OperatorMeta>> invalidCycles = new ArrayList<>();
  }

  public void resetNIndex()
  {
    for (OperatorMeta om : getAllOperators()) {
      om.lowlink = null;
      om.nindex = null;
    }
  }

  /**
   * Validate the plan. Includes checks that required ports are connected,
   * required configuration parameters specified, graph free of cycles etc.
   *
   * @throws ConstraintViolationException
   */
  public void validate() throws ConstraintViolationException
  {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();

    checkAttributeValueSerializable(this.getAttributes(), DAG.class.getName());

    // clear oioRoot values in all operators
    for (OperatorMeta n : operators.values()) {
      n.oioRoot = null;
    }

    // clear visited on all operators
    for (OperatorMeta n : operators.values()) {
      n.nindex = null;
      n.lowlink = null;

      // validate configuration
      Set<ConstraintViolation<Operator>> constraintViolations = validator.validate(n.getOperator());
      if (!constraintViolations.isEmpty()) {
        Set<ConstraintViolation<?>> copySet = new HashSet<>(constraintViolations.size());
        // workaround bug in ConstraintViolationException constructor
        // (should be public <T> ConstraintViolationException(String message, Set<ConstraintViolation<T>> constraintViolations) { ... })
        for (ConstraintViolation<Operator> cv: constraintViolations) {
          copySet.add(cv);
        }
        throw new ConstraintViolationException("Operator " + n.getName() + " violates constraints " + copySet, copySet);
      }

      OperatorMeta.PortMapping portMapping = n.getPortMapping();

      checkAttributeValueSerializable(n.getAttributes(), n.getName());

      // Check operator annotation
      if (n.operatorAnnotation != null) {
        // Check if partition property of the operator is being honored
        if (!n.operatorAnnotation.partitionable()) {
          // Check if any of the input ports have partition attributes set
          for (InputPortMeta pm: portMapping.inPortMap.values()) {
            Boolean paralellPartition = pm.getValue(PortContext.PARTITION_PARALLEL);
            if (paralellPartition) {
              throw new ValidationException("Operator " + n.getName() + " is not partitionable but PARTITION_PARALLEL attribute is set");
            }
          }

          // Check if the operator implements Partitioner
          if (n.getValue(OperatorContext.PARTITIONER) != null
              || n.attributes != null && !n.attributes.contains(OperatorContext.PARTITIONER) && Partitioner.class.isAssignableFrom(n.getOperator().getClass())) {
            throw new ValidationException("Operator " + n.getName() + " provides partitioning capabilities but the annotation on the operator class declares it non partitionable!");
          }
        }

        //If operator can not be check-pointed in middle of application window then the checkpoint window count should be
        // a multiple of application window count
        if (!n.operatorAnnotation.checkpointableWithinAppWindow()) {
          if (n.getValue(OperatorContext.CHECKPOINT_WINDOW_COUNT) % n.getValue(OperatorContext.APPLICATION_WINDOW_COUNT) != 0) {
            throw new ValidationException("Operator " + n.getName() + " cannot be check-pointed between an application window " +
              "but the checkpoint-window-count " + n.getValue(OperatorContext.CHECKPOINT_WINDOW_COUNT) +
              " is not a multiple application-window-count " + n.getValue(OperatorContext.APPLICATION_WINDOW_COUNT));
          }
        }
      }

      // check that non-optional ports are connected
      for (InputPortMeta pm: portMapping.inPortMap.values()) {
        checkAttributeValueSerializable(pm.getAttributes(), n.getName() + "." + pm.getPortName());
        StreamMeta sm = n.inputStreams.get(pm);
        if (sm == null) {
          if ((pm.portAnnotation == null || !pm.portAnnotation.optional()) && pm.classDeclaringHiddenPort == null) {
            throw new ValidationException("Input port connection required: " + n.name + "." + pm.getPortName());
          }
        } else {
          if (pm.classDeclaringHiddenPort != null) {
            throw new ValidationException(String.format("Invalid port connected: %s.%s is hidden by %s.%s", pm.classDeclaringHiddenPort.getName(),
              pm.getPortName(), pm.operatorMeta.getOperator().getClass().getName(), pm.getPortName()));
          }
          // check locality constraints
          DAG.Locality locality = sm.getLocality();
          if (locality == DAG.Locality.THREAD_LOCAL) {
            if (n.inputStreams.size() > 1) {
              validateThreadLocal(n);
            }
          }

          if (pm.portAnnotation != null && pm.portAnnotation.schemaRequired()) {
            //since schema is required, the port attribute TUPLE_CLASS should be present
            if (pm.attributes.get(PortContext.TUPLE_CLASS) == null) {
              throw new ValidationException("Attribute " + PortContext.TUPLE_CLASS.getName() + " missing on port : " + n.name + "." + pm.getPortName());
            }
          }
        }
      }

      for (OutputPortMeta pm: portMapping.outPortMap.values()) {
        checkAttributeValueSerializable(pm.getAttributes(), n.getName() + "." + pm.getPortName());
        if (!n.outputStreams.containsKey(pm)) {
          if ((pm.portAnnotation != null && !pm.portAnnotation.optional()) && pm.classDeclaringHiddenPort == null) {
            throw new ValidationException("Output port connection required: " + n.name + "." + pm.getPortName());
          }
        } else {
          //port is connected
          if (pm.classDeclaringHiddenPort != null) {
            throw new ValidationException(String.format("Invalid port connected: %s.%s is hidden by %s.%s", pm.classDeclaringHiddenPort.getName(),
              pm.getPortName(), pm.operatorMeta.getOperator().getClass().getName(), pm.getPortName()));
          }
          if (pm.portAnnotation != null && pm.portAnnotation.schemaRequired()) {
            //since schema is required, the port attribute TUPLE_CLASS should be present
            if (pm.attributes.get(PortContext.TUPLE_CLASS) == null) {
              throw new ValidationException("Attribute " + PortContext.TUPLE_CLASS.getName() + " missing on port : " + n.name + "." + pm.getPortName());
            }
          }
        }
      }
    }

    ValidationContext validatonContext = new ValidationContext();
    for (OperatorMeta n: operators.values()) {
      if (n.nindex == null) {
        findStronglyConnected(n, validatonContext);
      }
    }
    if (!validatonContext.invalidCycles.isEmpty()) {
      throw new ValidationException("Loops in graph: " + validatonContext.invalidCycles);
    }

    List<List<String>> invalidDelays = new ArrayList<>();
    for (OperatorMeta n : rootOperators) {
      findInvalidDelays(n, invalidDelays, new Stack<OperatorMeta>());
    }
    if (!invalidDelays.isEmpty()) {
      throw new ValidationException("Invalid delays in graph: " + invalidDelays);
    }

    for (StreamMeta s: streams.values()) {
      if (s.source == null) {
        throw new ValidationException("Stream source not connected: " + s.getName());
      }

      if (s.sinks.isEmpty()) {
        throw new ValidationException("Stream sink not connected: " + s.getName());
      }
    }

    // Validate root operators are input operators
    for (OperatorMeta om : this.rootOperators) {
      if (!(om.getOperator() instanceof InputOperator)) {
        throw new ValidationException(String.format("Root operator: %s is not a Input operator",
            om.getName()));
      }
    }

    // processing mode
    Set<OperatorMeta> visited = Sets.newHashSet();
    for (OperatorMeta om : this.rootOperators) {
      validateProcessingMode(om, visited);
    }

    validateAffinityRules();
  }

  /**
   * Pair of operator names to specify affinity rule
   * The order of operators is not considered in this class
   * i.e. OperatorPair("O1", "O2") is equal to OperatorPair("O2", "O1")
   */
  public static class OperatorPair extends Pair<String, String>
  {
    private static final long serialVersionUID = 4636942499106381268L;

    public OperatorPair(String first, String second)
    {
      super(first, second);
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj instanceof OperatorPair) {
        OperatorPair pairObj = (OperatorPair)obj;
        // The pair objects are equal if same 2 operators are present in both pairs
        // Order does not matter
        return ((this.first.equals(pairObj.first)) && (this.second.equals(pairObj.second)))
            || (this.first.equals(pairObj.second) && this.second.equals(pairObj.first));
      }
      return super.equals(obj);
    }
  }

  /**
   * validation for affinity rules validates following:
   *  1. The operator names specified in affinity rule are part of the dag
   *  2. Affinity rules do not conflict with anti-affinity rules directly or indirectly
   *  3. Anti-affinity rules do not conflict with Stream Locality
   *  4. Anti-affinity rules do not conflict with host-locality attribute
   *  5. Affinity rule between non stream operators does not have Thread_Local locality
   *  6. Affinity rules do not conflict with host-locality attribute
   */
  private void validateAffinityRules()
  {
    AffinityRulesSet affinityRuleSet = getAttributes().get(DAGContext.AFFINITY_RULES_SET);
    if (affinityRuleSet == null || affinityRuleSet.getAffinityRules() == null) {
      return;
    }

    Collection<AffinityRule> affinityRules = affinityRuleSet.getAffinityRules();

    HashMap<String, Set<String>> containerAffinities = new HashMap<>();
    HashMap<String, Set<String>> nodeAffinities = new HashMap<>();
    HashMap<String, String> hostNamesMapping = new HashMap<>();

    HashMap<OperatorPair, AffinityRule> affinities = new HashMap<>();
    HashMap<OperatorPair, AffinityRule> antiAffinities = new HashMap<>();
    HashMap<OperatorPair, AffinityRule> threadLocalAffinities = new HashMap<>();

    List<String> operatorNames = new ArrayList<>();

    for (OperatorMeta operator : getAllOperators()) {
      operatorNames.add(operator.getName());
      Set<String> containerSet = new HashSet<>();
      containerSet.add(operator.getName());
      containerAffinities.put(operator.getName(), containerSet);
      Set<String> nodeSet = new HashSet<>();
      nodeSet.add(operator.getName());
      nodeAffinities.put(operator.getName(), nodeSet);

      if (operator.getAttributes().get(OperatorContext.LOCALITY_HOST) != null) {
        hostNamesMapping.put(operator.getName(), operator.getAttributes().get(OperatorContext.LOCALITY_HOST));
      }
    }

    // Identify operators set as Regex and add to list
    for (AffinityRule rule : affinityRules) {
      if (rule.getOperatorRegex() != null) {
        convertRegexToList(operatorNames, rule);
      }
    }
    // Convert operators with list of operator to rules with operator pairs for validation
    for (AffinityRule rule : affinityRules) {
      if (rule.getOperatorsList() != null) {
        List<String> list = rule.getOperatorsList();
        for (int i = 0; i < list.size(); i++) {
          for (int j = i + 1; j < list.size(); j++) {
            OperatorPair pair = new OperatorPair(list.get(i), list.get(j));
            if (rule.getType() == com.datatorrent.api.AffinityRule.Type.AFFINITY) {
              addToMap(affinities, rule, pair);
            } else {
              addToMap(antiAffinities, rule, pair);
            }
          }
        }
      }
    }

    for (Entry<OperatorPair, AffinityRule> ruleEntry : affinities.entrySet()) {
      OperatorPair pair = ruleEntry.getKey();
      AffinityRule rule = ruleEntry.getValue();
      if (hostNamesMapping.containsKey(pair.first) && hostNamesMapping.containsKey(pair.second) && !hostNamesMapping.get(pair.first).equals(hostNamesMapping.get(pair.second))) {
        throw new ValidationException(String.format("Host Locality for operators: %s(host: %s) & %s(host: %s) conflicts with affinity rules", pair.first, hostNamesMapping.get(pair.first), pair.second, hostNamesMapping.get(pair.second)));
      }
      if (rule.getLocality() == Locality.THREAD_LOCAL) {
        addToMap(threadLocalAffinities, rule, pair);
      } else if (rule.getLocality() == Locality.CONTAINER_LOCAL) {
        // Combine the sets
        combineSets(containerAffinities, pair);
        // Also update node list
        combineSets(nodeAffinities, pair);
      } else if (rule.getLocality() == Locality.NODE_LOCAL) {
        combineSets(nodeAffinities, pair);
      }
    }


    for (StreamMeta stream : getAllStreams()) {
      String source = stream.source.getOperatorMeta().getName();
      for (InputPortMeta sink : stream.sinks) {
        String sinkOperator = sink.getOperatorMeta().getName();
        OperatorPair pair = new OperatorPair(source, sinkOperator);
        if (stream.getLocality() != null && stream.getLocality().ordinal() <= Locality.NODE_LOCAL.ordinal() && hostNamesMapping.containsKey(pair.first) && hostNamesMapping.containsKey(pair.second) && !hostNamesMapping.get(pair.first).equals(hostNamesMapping.get(pair.second))) {
          throw new ValidationException(String.format("Host Locality for operators: %s(host: %s) & %s(host: %s) conflicts with stream locality", pair.first, hostNamesMapping.get(pair.first), pair.second, hostNamesMapping.get(pair.second)));
        }
        if (stream.locality == Locality.CONTAINER_LOCAL) {
          combineSets(containerAffinities, pair);
          combineSets(nodeAffinities, pair);
        } else if (stream.locality == Locality.NODE_LOCAL) {
          combineSets(nodeAffinities, pair);
        }
        if (affinities.containsKey(pair)) {
          // Choose the lower bound on locality
          AffinityRule rule = affinities.get(pair);
          if (rule.getLocality() == Locality.THREAD_LOCAL) {
            stream.setLocality(rule.getLocality());
            threadLocalAffinities.remove(rule);
            affinityRules.remove(rule);
          }
          if (stream.locality != null && rule.getLocality().ordinal() > stream.getLocality().ordinal()) {
            // Remove the affinity rule from attributes, as it is redundant
            affinityRules.remove(rule);
          }
        }
      }
    }

    // Validate that all Thread local affinities were for stream connected operators
    if (!threadLocalAffinities.isEmpty()) {
      OperatorPair pair = threadLocalAffinities.keySet().iterator().next();
      throw new ValidationException(String.format("Affinity rule specified THREAD_LOCAL affinity for operators %s & %s which are not connected by stream", pair.first, pair.second));
    }

    for (Entry<OperatorPair, AffinityRule> ruleEntry : antiAffinities.entrySet()) {
      OperatorPair pair = ruleEntry.getKey();
      AffinityRule rule = ruleEntry.getValue();

      if (pair.first.equals(pair.second)) {
        continue;
      }
      if (rule.getLocality() == Locality.CONTAINER_LOCAL) {
        if (containerAffinities.get(pair.first).contains(pair.second)) {
          throw new ValidationException(String.format("Anti Affinity rule for operators %s & %s conflicts with affinity rules or Stream locality", pair.first, pair.second));

        }
      } else if (rule.getLocality() == Locality.NODE_LOCAL) {
        if (nodeAffinities.get(pair.first).contains(pair.second)) {
          throw new ValidationException(String.format("Anti Affinity rule for operators %s & %s conflicts with affinity rules or Stream locality", pair.first, pair.second));
        }
        // Check host locality for both operators
        // Check host attribute for all operators in node local set for both
        // anti-affinity operators
        String firstOperatorLocality = getHostLocality(nodeAffinities, pair.first, hostNamesMapping);
        String secondOperatorLocality = getHostLocality(nodeAffinities, pair.second, hostNamesMapping);
        if (firstOperatorLocality != null && secondOperatorLocality != null && firstOperatorLocality == secondOperatorLocality) {
          throw new ValidationException(String.format("Host Locality for operators: %s(host: %s) & %s(host: %s) conflict with anti-affinity rules", pair.first, firstOperatorLocality, pair.second, secondOperatorLocality));
        }
      }
    }
  }

  /**
   * Get host mapping for an operator using affinity settings and host locality specified for operator
   * @param nodeAffinities
   * @param operator
   * @param hostNamesMapping
   * @return
   */
  public String getHostLocality(HashMap<String, Set<String>> nodeAffinities, String operator, HashMap<String, String> hostNamesMapping)
  {
    if (hostNamesMapping.containsKey(operator)) {
      return hostNamesMapping.get(operator);
    }

    for (String op : nodeAffinities.get(operator)) {
      if (hostNamesMapping.containsKey(op)) {
        return hostNamesMapping.get(op);
      }
    }

    return null;
  }

  /**
   * Combine affinity sets for operators with affinity
   * @param containerAffinities
   * @param pair
   */
  public void combineSets(HashMap<String, Set<String>> containerAffinities, OperatorPair pair)
  {
    Set<String> set1 = containerAffinities.get(pair.first);
    Set<String> set2 = containerAffinities.get(pair.second);
    set1.addAll(set2);
    containerAffinities.put(pair.first, set1);
    containerAffinities.put(pair.second, set1);
  }

  /**
   * Convert regex in Affinity Rule to list of operators
   * Regex should match at least 2 operators, otherwise rule is not applied
   * @param operatorNames
   * @param rule
   */
  public void convertRegexToList(List<String> operatorNames, AffinityRule rule)
  {
    List<String> operators = new LinkedList<>();
    Pattern p = Pattern.compile(rule.getOperatorRegex());
    for (String name : operatorNames) {
      if (p.matcher(name).matches()) {
        operators.add(name);
      }
    }
    rule.setOperatorRegex(null);
    if (operators.size() <= 1) {
      LOG.warn("Regex should match at least 2 operators to add affinity rule. Ignoring rule");
    } else {
      rule.setOperatorsList(operators);
    }
  }

  /**
   * Validates that operators in Affinity Rule are valid: Checks that operator names are part of the dag and adds them to map of rules
   * @param affinitiesMap
   * @param rule
   * @param operators
   */
  private void addToMap(HashMap<OperatorPair, AffinityRule> affinitiesMap, AffinityRule rule, OperatorPair operators)
  {
    OperatorMeta operator1 = getOperatorMeta(operators.first);
    OperatorMeta operator2 = getOperatorMeta(operators.second);
    if (operator1 == null || operator2 == null) {
      if (operator1 == null && operator2 == null) {
        throw new ValidationException(String.format("Operators %s & %s specified in affinity rule are not part of the dag", operators.first, operators.second));
      }
      throw new ValidationException(String.format("Operator %s specified in affinity rule is not part of the dag", operator1 == null ? operators.first : operators.second));
    }
    affinitiesMap.put(operators, rule);
  }

  private void checkAttributeValueSerializable(AttributeMap attributes, String context)
  {
    StringBuilder sb = new StringBuilder();
    String delim = "";
    // Check all attributes got operator are serializable
    for (Entry<Attribute<?>, Object> entry : attributes.entrySet()) {
      if (entry.getValue() != null && !(entry.getValue() instanceof Serializable)) {
        sb.append(delim).append(entry.getKey().getSimpleName());
        delim = ", ";
      }
    }
    if (sb.length() > 0) {
      throw new ValidationException("Attribute value(s) for " + sb.toString() + " in " + context + " are not serializable");
    }
  }

  /*
   * Validates OIO constraints for operators with more than one input streams
   * For a node to be OIO,
   *  1. all its input streams should be OIO
   *  2. all its input streams should have OIO from single source node
   */
  private void validateThreadLocal(OperatorMeta om)
  {
    Integer oioRoot = null;

    // already visited and validated
    if (om.oioRoot != null) {
      return;
    }

    if (om.getOperator() instanceof Operator.DelayOperator) {
      String msg = String.format("Locality %s invalid for delay operator %s", Locality.THREAD_LOCAL, om);
      throw new ValidationException(msg);
    }

    for (StreamMeta sm: om.inputStreams.values()) {
      // validation fail as each input stream should be OIO
      if (sm.locality != Locality.THREAD_LOCAL) {
        String msg = String.format("Locality %s invalid for operator %s with multiple input streams as at least one of the input streams is not %s",
            Locality.THREAD_LOCAL, om, Locality.THREAD_LOCAL);
        throw new ValidationException(msg);
      }

      if (sm.source.operatorMeta.getOperator() instanceof Operator.DelayOperator) {
        String msg = String.format("Locality %s invalid for delay operator %s", Locality.THREAD_LOCAL, sm.source.operatorMeta);
        throw new ValidationException(msg);
      }
      // gets oio root for input operator for the stream
      Integer oioStreamRoot = getOioRoot(sm.source.operatorMeta);

      // validation fail as each input stream should have a common OIO root
      if (om.oioRoot != null && oioStreamRoot != om.oioRoot) {
        String msg = String.format("Locality %s invalid for operator %s with multiple input streams as at least one of the input streams is not originating from common OIO owner node",
            Locality.THREAD_LOCAL, om, Locality.THREAD_LOCAL);
        throw new ValidationException(msg);
      }

      // populate oioRoot with root OIO node id for first stream, then validate for subsequent streams to have same root OIO node
      if (oioRoot == null) {
        oioRoot = oioStreamRoot;
      } else if (oioRoot.intValue() != oioStreamRoot.intValue()) {
        String msg = String.format("Locality %s invalid for operator %s with multiple input streams as they origin from different owner OIO operators", sm.locality, om);
        throw new ValidationException(msg);
      }
    }

    om.oioRoot = oioRoot;
  }

  /**
   * Helper method for validateThreadLocal method, runs recursively
   * For a given operator, visits all upstream operators in DFS, validates and marks them as visited
   * returns hashcode of owner oio node if it exists, else returns hashcode of the supplied node
   */
  private Integer getOioRoot(OperatorMeta om)
  {
    // operators which were already marked a visited
    if (om.oioRoot != null) {
      return om.oioRoot;
    }

    // operators which were not visited before
    switch (om.inputStreams.size()) {
      case 1:
        StreamMeta sm = om.inputStreams.values().iterator().next();
        if (sm.locality == Locality.THREAD_LOCAL) {
          om.oioRoot = getOioRoot(sm.source.operatorMeta);
        } else {
          om.oioRoot = om.hashCode();
        }
        break;
      case 0:
        om.oioRoot = om.hashCode();
        break;
      default:
        validateThreadLocal(om);
    }

    return om.oioRoot;
  }

  /**
   * Check for cycles in the graph reachable from start node n. This is done by
   * attempting to find strongly connected components.
   *
   * @see <a href="http://en.wikipedia.org/wiki/Tarjan%E2%80%99s_strongly_connected_components_algorithm">http://en.wikipedia.org/wiki/Tarjan%E2%80%99s_strongly_connected_components_algorithm</a>
   *
   * @param om
   * @param ctx
   */
  public void findStronglyConnected(OperatorMeta om, ValidationContext ctx)
  {
    om.nindex = ctx.nodeIndex;
    om.lowlink = ctx.nodeIndex;
    ctx.nodeIndex++;
    ctx.stack.push(om);
    ctx.path.push(om);

    // depth first successors traversal
    for (StreamMeta downStream: om.outputStreams.values()) {
      for (InputPortMeta sink: downStream.sinks) {
        OperatorMeta successor = sink.getOperatorMeta();
        if (successor == null) {
          continue;
        }
        // check for self referencing node
        if (om == successor) {
          ctx.invalidCycles.add(Collections.singleton(om));
        }
        if (successor.nindex == null) {
          // not visited yet
          findStronglyConnected(successor, ctx);
          om.lowlink = Math.min(om.lowlink, successor.lowlink);
        } else if (ctx.stack.contains(successor)) {
          om.lowlink = Math.min(om.lowlink, successor.nindex);
          boolean isDelayLoop = false;
          for (int i = ctx.stack.size(); i > 0; i--) {
            OperatorMeta om2 = ctx.stack.get(i - 1);
            if (om2.getOperator() instanceof Operator.DelayOperator) {
              isDelayLoop = true;
            }
            if (om2 == successor) {
              break;
            }
          }
          if (!isDelayLoop) {
            ctx.invalidLoopAt = successor;
          }
        }
      }
    }

    // pop stack for all root operators
    if (om.lowlink.equals(om.nindex)) {
      Set<OperatorMeta> connectedSet = new LinkedHashSet<>(ctx.stack.size());
      while (!ctx.stack.isEmpty()) {
        OperatorMeta n2 = ctx.stack.pop();
        connectedSet.add(n2);
        if (n2 == om) {
          break; // collected all connected operators
        }
      }
      // strongly connected (cycle) if more than one node in stack
      if (connectedSet.size() > 1) {
        ctx.stronglyConnected.add(connectedSet);
        if (connectedSet.contains(ctx.invalidLoopAt)) {
          ctx.invalidCycles.add(connectedSet);
        }
      }
    }
    ctx.path.pop();

  }

  public void findInvalidDelays(OperatorMeta om, List<List<String>> invalidDelays, Stack<OperatorMeta> stack)
  {
    stack.push(om);

    // depth first successors traversal
    boolean isDelayOperator = om.getOperator() instanceof Operator.DelayOperator;
    if (isDelayOperator) {
      if (om.getValue(OperatorContext.APPLICATION_WINDOW_COUNT) != 1) {
        LOG.debug("detected DelayOperator having APPLICATION_WINDOW_COUNT not equal to 1");
        invalidDelays.add(Collections.singletonList(om.getName()));
      }
    }

    for (StreamMeta downStream: om.outputStreams.values()) {
      for (InputPortMeta sink : downStream.sinks) {
        OperatorMeta successor = sink.getOperatorMeta();
        if (isDelayOperator) {
          sink.attributes.put(IS_CONNECTED_TO_DELAY_OPERATOR, true);
          // Check whether all downstream operators are already visited in the path
          if (successor != null && !stack.contains(successor)) {
            LOG.debug("detected DelayOperator does not immediately output to a visited operator {}.{}->{}.{}",
                om.getName(), downStream.getSource().getPortName(), successor.getName(), sink.getPortName());
            invalidDelays.add(Arrays.asList(om.getName(), successor.getName()));
          }
        } else {
          findInvalidDelays(successor, invalidDelays, stack);
        }
      }
    }
    stack.pop();
  }

  private void validateProcessingMode(OperatorMeta om, Set<OperatorMeta> visited)
  {
    for (StreamMeta is : om.getInputStreams().values()) {
      if (!visited.contains(is.getSource().getOperatorMeta())) {
        // process all inputs first
        return;
      }
    }
    visited.add(om);
    Operator.ProcessingMode pm = om.getValue(OperatorContext.PROCESSING_MODE);
    for (StreamMeta os : om.outputStreams.values()) {
      for (InputPortMeta sink: os.sinks) {
        OperatorMeta sinkOm = sink.getOperatorMeta();
        Operator.ProcessingMode sinkPm = sinkOm.attributes == null ? null : sinkOm.attributes.get(OperatorContext.PROCESSING_MODE);
        if (sinkPm == null) {
          // If the source processing mode is AT_MOST_ONCE and a processing mode is not specified for the sink then
          // set it to AT_MOST_ONCE as well
          if (Operator.ProcessingMode.AT_MOST_ONCE.equals(pm)) {
            LOG.warn("Setting processing mode for operator {} to {}", sinkOm.getName(), pm);
            sinkOm.getAttributes().put(OperatorContext.PROCESSING_MODE, pm);
          } else if (Operator.ProcessingMode.EXACTLY_ONCE.equals(pm)) {
            // If the source processing mode is EXACTLY_ONCE and a processing mode is not specified for the sink then
            // throw a validation error
            String msg = String.format("Processing mode for %s should be AT_MOST_ONCE for source %s/%s", sinkOm.getName(), om.getName(), pm);
            throw new ValidationException(msg);
          }
        } else {
          /*
           * If the source processing mode is AT_MOST_ONCE and the processing mode for the sink is not AT_MOST_ONCE throw a validation error
           * If the source processing mode is EXACTLY_ONCE and the processing mode for the sink is not AT_MOST_ONCE throw a validation error
           */
          if ((Operator.ProcessingMode.AT_MOST_ONCE.equals(pm) && (sinkPm != pm))
              || (Operator.ProcessingMode.EXACTLY_ONCE.equals(pm) && !Operator.ProcessingMode.AT_MOST_ONCE.equals(sinkPm))) {
            String msg = String.format("Processing mode %s/%s not valid for source %s/%s", sinkOm.getName(), sinkPm, om.getName(), pm);
            throw new ValidationException(msg);
          }
        }
        validateProcessingMode(sinkOm, visited);
      }
    }
  }

  public static void write(DAG dag, OutputStream os) throws IOException
  {
    ObjectOutputStream oos = new ObjectOutputStream(os);
    oos.writeObject(dag);
  }

  public static LogicalPlan read(InputStream is) throws IOException, ClassNotFoundException
  {
    return (LogicalPlan)new ClassLoaderObjectInputStream(Thread.currentThread().getContextClassLoader(), is).readObject();
  }


  public static Type getPortType(Field f)
  {
    if (f.getGenericType() instanceof ParameterizedType) {
      ParameterizedType t = (ParameterizedType)f.getGenericType();
      //LOG.debug("Field type is parameterized: " + Arrays.asList(t.getActualTypeArguments()));
      //LOG.debug("rawType: " + t.getRawType()); // the port class
      Type typeArgument = t.getActualTypeArguments()[0];
      if (typeArgument instanceof Class) {
        return typeArgument;
      } else if (typeArgument instanceof TypeVariable) {
        TypeVariable<?> tv = (TypeVariable<?>)typeArgument;
        LOG.debug("bounds: {}", Arrays.asList(tv.getBounds()));
        // variable may contain other variables, java.util.Map<java.lang.String, ? extends T2>
        return tv.getBounds()[0];
      } else if (typeArgument instanceof GenericArrayType) {
        LOG.debug("type {} is of GenericArrayType", typeArgument);
        return typeArgument;
      } else if (typeArgument instanceof WildcardType) {
        LOG.debug("type {} is of WildcardType", typeArgument);
        return typeArgument;
      } else if (typeArgument instanceof ParameterizedType) {
        return typeArgument;
      } else {
        LOG.error("Type argument is of expected type {}", typeArgument);
        return null;
      }
    } else {
      // ports are always parameterized
      LOG.error("No type variable: {}, typeParameters: {}", f.getType(), Arrays.asList(f.getClass().getTypeParameters()));
      return null;
    }
  }

  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("operators", this.operators)
        .append("streams", this.streams)
        .append("properties", this.attributes)
        .toString();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LogicalPlan)) {
      return false;
    }

    LogicalPlan that = (LogicalPlan)o;

    if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) {
      return false;
    }
    return !(streams != null ? !streams.equals(that.streams) : that.streams != null);
  }

  @Override
  public int hashCode()
  {
    int result = streams != null ? streams.hashCode() : 0;
    result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
    return result;
  }

}
