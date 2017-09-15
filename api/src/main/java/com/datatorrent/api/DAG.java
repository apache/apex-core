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
package com.datatorrent.api;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.annotation.OperatorAnnotation;

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
public interface DAG extends DAGContext, Serializable
{
  interface InputPortMeta extends Serializable, PortContext
  {
    /**
     * Return port object represented by this InputPortMeta
     * @return
     */
    Operator.InputPort<?> getPort();

    <T extends OperatorMeta> T getOperatorMeta();
  }

  interface OutputPortMeta extends Serializable, PortContext
  {
    OperatorMeta getUnifierMeta();

    /**
     * Return port object represented by this OutputPortMeta
     * @return
     */
    Operator.OutputPort<?> getPort();

    <T extends OperatorMeta> T getOperatorMeta();
  }

  /**
   * Locality setting affects how operators are scheduled for deployment by
   * the platform. The setting serves as hint to the planner and can yield
   * significant performance gains. Optimizations are subject to resource
   * availability.
   */
  enum Locality
  {
    /**
     * Adjacent operators should be deployed into the same executing thread,
     * effectively serializing the computation. This setting is beneficial
     * where the cost of intermediate queuing exceeds the benefit of parallel
     * processing. An example could be chaining of multiple operators with low
     * compute requirements in a parallel partition setup.
     */
    THREAD_LOCAL,
    /**
     * Adjacent operators should be deployed into the same process, executing
     * in different threads. Useful when interprocess communication is a
     * limiting factor and sufficient resources can be provisioned in a single
     * container. Eliminates data serialization and networking stack overhead.
     */
    CONTAINER_LOCAL,
    /**
     * Adjacent operators should be deployed into processes on the same machine.
     * Eliminates network as bottleneck, as the loop back interface can be used
     * instead.
     */
    NODE_LOCAL,
    /**
     * Adjacent operators should be deployed into processes on nodes in the same
     * rack. Best effort to not have allocation on same node.
     * Not implemented yet.
     */
    RACK_LOCAL
  }

  /**
   * Representation of streams in the logical layer. Instances are created through {@link DAG#addStream}.
   */
  interface StreamMeta extends Serializable
  {
    String getName();

    /**
     * Returns the locality for this stream.
     * @return locality for this stream, default is null.
     */
    Locality getLocality();

    /**
     * Set locality for the stream. The setting is best-effort, engine can
     * override due to other settings or constraints.
     *
     * @param locality
     * @return Object that describes the meta for the stream.
     */
    StreamMeta setLocality(Locality locality);

    StreamMeta setSource(Operator.OutputPort<?> port);

    StreamMeta addSink(Operator.InputPort<?> port);

    /**
     * Persist entire stream using operator passed.
     *
     * @param Persist Operator name
     * @param Operator to use for persisting
     * @param Input port to use for persisting
     * @return Object that describes the meta for the stream.
     */
    StreamMeta persistUsing(String name, Operator persistOperator, Operator.InputPort<?> persistOperatorInputPort);

    /**
     * Set locality for the stream. The setting is best-effort, engine can
     * override due to other settings or constraints.
     *
     * @param Persist Operator name
     * @param Operator to use for persisting
     * @return Object that describes the meta for the stream.
     */
    StreamMeta persistUsing(String name, Operator persistOperator);

    /**
     * Set locality for the stream. The setting is best-effort, engine can
     * override due to other settings or constraints.
     *
     * @param Persist Operator name
     * @param Operator to use for persisting
     * @param Input port to use for persisting
     * @param Sink to persist
     * @return Object that describes the meta for the stream.
     */
    StreamMeta persistUsing(String name, Operator persistOperator, Operator.InputPort<?> persistOperatorInputPort, Operator.InputPort<?> sinkToPersist);

    /**
     * Return source of the stream.
     * @param <T>
     * @return
     */
    <T extends OutputPortMeta> T getSource();

    /**
     * Return all sinks connected to this stream.
     * @param <T>
     * @return
     */
    <T extends InputPortMeta> Collection<T> getSinks();
  }

  /**
   * Operator meta object.
   */
  interface OperatorMeta extends Serializable, Context
  {
    String getName();

    Operator getOperator();

    InputPortMeta getMeta(Operator.InputPort<?> port);

    OutputPortMeta getMeta(Operator.OutputPort<?> port);

    OperatorAnnotation getOperatorAnnotation();

    /**
     * Return collection of stream which are connected to this operator's
     * input ports.
     * @param <T>
     * @return
     */
    <K extends InputPortMeta, V extends StreamMeta> Map<K, V> getInputStreams();

    /**
     * Return collection of stream which are connected to this operator's
     * output ports.
     * @param <T>
     * @return
     */
    <K extends OutputPortMeta, V extends StreamMeta> Map<K, V> getOutputStreams();
  }

  /**
   * Add new instance of operator under given name to the DAG.
   * The operator class must have a default constructor.
   * If the class extends {@link BaseOperator}, the name is passed on to the instance.
   * Throws exception if the name is already linked to another operator instance.
   *
   * @param <T> Concrete type of the operator
   * @param name Logical name of the operator used to identify the operator in the DAG
   * @param clazz Concrete class with default constructor so that instance of it can be initialized and added to the DAG.
   * @return Instance of the operator that has been added to the DAG.
   */
  <T extends Operator> T addOperator(@Nonnull String name, Class<T> clazz);

  /**
   * <p>addOperator.</p>
   * @param <T> Concrete type of the operator
   * @param name Logical name of the operator used to identify the operator in the DAG
   * @param operator Instance of the operator that needs to be added to the DAG
   * @return Instance of the operator that has been added to the DAG.
   */
  <T extends Operator> T addOperator(@Nonnull String name, T operator);

  @InterfaceStability.Evolving
  <T extends Module> T addModule(@Nonnull String name, Class<T> moduleClass);

  @InterfaceStability.Evolving
  <T extends Module> T addModule(@Nonnull String name, T module);

  /**
   * <p>addStream.</p>
   * @param id Identifier of the stream that will be used to identify stream in DAG
   * @return
   */
  StreamMeta addStream(@Nonnull String id);

  /**
   * Add identified stream for given source and sinks. Multiple sinks can be
   * connected to a stream, but each port can only be connected to a single
   * stream. Attempt to add stream to an already connected port will throw an
   * error.
   * <p>
   * This method allows to connect all interested ports to a stream at
   * once. Alternatively, use the returned {@link StreamMeta} builder object to
   * add more sinks and set other stream properties.
   *
   * @param <T>
   * @param id
   * @param source
   * @param sinks
   * @return StreamMeta
   */
  <T> StreamMeta addStream(@Nonnull String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T>... sinks);

  /**
   * Overload varargs version to avoid generic array type safety warnings in calling code.
   * "Type safety: A generic array of Operator.InputPort<> is created for a varargs parameter"
   *
   * @param <T>
   * @link <a href=http://www.angelikalanger.com/GenericsFAQ/FAQSections/ProgrammingIdioms.html#FAQ300>Programming Idioms</a>
   * @param id
   * @param source
   * @param sink1
   * @return StreamMeta
   */
  <T> StreamMeta addStream(@Nonnull String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1);

  /**
   * <p>addStream.</p>
   */
  <T> StreamMeta addStream(@Nonnull String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1, Operator.InputPort<? super T> sink2);

  /**
   * <p>setAttribute.</p>
   */
  <T> void setAttribute(Attribute<T> key, T value);

  /**
   * @Deprecated
   * Use {@link #setOperatorAttribute} instead
   */
  @Deprecated
  <T> void setAttribute(Operator operator, Attribute<T> key, T value);

  /**
   * Set an attribute for an operator.
   * @param <T> Value type of the attribute.
   * @param operator The Operator for which the attribute is being set.
   * @param key The attribute which needs to be tuned.
   * @param value The new value of the attribute.
   */
  <T> void setOperatorAttribute(Operator operator, Attribute<T> key, T value);

  /**
   * <p>setOutputPortAttribute.</p>
   */
  <T> void setOutputPortAttribute(Operator.OutputPort<?> port, Attribute<T> key, T value);

  /**
   * Set an attribute on the unifier for an output of an operator.
   * @param <T> Object type of the attribute
   * @param port The port for which the unifier is needed.
   * @param key The attribute which needs to be tuned.
   * @param value The new value of the attribute.
   */
  <T> void setUnifierAttribute(Operator.OutputPort<?> port, Attribute<T> key, T value);

  /**
   * <p>setInputPortAttribute.</p>
   */
  <T> void setInputPortAttribute(Operator.InputPort<?> port, Attribute<T> key, T value);

  /**
   * <p>getOperatorMeta.</p>
   */
  OperatorMeta getOperatorMeta(String operatorId);

  /**
   * <p>getMeta.</p>
   */
  OperatorMeta getMeta(Operator operator);

  /**
   * Return all operators present in the DAG.
   * @param <T>
   * @return
   */
  <T extends OperatorMeta> Collection<T> getAllOperatorsMeta();

  /**
   * Get all input operators in the DAG. This method returns operators which are
   * not connected to any upstream operator. i.e the operators which do not have
   * any input ports or operators which is not connected through any input ports
   * in the DAG.
   *
   * @param <T>
   * @return list of {@see OperatorMeta} for root operators in the DAG.
   */
  <T extends OperatorMeta> Collection<T> getRootOperatorsMeta();

  /**
   * Returns all Streams present in the DAG.
   * @param <T>
   * @return
   */
  <T extends StreamMeta> Collection<T> getAllStreamsMeta();

  /**
   * Marker interface for the Node in the DAG. Any object which can be added as a Node in the DAG
   * needs to implement this interface.
   */
  interface GenericOperator
  {

  }
}
