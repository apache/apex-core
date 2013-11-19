/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.api;

import com.datatorrent.api.AttributeMap.Attribute;
import java.io.Serializable;

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
  public interface InputPortMeta extends Serializable, PortContext
  {
  }

  public interface OutputPortMeta extends Serializable, PortContext
  {
  }

  /**
   * Locality setting affects how operators are scheduled for deployment by
   * the platform. The setting serves as hint to the planner and can yield
   * significant performance gains. Optimizations are subject to resource
   * availability.
   */
  public enum Locality {
    /**
     * Adjacent operators should be deployed into the same executing thread,
     * effectively serializing the computation. This setting is beneficial
     * where the cost of intermediate queuing exceeds the benefit of parallel
     * processing. An example could be chaining of multiple operators with low
     * compute requirements in a parallel partition setup.
     * Not implemented yet.
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
  public interface StreamMeta extends Serializable
  {
    public String getId();

    /**
     * Returns the locality for this stream.
     * @return locality for this stream, default is null.
     */
    public Locality getLocality();

    /**
     * Set locality for the stream. The setting is best-effort, engine can
     * override due to other settings or constraints.
     *
     * @param locality
     */
    public StreamMeta setLocality(Locality locality);

    public StreamMeta setSource(Operator.OutputPort<?> port);

    public StreamMeta addSink(Operator.InputPort<?> port);

  }

  /**
   * Operator meta object.
   */
  public interface OperatorMeta extends Serializable, Context
  {
    public Operator getOperator();

    public InputPortMeta getMeta(Operator.InputPort<?> port);

    public OutputPortMeta getMeta(Operator.OutputPort<?> port);
  }

  /**
   * Add new instance of operator under given name to the DAG.
   * The operator class must have a default constructor.
   * If the class extends {@link BaseOperator}, the name is passed on to the instance.
   * Throws exception if the name is already linked to another operator instance.
   *
   * @param <T>
   * @param name
   * @param clazz
   * @return <T extends Operator> T
   */
  public abstract <T extends Operator> T addOperator(String name, Class<T> clazz);

  /**
   * <p>addOperator.</p>
   */
  public abstract <T extends Operator> T addOperator(String name, T operator);

  /**
   * <p>addStream.</p>
   */
  public abstract StreamMeta addStream(String id);

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
  public abstract <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T>... sinks);

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
  public abstract <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1);

  /**
   * <p>addStream.</p>
   */
  public abstract <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1, Operator.InputPort<? super T> sink2);

  /**
   * <p>setAttribute.</p>
   */
  public abstract <T> void setAttribute(Attribute<T> key, T value);

  /**
   * <p>setAttribute.</p>
   */
  public abstract <T> void setAttribute(Operator operator, Attribute<T> key, T value);

  /**
   * <p>setOutputPortAttribute.</p>
   */
  public abstract <T> void setOutputPortAttribute(Operator.OutputPort<?> port, Attribute<T> key, T value);

  /**
   * <p>setInputPortAttribute.</p>
   */
  public abstract <T> void setInputPortAttribute(Operator.InputPort<?> port, Attribute<T> key, T value);

  /**
   * <p>getOperatorMeta.</p>
   */
  public abstract OperatorMeta getOperatorMeta(String operatorId);

  /**
   * <p>getMeta.</p>
   */
  public abstract OperatorMeta getMeta(Operator operator);

}
