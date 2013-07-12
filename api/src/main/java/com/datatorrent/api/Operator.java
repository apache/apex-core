/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;

public interface Operator extends Component<OperatorContext>
{
  // define string counterparts to it for usage in properties file
  public enum ProcessingMode
  {
    AT_LEAST_ONCE,
    AT_MOST_ONCE,
    EXACTLY_ONCE
  }

  /**
   * This method gets called at the beginning of each window.
   *
   * @param windowId identifier for the window that is unique for this run of the application.
   */
  public void beginWindow(long windowId);

  /**
   * This method gets called at the end of each window.
   *
   */
  public void endWindow();

  /**
   * If the Operator can be partitioned, then Unifier is used to merge
   * the tuples from the output ports from all the partitioned instances.
   * Unifier are the operators which do not have any input ports defined
   * and exactly one output port defined which emits the tuple of the
   * type identical as the type emitted by the output port which is being
   * unified.
   *
   * @param <T> Type of the tuple emitted by the output port which is being unified
   */
  public interface Unifier<T> extends Operator
  {
    public void process(T tuple);

  }

  /**
   * A operator provides ports as a means to consume and produce data tuples.
   * Concrete ports implement derived interfaces.
   */
  public interface Port extends Component<PortContext>
  {
  }

  /**
   * Input ports process data delivered through a stream. The execution engine
   * will call the port's associated sink to pass the tuples. Ports are declared
   * as annotated fields in the operator. The interface should be implemented by a
   * non parameterized class to make the type parameter are available at runtime
   * for validation.
   *
   * @param <T>
   */
  public interface InputPort<T> extends Port
  {
    /**
     * Provide the sink that will process incoming data. Sink would typically be
     * the port itself but can also be implemented by the enclosing operator or
     * separate class.
     *
     * @return Sink<T>
     */
    public Sink<T> getSink();

    /**
     * Informs the port that it is active, i.e. connected to an incoming stream.
     *
     * @param connected
     */
    public void setConnected(boolean connected);

    /**
     * Provide the codec which can be used to serialize or deserialize the data
     * that can be received on the port. If there is no specific implementation
     * then it can return null, in which case the engine may use a generic codec.
     *
     * @return codec if special implementation, null otherwise.
     */
    public Class<? extends StreamCodec<T>> getStreamCodec();

  }

  /**
   * Output ports deliver data produced by the operator to a stream, abstracted by
   * Sink and injected by the execution engine at deployment time. Ports are
   * declared as annotated fields in the operator. The interface should be
   * implemented with a bounded type parameter for introspection and validation.
   *
   * @param <T>
   */
  public interface OutputPort<T> extends Port
  {
    /**
     * Set the sink for the output port.
     * Called by execution engine sink at deployment time.
     *
     * @param s
     */
    public void setSink(Sink<Object> s);

    /**
     * Merge tuples emitted by multiple upstream instances of the enclosing
     * operator (partitioned or load balanced).
     *
     * @return unifier object which can merge partitioned streams together into a single stream.
     */
    public Unifier<T> getUnifier();

  }

}
