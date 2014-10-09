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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;

/**
 * <p>
 * Operator interface.</p>
 *
 * @since 0.3.2
 */
public interface Operator extends Component<OperatorContext>
{
  /**
   * One can set attribute on an Operator to indicate the mode in which it processes Tuples.
   * In AT_LEAST_ONCE mode it's guaranteed that the tuples will be processed at least once. In this mode even though
   * some tuples are processed again, the processing itself is idempotent and the output does not reflect double
   * processed data. This is the default mode.
   * <br />
   * In AT_MOST_ONCE mode in case of failure, the operator will start with the tuples which are being sent at the time
   * the failed operator is recovered. Unlike AT_LEAST_MOST once operator, it will not try to recover the tuples which
   * may have arrived while operator was down. Typically you would want to mark operators AT_MOST_ONCE if it does not
   * materially impact your computation if a few tuples are omitted from the computation and the expected throughput is
   * most likely to consume all the resources available for the operator or the DAG.
   * <br />
   * In EXACTLY_ONCE mode, it will be guaranteed that once a streaming window is processed
   * completely, none of the tuples in that window will be processed again.
   *
   */
  enum ProcessingMode
  {
    AT_LEAST_ONCE("AT_LEAST_ONCE"),
    AT_MOST_ONCE("AT_MOST_ONCE"),
    EXACTLY_ONCE("EXACTLY_ONCE");
    private final String mode;

    private ProcessingMode(String mode)
    {
      this.mode = mode;
    }

    public boolean equalsName(String othermode)
    {
      return othermode == null ? false : mode.equals(othermode);
    }

    @Override
    public String toString()
    {
      return mode;
    }

  }

  /**
   * This method gets called at the beginning of each window.
   *
   * @param windowId identifier for the window that is unique for this run of the application.
   */
  void beginWindow(long windowId);

  /**
   * This method gets called at the end of each window.
   */
  void endWindow();

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
  interface Unifier<T> extends Operator
  {
    void process(T tuple);

  }

  /**
   * A operator provides ports as a means to consume and produce data tuples.
   * Concrete ports implement derived interfaces.
   */
  @SuppressWarnings("MarkerInterface")
  interface Port extends Component<PortContext>
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
  interface InputPort<T> extends Port
  {
    /**
     * Provide the sink that will process incoming data. Sink would typically be
     * the port itself but can also be implemented by the enclosing operator or
     * separate class.
     *
     * @return Sink<T>
     */
    Sink<T> getSink();

    /**
     * Informs the port that it is active, i.e. connected to an incoming stream.
     *
     * @param connected
     */
    void setConnected(boolean connected);

    /**
     * Provide the codec which can be used to serialize or deserialize the data
     * that can be received on the port. If there is no specific implementation
     * then it can return null, in which case the engine may use a generic codec.
     *
     * @return codec if special implementation, null otherwise.
     */
    Class<? extends StreamCodec<T>> getStreamCodec();
  }

  /**
   * Output ports deliver data produced by the operator to a stream, abstracted by
   * Sink and injected by the execution engine at deployment time. Ports are
   * declared as annotated fields in the operator. The interface should be
   * implemented with a bounded type parameter for introspection and validation.
   *
   * @param <T>
   */
  interface OutputPort<T> extends Port
  {
    /**
     * Set the sink for the output port.
     * Called by execution engine sink at deployment time.
     *
     * @param s
     */
    void setSink(Sink<Object> s);

    /**
     * Merge tuples emitted by multiple upstream instances of the enclosing
     * operator (partitioned or load balanced).
     *
     * @return unifier object which can merge partitioned streams together into a single stream.
     */
    Unifier<T> getUnifier();

  }

  /**
   * The operator should throw the following exception if it wants to gracefully conclude its operation.
   * This exception is not treated as an error by the engine. It's considered a request by the operator
   * to deactivate itself. Upon receiving this, the engine would let the operator finish its in progress
   * window and then call deactivate method on it if present.
   *
   */
  static class ShutdownException extends RuntimeException
  {
    private static final long serialVersionUID = 201401081529L;

  }

  /**
   * Interface operator must implement if they want the the engine to inform them as
   * they are activated or before they are deactivated.
   *
   * An operator may be subjected to activate/deactivate cycle multiple times during
   * its lifetime which is bounded by setup/teardown method pair. So it's advised that
   * all the operations which need to be done right before the first window is delivered
   * to the operator be done during activate and opposite be done in the deactivate.
   *
   * An example of where one would consider implementing ActivationListener is an
   * input operator which wants to consume a high throughput stream. Since there is
   * typically at least a few hundreds of milliseconds between the time the setup method
   * is called and the first window, you would want to place the code to activate the
   * stream inside activate instead of setup.
   *
   * @param <CONTEXT> Context for the current run during which the operator is getting de/activated.
   * @since 0.3.2
   */
  public static interface ActivationListener<CONTEXT extends Context>
  {
    /**
     * Do the operations just before the operator starts processing tasks within the windows.
     * e.g. establish a network connection.
     * @param context - the context in which the operator is executing.
     */
    public void activate(CONTEXT context);

    /**
     * Do the opposite of the operations the operator did during activate.
     * e.g. close the network connection.
     */
    public void deactivate();

  }

  /**
   * Operators must implement this interface if they are interested in being notified as
   * soon as the operator state is checkpointed or committed.
   *
   * @since 0.3.2
   */
  public static interface CheckpointListener
  {
    /**
     * Inform the operator that it's checkpointed.
     *
     * @param windowId Id of the window after which the operator was checkpointed.
     */
    public void checkpointed(long windowId);

    /**
     * Inform the operator that a particular windowId is processed successfully by all the operators in the DAG.
     *
     * @param windowId Id of the window which is processed by each operator.
     */
    public void committed(long windowId);

  }

}
