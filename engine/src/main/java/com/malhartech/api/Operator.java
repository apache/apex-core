/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

import com.malhartech.api.Context.OperatorContext;

public interface Operator extends Component<OperatorContext>
{
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
    public void merge(T tuple);

  }

  /**
   * A operator provides ports as a means to consume and produce data tuples.
   * Concrete ports implement derived interfaces. The common characteristic is
   * that ports provide a reference to the operator instance they belong to.
   */
  public interface Port
  {
    /**
     * Reference to the operator to which this port belongs.
     *
     * @return Operator
     */
    public Operator getOperator();

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
    public void setSink(Sink<T> s);

    /**
     * Merge tuples emitted by multiple upstream instances of the enclosing
     * operator (partitioned or load balanced).
     *
     * @return unifier object which can merge partitioned streams together into a single stream.
     */
    public Unifier<T> getUnifier();

    /**
     * Returns whether this port should be auto recorded
     *
     * @return boolean
     */
    public boolean isAutoRecord();

    /**
     * Sets whether this port should be auto recorded
     *
     */
    public void setAutoRecord(boolean autoRecord);

  }

}
