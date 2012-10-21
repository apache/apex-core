/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

import com.malhartech.dag.Component;
import com.malhartech.dag.OperatorContext;

public interface Operator extends Component<OperatorConfiguration, OperatorContext>
{
  /**
   * This method gets called at the beginning of each window.
   *
   */
  public void beginWindow();

  /**
   * This method gets called at the end of each window.
   *
   */
  public void endWindow();

  /**
   * A module provides ports as a means to consume and produce data tuples.
   * Concrete ports implement derived interfaces. The common characteristic is
   * that ports provide a reference to the module instance they belong to.
   */
  public interface Port
  {
    /**
     * Reference to the operator to which this port belongs.
     *
     * @return
     */
    public Operator getOperator();
  }

  /**
   * Input ports process data delivered through a stream. The execution engine
   * will call the port's associated sink to pass the tuples. Ports are declared
   * as annotated fields in the module. The interface should be implemented by a
   * non parameterized class to make the type parameter are available at runtime
   * for validation.
   *
   * @param <T>
   */
  public interface InputPort<T> extends Port
  {
    /**
     * Provide the sink that will process incoming data. Sink would typically be
     * the port itself but can also be implemented by the enclosing module or
     * separate class.
     *
     * @param payload
     */
    public Sink<T> getSink();

    /**
     * Informs the port that it is active, i.e. connected to an incoming stream.
     *
     * @param connected
     */
    public void setConnected(boolean connected);
  }

  /**
   * Output ports deliver data produced by a module to a stream, abstracted by
   * Sink and injected by the execution engine at deployment time. Ports are
   * declared as annotated fields in the module. The interface should be
   * implemented by a non parameterized class to make the type parameter
   * available at runtime for validation.
   *
   * @param <T>
   */
  public interface OutputPort<T> extends Port
  {
    /**
     * Called by execution engine to inject sink at deployment time.
     *
     * @param s
     */
    public void setSink(Sink<T> s);

    /**
     * Merge tuples emitted by multiple upstream instances of the enclosing
     * module (partitioning or load balancing).
     *
     * @param tuple
     */
    public void merge(T tuple);
  }
}
