package com.malhartech.api;

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
   * Representation of streams in the logical layer. Instances are created through {@link DAG.addStream}.
   */
  public interface StreamMeta extends Serializable
  {
    public String getId();

    /**
     * Hint to manager that adjacent operators should be deployed in same container.
     *
     * @return boolean
     */
    public boolean isInline();

    public StreamMeta setInline(boolean inline);

    public boolean isNodeLocal();

    public StreamMeta setNodeLocal(boolean local);

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

  public abstract <T extends Operator> T addOperator(String name, T operator);

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

  public abstract <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1, Operator.InputPort<? super T> sink2);

  public abstract <T> void setAttribute(DAGContext.AttributeKey<T> key, T value);

  public abstract <T> void setAttribute(Operator operator, OperatorContext.AttributeKey<T> key, T value);

  public abstract <T> void setOutputPortAttribute(Operator.OutputPort<?> port, PortContext.AttributeKey<T> key, T value);

  public abstract <T> void setInputPortAttribute(Operator.InputPort<?> port, PortContext.AttributeKey<T> key, T value);

  public abstract OperatorMeta getOperatorMeta(String operatorId);

  public abstract OperatorMeta getMeta(Operator operator);

}
