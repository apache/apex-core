package com.datatorrent.api;

/**
 * Created by sandeep on 24/8/15.
 */

public class PortPair <T>
{
  public PortPair(DefaultOutputPort<T> a, DefaultInputPort<T> b)
  {
    this.outputPort = a;
    this.inputPort = b;
  }

  public Operator.OutputPort getOutputPort()
  {
    return outputPort;
  }

  public void setOutputPort(Operator.OutputPort<T> a)
  {
    outputPort = a;
  }

  public Operator.InputPort<T> getInputPort()
  {
    return inputPort;
  }

  public void setInputPort(Operator.InputPort<T> b)
  {
    inputPort = b;
  }

  Operator.OutputPort outputPort;
  Operator.InputPort inputPort;

  public void PortPair(Operator.OutputPort<T> a, Operator.InputPort<T> b){
    this.outputPort = a ;
    this.inputPort = b ;
  }
  public void connect(DAG dag, String label){
    dag.addStream(label, this.outputPort, this.inputPort);

  }
}
