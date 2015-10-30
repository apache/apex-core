package com.datatorrent.stram.moduleexperiment.testModule;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.api.Operator.ProxyInputPort;
import com.datatorrent.api.Operator.ProxyOutputPort;

/**
 * Inner Module - Level 2
 * Has an Operator which splits the stream into odd and even integers
 */
public class InnerModule implements Module
{
  /*
   * Proxy ports for the module
   * mInput - Input Proxy Port
   * mOutputEven, mOutputOdd - Output Proxy Ports
   */
  public transient ProxyInputPort<Integer> mInput = new ProxyInputPort<Integer>();
  public transient ProxyOutputPort<Integer> mOutputEven = new ProxyOutputPort<Integer>();
  public transient ProxyOutputPort<Integer> mOutputOdd = new ProxyOutputPort<Integer>();

  /**
   * populateDag() method for the module.
   * Called when expanding the logical Dag
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    OddEvenOperator moduleOperator = dag.addOperator("InnerModule", new OddEvenOperator());
    /*
     * Set the operator ports inside the proxy port objects
     */
    mInput.set(moduleOperator.input);
    mOutputEven.set(moduleOperator.even);
    mOutputOdd.set(moduleOperator.odd);
  }
}
