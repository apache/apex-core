package com.datatorrent.stram.moduleexperiment.testModule;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.api.Operator.ProxyInputPort;
import com.datatorrent.api.Operator.ProxyOutputPort;

/**
 * Outer module - Level 1
 * Has an Operator - FilterOperator and a Module - OddEvenModule (Level 2)
 */
public class OuterModule implements Module
{

  public transient ProxyInputPort<Integer> mInput = new ProxyInputPort<Integer>();
  public transient ProxyOutputPort<Integer> mOutputEven = new ProxyOutputPort<Integer>();
  public transient ProxyOutputPort<Integer> mOutputOdd = new ProxyOutputPort<Integer>();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FilterOperator filter = dag.addOperator("FilterOperator", new FilterOperator());
    mInput.set(filter.input);

    InnerModule innerModule = dag.addModule("InnerModule", new InnerModule());
    mOutputEven.set(innerModule.mOutputEven);
    mOutputOdd.set(innerModule.mOutputOdd);

    dag.addStream("FilterToInnerModule", filter.output, innerModule.mInput);
  }
}
