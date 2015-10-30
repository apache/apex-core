/**
 * Put your copyright and license info here.
 */
package com.datatorrent.stram.moduleexperiment.testModule;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.stram.moduleexperiment.testModule.OutputOperator;
import com.datatorrent.stram.moduleexperiment.testModule.RandomInputOperator;
import com.datatorrent.stram.moduleexperiment.testModule.OuterModule;

@ApplicationAnnotation(name="ApplicationWithModules")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Random input operator
    RandomInputOperator inputOperator = dag.addOperator("RandomInput", new RandomInputOperator());
    // Add a module
    OuterModule m = dag.addModule("OuterModule", new OuterModule());
    // Add output operators
    OutputOperator outputOperatorEven = dag.addOperator("ConsoleOutputEven", new OutputOperator("Even"));
    OutputOperator outputOperatorOdd = dag.addOperator("ConsoleOutputOdd", new OutputOperator("Odd"));

    // Add streams between operators and modules in the dag
    dag.addStream("RandomInputToModule", inputOperator.output, m.mInput);
    dag.addStream("ModuleToConsoleOutputEven", m.mOutputEven, outputOperatorEven.input);
    dag.addStream("ModuleToConsoleOutputOdd", m.mOutputOdd, outputOperatorOdd.input);
  }
}
