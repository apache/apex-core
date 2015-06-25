/**
 * Put your copyright and license info here.
 */
package com.example.mydtapp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build

    RandomNumberGenerator rand = dag.addOperator("rand", RandomNumberGenerator.class);
    StdoutOperator stdout = dag.addOperator("stdout", new StdoutOperator());

    dag.addStream("data", rand.out, stdout.in).setLocality(Locality.CONTAINER_LOCAL);
  }
}
