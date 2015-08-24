/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.api;

import com.datatorrent.api.Context.ModuleContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.sun.tools.javac.util.Pair;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * Module interface.</p>
 *
 */
public interface Module extends Component<ModuleContext>
{

 /**
  * A method to return a map of operator name to operators in the module
  *
  */

  public Map<String, Operator> getOperators();

 /**
  * A method to return a map of stream name to streams in the module
  */
  public Map<String, PortPair> getStreams();

 /**
  * A method that returns a map of port name to input ports that are exposed by the module
  *
  * @return input ports map
  */

  public Map<String, Operator.InputPort> getInputPorts();

 /**
  * A method that returns a map of port name to output ports that are exposed by the module
  *
  * @return output ports map
  */

  public Map<String, Operator.OutputPort> getOutputPorts();

 /**
  * The method will flatten the DAG with internal operators and connections
  *
  * @param dag
  * @param conf
  */

  public void flattenDAG(DAG dag, Configuration conf);

  /**
   * Get property for the module
   * @param key
   * @param value
   */
  //public Object getProperty(String key);

  /**
   * Set a property for the module
   * @param key
   * @param value
   * @return
   */

  //public Object setProperty(String key, Object value);

}
