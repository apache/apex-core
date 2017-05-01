/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.api.plugin;

import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;

@Evolving
/**
 * @since 3.6.0
 */
public class DAGSetupEvent extends Event.BaseEvent<DAGSetupEvent.Type>
{
  @Evolving
  public enum Type implements Event.Type
  {
    /**
     * This event is sent before platform adds operators and streams in the DAG. i.e this method
     * will get called just before {@link com.datatorrent.api.StreamingApplication#populateDAG(DAG, Configuration)}
     *
     * For Application specified using property and json file format, this will be sent
     * before platform adds operators and streams in the DAG as per specification in the file.
     */
    PRE_POPULATE_DAG,

    /**
     * This event is sent after platform adds operators and streams in the DAG. i.e this method
     * will get called just after {@link com.datatorrent.api.StreamingApplication#populateDAG(DAG, Configuration)}
     * in case application is specified in java.
     *
     * For Application specified using property and json file format, this will be sent
     * after platform has added operators and streams in the DAG as per specification in the file.
     */
    POST_POPULATE_DAG,

    /**
     * This event is sent before DAG is configured, i.e operator and application
     * properties/attributes are injected from configuration files.
     */
    PRE_CONFIGURE_DAG,

    /**
     * This event is sent after DAG is configured, i.e operator and application
     * properties/attributes are injected from configuration files.
     */
    POST_CONFIGURE_DAG,

    /**
     * This event is sent just before dag is validated before final job submission.
     */
    PRE_VALIDATE_DAG,

    /**
     * This event is sent after dag is validated. If plugin makes in incompatible changes
     * to the DAG at this stage, then application may get launched incorrectly or application
     * launch may fail.
     */
    POST_VALIDATE_DAG;

    public final DAGSetupEvent event;

    Type()
    {
      event = new DAGSetupEvent(this);
    }
  }

  private DAGSetupEvent(DAGSetupEvent.Type type)
  {
    super(type);
  }
}
