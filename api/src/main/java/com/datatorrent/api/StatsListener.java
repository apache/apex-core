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
package com.datatorrent.api;

import java.io.IOException;
import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.List;

import com.datatorrent.api.Stats.OperatorStats;


/**
 * Listener for operator status updates.
 * <p>
 * Can be directly implemented by operator class or defined via operator context attribute. Implementation in the
 * operator allows the operator developer to define a handler that along with the stats can access the operator
 * properties and control partitioning.
 *
 * @since 0.9.1
 */
@Deprecated
public interface StatsListener
{
  /**
   * Command to be executed at subsequent end of window on the operator instance that is deployed in the container.
   * Provides the opportunity to define operator specific actions such as method invocation or property set.
   */
  interface OperatorRequest
  {
    /**
     * Execute the command.
     *
     * @param operator
     * @param operatorId
     * @param windowId
     * @throws IOException
     */
    OperatorResponse execute(Operator operator, int operatorId, long windowId) throws IOException;
  }

  /**
   * Use {@link OperatorRequest}
   */
  @Deprecated
  interface OperatorCommand
  {
    /**
     * Execute the command.
     *
     * @param operator
     * @param operatorId
     * @param windowId
     * @throws IOException
     */
    void execute(Operator operator, int operatorId, long windowId) throws IOException;
  }

  interface OperatorResponse
  {

    /*
     * The Object to identify the response
     */
    Object getResponseId();

    /*
     * The data payload that needs to be sent back
     */
    Object getResponse();

  }

  /**
   * List of recent, per window operator stats and moving averages.
   */
  interface BatchedOperatorStats
  {
    /**
      Stats list will typically contain multiple entries, depending on streaming window size and heartbeat interval.
      * @return
      */
    List<OperatorStats> getLastWindowedStats();

    int getOperatorId();

    long getCurrentWindowId();

    long getTuplesProcessedPSMA();

    long getTuplesEmittedPSMA();

    double getCpuPercentageMA();

    long getLatencyMA();

    List<OperatorResponse> getOperatorResponse();
  }

  /**
   * This is an interface to through wich {@link StatsListenerWithContext#processStats(BatchedOperatorStats, StatsListenerContext)}
   * can access information about operator or other elements in the DAG. Currently we only provide method to
   * extract the operator name based on the physical id of the operator. In future more methods can be added
   * to provide additional information to the StatsHandlers.
   */
  interface StatsListenerContext
  {
    /**
     * Return name of the operator given its id. Returns null if operator id is not found
     * in the DAG.
     *
     * @param id Operator id
     * @return name of the operator. null in case operator id is not found in DAG.
     */
    String getOperatorName(int id);
  }

  class Response implements Serializable
  {
    /**
     * Set true to request repartition of the logical operator.
     * The controller will call {@link Partitioner#definePartitions(Collection, Partitioner.PartitioningContext)} if applicable.
     */
    public boolean repartitionRequired;

    /**
     * Load indicator for the partition. See {@link Partitioner.Partition#getLoad()}.
     * Taken into consideration on repartition.
     */
    public int loadIndicator;

    /**
     * Note for repartition.  Should indicate the reason if there is a partition of the operator
     */
    public String repartitionNote;

    /**
     * List of commands to be executed on all deployed operator instances.
     */
    public List<? extends OperatorRequest> operatorRequests;

    /**
     * for backward compatibility
     */
    public List<? extends OperatorCommand> operatorCommands;

    private static final long serialVersionUID = 201401201506L;
  }

  /**
   * Called when new stats become available and status for operator is updated.
   * @param stats
   * @return
   */
  @Deprecated
  Response processStats(BatchedOperatorStats stats);

  /**
   *  This is used to tell the operator stats listener is interested in knowing the queue_size
   */
  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  @interface DataQueueSize
  {
  }

  /**
   * This interface extends existing {@link StatsListener} interface to provide addition argument of type
   * {@link StatsListenerContext} to {@link StatsListener#processStats(BatchedOperatorStats)},
   * Using this extra argument listener can get access to additional information about the DAG.
   * The separate interface is created to maintain backward compatibility.
   */
  interface StatsListenerWithContext extends StatsListener
  {
    /**
     * Called when new stats become available and status for the operator is updated.
     *
     * @param stats collected stats for the operator instance.
     * @param context instance of StatsListenerContext
     * @return response
     */
    Response processStats(BatchedOperatorStats stats, StatsListenerContext context);
  }
}
