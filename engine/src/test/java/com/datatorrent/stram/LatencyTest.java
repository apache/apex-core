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
package com.datatorrent.stram;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.support.StramTestSupport;

public class LatencyTest
{
  private static final Logger LOG = LoggerFactory.getLogger(LatencyTest.class);
  @Rule
  public StramTestSupport.TestMeta testMeta = new StramTestSupport.TestMeta();

  private LogicalPlan dag;
  private StreamingContainerManager scm;
  private PTOperator o1p1;
  private PTOperator o2p1;
  private PTOperator o3p1;

  private static final int windowWidthMillis = 600;
  private static final int heartbeatTimeoutMillis = 30000;

  @Before
  public void setup()
  {
    dag = StramTestSupport.createDAG(testMeta);
    dag.setAttribute(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS, windowWidthMillis);
    dag.setAttribute(Context.DAGContext.HEARTBEAT_TIMEOUT_MILLIS, heartbeatTimeoutMillis);
    dag.setAttribute(com.datatorrent.api.Context.OperatorContext.STORAGE_AGENT, new StramTestSupport
        .MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);

    dag.addStream("o1.output1", o1.outport1, o3.inport1);
    dag.addStream("o2.output1", o2.outport1, o3.inport2);
    scm = new StreamingContainerManager(dag);
    PhysicalPlan plan = scm.getPhysicalPlan();
    o1p1 = plan.getOperators(dag.getMeta(o1)).get(0);
    o2p1 = plan.getOperators(dag.getMeta(o2)).get(0);
    o3p1 = plan.getOperators(dag.getMeta(o3)).get(0);
  }

  private long getLatency(long windowId1, long windowId2, long windowId3, final boolean endWindowStatsExists, final long ewt1, final long ewt2, final long ewt3)
  {
    o1p1.stats.statsRevs.checkout();
    o1p1.stats.currentWindowId.set(windowId1);
    o1p1.stats.statsRevs.commit();

    o2p1.stats.statsRevs.checkout();
    o2p1.stats.currentWindowId.set(windowId2);
    o2p1.stats.statsRevs.commit();

    o3p1.stats.statsRevs.checkout();
    o3p1.stats.currentWindowId.set(windowId3);
    o3p1.stats.statsRevs.commit();

    return scm.updateOperatorLatency(o3p1, new StreamingContainerManager.UpdateOperatorLatencyContext()
    {
      @Override
      long getRPCLatency(PTOperator oper)
      {
        return 0;
      }

      @Override
      boolean endWindowStatsExists(long windowId)
      {
        return endWindowStatsExists;
      }

      @Override
      long getEndWindowEmitTimestamp(long windowId, PTOperator oper)
      {
        if (oper == o1p1) {
          return ewt1;
        } else if (oper == o2p1) {
          return ewt2;
        } else if (oper == o3p1) {
          return ewt3;
        } else {
          Assert.fail();
          return 0;
        }
      }
    });
  }

  @Test
  public void testLatency()
  {
    // When all end window stats are available and latency within heartbeatTimeout
    Assert.assertEquals(100, getLatency(1000, 1000, 1000, true, 1000, 1500, 1600));

    // When all end window stats are available and calculated latency is more than heartbeatTimeout
    Assert.assertEquals((10000 - 100) * windowWidthMillis, getLatency(10000, 10000, 100, true, 1000, 1500, 1600));

    // When end window stats are not available
    Assert.assertEquals((1000 - 997) * windowWidthMillis, getLatency(1000, 1000, 997, false, 1000, 1500, 1600));

    // When the current window is larger than upstream's current window
    Assert.assertEquals(-1, getLatency(1000, 1000, 1001, true, -1, -1, 1600));

    // When the current window of an operator is not available yet
    Assert.assertEquals(-1, getLatency(1000, 1000, 0, false, -1, -1, -1));

    // When the current window of an operator is the same as upstream and no end window stats are available
    Assert.assertEquals(0, getLatency(1000, 90, 1000, false, -1, -1, -1));

  }

}
