package com.datatorrent.stram.plan.physical;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitioner.PartitionKeys;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.google.common.collect.Maps;

/**
 * <p>OperatorPartitions class.</p>
 *
 * @since 0.3.2
 */
public class OperatorPartitions {

  public static Map<LogicalPlan.InputPortMeta, PartitionKeys> convertPartitionKeys(PTOperator oper, Map<InputPort<?>, PartitionKeys> portKeys)
  {
    if (portKeys == null) {
      return Collections.emptyMap();
    }
    HashMap<LogicalPlan.InputPortMeta, PartitionKeys> partitionKeys = Maps.newHashMapWithExpectedSize(portKeys.size());
    for (Map.Entry<InputPort<?>, PartitionKeys> portEntry : portKeys.entrySet()) {
      LogicalPlan.InputPortMeta pportMeta = oper.operatorMeta.getMeta(portEntry.getKey());
      if (pportMeta == null) {
        throw new AssertionError("Invalid port reference " + portEntry);
      }
      partitionKeys.put(pportMeta, portEntry.getValue());
    }
    return partitionKeys;
  }
}
