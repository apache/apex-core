package com.datatorrent.stram;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

/**
 * <p>LocalModeImpl class.</p>
 *
 * @since 0.3.2
 */
public class LocalModeImpl extends LocalMode {

  private final LogicalPlan lp = new LogicalPlan();

  @Override
  public DAG getDAG() {
    return lp;
  }

  @Override
  public DAG cloneDAG() throws Exception
  {
    return StramLocalCluster.cloneLogicalPlan(lp);
  }

  @Override
  public DAG prepareDAG(StreamingApplication app, Configuration conf) throws Exception
  {
    if (app == null && conf == null) {
      throw new IllegalArgumentException("Require app or configuration to populate logical plan.");
    }

    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration();
    String appName = "unknown";
    if (app == null) {
      app = lpc;
    } else {
      if (conf == null) {
        conf = new Configuration(false);
      }
      appName = app.getClass().getName();
    }
    lpc.addFromConfiguration(conf);
    lpc.prepareDAG(lp, app, appName, conf);
    return lp;
  }

  @Override
  public Controller getController() {
    try {
      return new StramLocalCluster(lp);
    } catch (Exception e) {
      throw new RuntimeException("Error creating local cluster", e);
    }
  }
}
