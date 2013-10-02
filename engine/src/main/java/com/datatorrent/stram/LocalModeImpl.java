package com.datatorrent.stram;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

import com.datatorrent.stram.cli.StramAppLauncher;
import com.datatorrent.stram.cli.StramAppLauncher.AppConfig;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * <p>LocalModeImpl class.</p>
 *
 * @since 0.3.2
 */
public class LocalModeImpl extends LocalMode {
  private static final Logger LOG = LoggerFactory.getLogger(LocalModeImpl.class);

  private final LogicalPlan lp = new LogicalPlan();

  @Override
  public DAG getDAG() {
    return lp;
  }

  @Override
  public DAG cloneDAG() throws Exception
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    LogicalPlan.write(lp, bos);
    bos.flush();
    LOG.debug("serialized size: {}", bos.toByteArray().length);
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    return LogicalPlan.read(bis);
  }

  @Override
  public DAG prepareDAG(final StreamingApplication app, Configuration conf) throws Exception
  {
    if (app == null && conf == null) {
      throw new IllegalArgumentException("Require app or configuration to populate logical plan.");
    }

    DAGPropertiesBuilder pb = new DAGPropertiesBuilder();
    pb.addFromConfiguration(conf);
    String name = "unknown";
    if (app != null) {
      name = app.getClass().getName();
    }
    StreamingApplication sapp = app;
    if (app == null) {
      sapp = pb;
    }
    pb.prepareDAG(sapp, lp, name, conf);

    /*
    final AppConfig appConfig = new AppConfig() {
      @Override
      public String getName()
      {
        if (app != null) {
          return app.getClass().getName();
        } else {
          return "unknown";
        }
      }

      @Override
      public StreamingApplication createApp(Configuration conf)
      {
        if (app == null) {
          DAGPropertiesBuilder tb = new DAGPropertiesBuilder();
          tb.addFromConfiguration(conf);
          return tb;
        } else {
          return app;
        }
      }
    };
    StramAppAgent appAgent = new StramAppAgent(conf);
    appAgent.prepareDAG(this.lp, appConfig);
    */
    //StramAppLauncher.prepareDAG(this.lp, appConfig, conf);
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
