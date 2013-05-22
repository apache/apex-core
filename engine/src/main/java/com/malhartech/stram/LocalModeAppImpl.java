package com.malhartech.stram;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.DAG;
import com.malhartech.api.LocalMode;
import com.malhartech.stram.plan.logical.LogicalPlan;

public class LocalModeAppImpl extends LocalMode {
  private static final Logger LOG = LoggerFactory.getLogger(LocalModeAppImpl.class);

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
  public Controller getController() {
    try {
      return new StramLocalCluster(lp);
    } catch (Exception e) {
      throw new RuntimeException("Error creating local cluster", e);
    }
  }
}
