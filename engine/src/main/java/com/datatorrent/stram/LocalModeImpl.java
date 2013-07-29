package com.datatorrent.stram;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;

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
  public Controller getController() {
    try {
      return new StramLocalCluster(lp);
    } catch (Exception e) {
      throw new RuntimeException("Error creating local cluster", e);
    }
  }
}
