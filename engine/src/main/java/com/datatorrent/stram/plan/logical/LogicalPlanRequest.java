/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan.logical;

import com.datatorrent.stram.plan.physical.PlanModifier;
import java.io.IOException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * <p>Abstract LogicalPlanRequest class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.2
 */
public abstract class LogicalPlanRequest
{
  public String getRequestType()
  {
    return this.getClass().getSimpleName();
  }

  @Override
  public String toString()
  {
    try {
      return new ObjectMapper().writeValueAsString(this);
    }
    catch (IOException ex) {
      return ex.toString();
    }
  }
  
  public abstract void execute(PlanModifier pm);

}
