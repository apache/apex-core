/*
 *  Copyright (c) 2012-2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

import com.datatorrent.lib.util.JacksonObjectMapperProvider;
import com.datatorrent.stram.codec.LogicalPlanSerializer;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class JSONSerializationProvider extends JacksonObjectMapperProvider
{

  public JSONSerializationProvider()
  {
    super();
    addSerializer(LogicalPlan.class, new LogicalPlanSerializer());
  }
}
