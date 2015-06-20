/*
 *  Copyright (c) 2012-2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

import com.datatorrent.common.util.JacksonObjectMapperProvider;
import com.datatorrent.stram.codec.LogicalPlanSerializer;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * <p>JSONSerializationProvider class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 2.1.0
 */
public class JSONSerializationProvider extends JacksonObjectMapperProvider
{

  public JSONSerializationProvider()
  {
    super();
    addSerializer(LogicalPlan.class, new LogicalPlanSerializer());
  }
}
