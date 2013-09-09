/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.stram.webapp.OperatorInfo;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * <p>StatsRecorder interface.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.4
 */
public interface StatsRecorder
{
  public void recordContainers(Map<String, StramChildAgent> containerMap, long timestamp) throws IOException;

  public void recordOperators(List<OperatorInfo> operatorList, long timestamp) throws IOException;

}
