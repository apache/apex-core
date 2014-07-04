/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.api;

import java.io.Serializable;
import java.util.Map;

/**
 * <p>StramToNodeChangeLoggersRequest class.</p>
 *
 * @since 1.0.2
 */
public class StramToNodeChangeLoggersRequest extends StreamingContainerUmbilicalProtocol.StramToNodeRequest implements Serializable
{
  private Map<String, String> targetChanges;

  public StramToNodeChangeLoggersRequest()
  {
    requestType = RequestType.SET_LOG_LEVEL;
  }

  public void setTargetChanges(Map<String, String> targetChanges)
  {
    this.targetChanges = targetChanges;
  }

  public Map<String, String> getTargetChanges()
  {
    return this.targetChanges;
  }

  private static final long serialVersionUID = 201405271034L;

}
