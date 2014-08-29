/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.api;

import java.io.Serializable;
import java.util.Map;

/**
 * <p>StramToNodeChangeLoggersRequest class.</p>
 *
 * @since 1.0.4
 */
public class StramToNodeStartRecordingRequest extends StreamingContainerUmbilicalProtocol.StramToNodeRequest implements Serializable
{
  private long numWindows = 0;

  public StramToNodeStartRecordingRequest()
  {
    requestType = RequestType.START_RECORDING;
  }

  public long getNumWindows()
  {
    return numWindows;
  }

  public void setNumWindows(long numWindows)
  {
    this.numWindows = numWindows;
  }
  
  private static final long serialVersionUID = 201405271034L;

}
