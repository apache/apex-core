/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.api;

import java.io.Serializable;

/**
 * <p>StramToNodeChangeLoggersRequest class.</p>
 *
 * @since 1.0.4
 */
public class StramToNodeStartRecordingRequest extends StreamingContainerUmbilicalProtocol.StramToNodeRequest implements Serializable
{
  private long numWindows = 0;
  private String id;

  public StramToNodeStartRecordingRequest()
  {
    requestType = RequestType.START_RECORDING;
  }

  public long getNumWindows()
  {
    return numWindows;
  }

  public String getId()
  {
    return id;
  }

  public void setNumWindows(long numWindows)
  {
    this.numWindows = numWindows;
  }

  public void setId(String id)
  {
    this.id = id;
  }
  
  private static final long serialVersionUID = 201405271034L;

}
