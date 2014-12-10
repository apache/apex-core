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
public class StramToNodeSetPropertyRequest extends StreamingContainerUmbilicalProtocol.StramToNodeRequest implements Serializable
{
  private String propertyKey;
  private String propertyValue;

  public StramToNodeSetPropertyRequest()
  {
    requestType = RequestType.SET_PROPERTY;
  }

  public String getPropertyKey()
  {
    return propertyKey;
  }

  public void setPropertyKey(String propertyKey)
  {
    this.propertyKey = propertyKey;
  }

  public String getPropertyValue()
  {
    return propertyValue;
  }

  public void setPropertyValue(String propertyValue)
  {
    this.propertyValue = propertyValue;
  }

  private static final long serialVersionUID = 201405271034L;

}
