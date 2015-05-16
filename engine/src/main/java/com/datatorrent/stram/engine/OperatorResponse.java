/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 */
package com.datatorrent.stram.engine;

import java.io.Serializable;

import com.datatorrent.api.StatsListener;

/**
 * <p>OperatorResponse class.</p>
 *
 * @since 2.1.0
 */
public class OperatorResponse implements StatsListener.OperatorResponse, Serializable
{
  private static final long serialVersionUID = -95162161527528335L;
  /*
   * The unique responseId
   */
  private Long responseId;

  /*
   * The data payload that needs to be sent back
   */
  private Object data;

  public OperatorResponse(long responseId, Object data)
  {
    this.responseId = responseId;
    this.data = data;
  }

  @Override
  public Object getResponseId()
  {
    return responseId;
  }

  @Override
  public Object getResponse()
  {
    return data;
  }

  @Override
  public String toString()
  {
    return "{ responseId: " + responseId + ", data :" + data + "}";
  }
}
