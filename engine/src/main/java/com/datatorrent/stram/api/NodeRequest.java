/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.api;

import java.io.IOException;

import com.datatorrent.api.Operator;

/**
 * <p>NodeRequest interface.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.5
 */
public interface NodeRequest
{
  enum RequestType
  {
    START_RECORDING, STOP_RECORDING, SYNC_RECORDING, SET_PROPERTY
  }

  /**
   * Command to be executed at subsequent end of window.
   *
   * @param operator
   * @param id
   * @param windowId
   * @throws IOException
   */
  public void execute(Operator operator, int id, long windowId) throws IOException;

}
