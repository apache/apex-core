/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import java.io.IOException;

import com.malhartech.api.OperatorCodec;

/**
 *
 * Interface that defines how to write/read checkpoint state<p>
 * <br>
 * Currently this interface is for internal use only,
 * it may be exposed in the future to allow customization of state saving.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface BackupAgent
{
  public void backup(int operatorId, long windowId, Object o, OperatorCodec serDe) throws IOException;

  public Object restore(int operatorId, long windowId, OperatorCodec serDe) throws IOException;

  public void delete(int operatorId, long windowId) throws IOException;
}
