/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

import java.io.IOException;

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
  public void backup(int operatorId, long windowId, Object o) throws IOException;

  public Object restore(int operatorId, long windowId) throws IOException;

  public void delete(int operatorId, long windowId) throws IOException;

  public OperatorCodec getOperatorSerDe();
}
