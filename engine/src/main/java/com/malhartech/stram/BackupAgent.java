/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import java.io.IOException;

import com.malhartech.dag.ModuleSerDe;

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
  public void backup(String operatorId, long windowId, Object o, ModuleSerDe serDe) throws IOException;

  public Object restore(String operatorId, long windowId, ModuleSerDe serDe) throws IOException;

  public void delete(String operatorId, long windowId) throws IOException;
}
