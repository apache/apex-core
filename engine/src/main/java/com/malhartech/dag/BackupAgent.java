/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import java.io.IOException;

/**
 *
 * Interface that defines how to write checkpoint state<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface BackupAgent
{
  public void backup(String nodeId, long windowId, Object o) throws IOException;

  /**
   * Return the input stream for restoring the node.
   * Caller is responsible for closing stream once done.
   *
   * @param nodeId
   * @return {@link java.io.InputStream}
   * @throws IOException
   */
  public Object restore(String nodeId, long windowId) throws IOException;
}
