/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 * Interface that defines how to write checkpoint state<p>
 * <br>
 * 
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface BackupAgent
{
  public OutputStream borrowOutputStream(String nodeId, long windowId) throws IOException;

  public void returnOutputStream(OutputStream os) throws IOException;

  /**
   * Return the input stream for restoring the node.
   * Caller is responsible for closing stream once done.
   *
   * @param nodeId
   * @return {@link java.io.InputStream}
   * @throws IOException
   */
  public InputStream getInputStream(String nodeId, long windowId) throws IOException;
}
