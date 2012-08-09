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
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface BackupAgent
{
  public OutputStream borrowOutputStream(String nodeId) throws IOException;

  public void returnOutputStream(String nodeId, long windowId, OutputStream os) throws IOException;

  public InputStream getInputStream(String nodeId);
}
