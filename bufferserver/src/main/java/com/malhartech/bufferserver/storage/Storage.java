/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.storage;

import java.io.IOException;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface Storage
{
  public Storage getInstance() throws IOException;
  public void store(String Identifier, byte[] bytes, int startingOffset, int endingOffset);
  public void retrieve(String identifier);
  public void discard(String identifier);
}
