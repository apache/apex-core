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
  interface Block
  {
    public String getIdentifier();

    public String getNumber();

    public byte[] getBytes();

  }

  public Storage getInstance() throws IOException;

  public Block retrieveFirstBlock(String identifier);

  /**
   * Delete the data from permanent storage backing the block.
   *
   * @param block block whose associated data has to be deleted from the backing storage
   * @return the block if it was deleted, null otherwise.
   */
  public Block retrieveNextBlock(Block block);

  public Block storeFirstBlock(String identifier, byte[] bytes, int startingOffset, int endingOffset);

  public Block storeNextBlock(Block block, byte[] bytes, int startingOffset, int endingOffset);

  public Block delete(Block block);

}
