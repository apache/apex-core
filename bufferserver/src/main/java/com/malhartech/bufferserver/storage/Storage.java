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
  /**
   * get an instance of the class which implements Storage interface.
   *
   * @return instance of storage.
   * @throws IOException
   */
  public Storage getInstance() throws IOException;

  /**
   * Store memory block represented by block in non memory storage.
   *
   * @param Identifier - application specific Id, this need not be unique.
   * @param bytes - memory represented as byte array
   * @param start - the offset of the first byte in the array
   * @param end - the offset of the last byte in the array.
   * @return unique identifier for the stored block.
   */
  public int store(String Identifier, byte[] bytes, int start, int end);

  /**
   *
   * @param identifier
   * @param uniqueIdentifier
   * @return memory block which was stored with the passed parameters as identifying information.
   */
  public byte[] retrieve(String identifier, int uniqueIdentifier);

  /**
   * Discard the block stored from the secondary storage.
   * @param identifier
   * @param uniqueIdentifier
   */
  public void discard(String identifier, int uniqueIdentifier);
}
