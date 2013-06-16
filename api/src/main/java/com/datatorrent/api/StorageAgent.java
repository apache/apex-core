/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Interface to define writing/reading checkpoint state
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface StorageAgent
{
  /**
   * Store the state of the object against the unique key formed using operatorId and windowId.
   *
   * Typically the object passed is an operator or some other aggregate object which contains
   * reference to operator object. One can use JavaSerializer
   *
   * @param operatorId
   * @param windowId
   * @return
   * @throws IOException
   */
  public OutputStream getSaveStream(int operatorId, long windowId) throws IOException;

  /**
   * Get the input stream from which can be used to retrieve the stored objects back.
   *
   * @param operatorId Operator of the id for which the object was previously saved
   * @param windowId WindowId for which the object was previously saved
   * @return Input stream which can be used to retrieve the serialized object
   * @throws IOException
   */
  public InputStream getLoadStream(int operatorId, long windowId) throws IOException;

  public void delete(int operatorId, long windowId) throws IOException;

}
