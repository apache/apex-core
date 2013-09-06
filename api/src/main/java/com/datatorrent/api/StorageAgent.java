/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Interface to define writing/reading checkpoint state
 *
 * @since 0.3.2
 */
public interface StorageAgent
{
  /**
   * Store the state of the object against the unique key formed using operatorId and windowId.
   *
   * Typically the stream is used to write an operator or some other aggregate object which contains
   * reference to operator object.
   *
   * @param operatorId
   * @param windowId
   * @return OutputStream
   * @throws IOException
   */
  public OutputStream getSaveStream(int operatorId, long windowId) throws IOException;

  /**
   * Get the input stream from which can be used to retrieve the stored objects back.
   *
   * @param operatorId Id for which the object was previously saved
   * @param windowId WindowId for which the object was previously saved
   * @return Input stream which can be used to retrieve the serialized object
   * @throws IOException
   */
  public InputStream getLoadStream(int operatorId, long windowId) throws IOException;

  /**
   * <p>delete.</p>
   */
  public void delete(int operatorId, long windowId) throws IOException;

  /**
   * Return the most recent windowId for which state identified by operatorId was saved.
   * @param operatorId
   * @return windowId, null when no state was saved for given id.
   * @throws IOException
   * @since 0.3.5
   */
  public Long getMostRecentWindowId(int operatorId) throws IOException;

}
