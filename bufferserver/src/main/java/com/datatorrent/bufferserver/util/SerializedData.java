/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.bufferserver.util;

import com.datatorrent.netlet.util.Slice;

/**
 * Wrapper for a {@code byte[]}, which provides read-only access and can "reveal" a partial slice of the underlying
 * array.<p>
 *
 * <b>Note:</b> Multibyte accessors all use big-endian order.
 *
 * @since 0.3.2
 */
public final class SerializedData extends Slice
{
  /**
   * the offset at which the actual data begins. Between offset and dataOffset, the length of the data is stored.
   */
  public int dataOffset;

  public SerializedData(byte[] array, int offset, int size)
  {
    super(array, offset, size);
  }

  private static final long serialVersionUID = 201596191703L;
}
