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
package com.datatorrent.bufferserver.packet;

import com.datatorrent.netlet.util.Slice;

/**
 * <p>DataTuple class.</p>
 *
 * @since 0.3.2
 */
public class DataTuple extends Tuple
{
  public DataTuple(byte[] array, int offset, int index)
  {
    super(array, offset, index);
  }

  @Override
  public MessageType getType()
  {
    return MessageType.CODEC_STATE;
  }

  @Override
  public Slice getData()
  {
    return new Slice(buffer, offset, limit - offset);
  }

  public static byte[] getSerializedTuple(byte type, Slice f)
  {
    byte[] array = new byte[f.length + 1];
    array[0] = type;
    System.arraycopy(f.buffer, f.offset, array, 1, f.length);
    return array;
  }

}
