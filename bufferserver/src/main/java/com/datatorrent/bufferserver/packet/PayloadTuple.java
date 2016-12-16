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

import java.nio.ByteBuffer;

import com.datatorrent.netlet.util.Slice;

/**
 * <p>PayloadTuple class.</p>
 *
 * @since 0.3.2
 */
public class PayloadTuple extends Tuple
{
  public PayloadTuple(byte[] array, int offset, int length)
  {
    super(array, offset, length);
  }

  @Override
  public MessageType getType()
  {
    return MessageType.PAYLOAD;
  }

  @Override
  public int getPartition()
  {
    return ByteBuffer.wrap(buffer, offset, 4).getInt();
  }

  @Override
  public Slice getData()
  {
    return new Slice(buffer, offset + 4, limit - offset - 4);
  }

  @Override
  public String toString()
  {
    return "PayloadTuple{" + getPartition() + ", " + getData() + '}';
  }

  public static byte[] getSerializedTuple(int partition, int size)
  {
    byte[] array = new byte[size + 5];
    ByteBuffer byteBuffer = ByteBuffer.wrap(array);
    byteBuffer.put(MessageType.PAYLOAD_VALUE);
    byteBuffer.putInt(partition);
    return array;
  }

  public static byte[] getSerializedTuple(int partition, Slice f)
  {
    byte[] array = new byte[5 + f.length];
    ByteBuffer byteBuffer = ByteBuffer.wrap(array);
    byteBuffer.put(MessageType.PAYLOAD_VALUE);
    byteBuffer.putInt(partition);
    System.arraycopy(f.buffer, f.offset, array, 5, f.length);
    return array;
  }

}
