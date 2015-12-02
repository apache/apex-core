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
/**
 * <p>VarInt class.</p>
 *
 * @since 3.2.0
 */
public class VarInt extends com.datatorrent.netlet.util.VarInt
{
  public static void read(SerializedData current)
  {
    final byte[] data = current.buffer;
    int offset = current.offset;

    byte tmp = data[offset++];
    if (tmp >= 0) {
      current.dataOffset = offset;
      current.length = tmp + offset - current.offset;
      return;
    }
    int result = tmp & 0x7f;
    if ((tmp = data[offset++]) >= 0) {
      result |= tmp << 7;
    } else {
      result |= (tmp & 0x7f) << 7;
      if ((tmp = data[offset++]) >= 0) {
        result |= tmp << 14;
      } else {
        result |= (tmp & 0x7f) << 14;
        if ((tmp = data[offset++]) >= 0) {
          result |= tmp << 21;
        } else {
          result |= (tmp & 0x7f) << 21;
          result |= (tmp = data[offset++]) << 28;
          if (tmp < 0) {
            // Discard upper 32 bits.
            for (int i = 0; i < 5; i++) {
              if (data[offset++] >= 0) {
                current.dataOffset = offset;
                current.length = result + offset - current.offset;
                return;
              }
            }

            current.length = -1;
            return;
          }
        }
      }
    }

    current.dataOffset = offset;
    current.length = result + offset - current.offset;
  }

}
