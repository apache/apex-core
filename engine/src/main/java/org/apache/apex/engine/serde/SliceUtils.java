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
package org.apache.apex.engine.serde;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.netlet.util.Slice;

/**
 * A utility class which contains static methods for manipulating byte arrays and {@link Slice}s
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class SliceUtils
{
  private SliceUtils()
  {
  }

  /**
   * Concatenates two byte arrays.
   * @param a The first byte array to concatenate.
   * @param b The second byte array to concatenate.
   * @return The concatenated byte arrays.
   */
  public static byte[] concatenate(byte[] a, byte[] b)
  {
    byte[] output = new byte[a.length + b.length];

    System.arraycopy(a, 0, output, 0, a.length);
    System.arraycopy(b, 0, output, a.length, b.length);
    return output;
  }

  /**
   * Concatenates two {@link Slice}s
   * @param a The first {@link Slice} to concatenate.
   * @param b The second {@link Slice} to concatenate.
   * @return The concatenated {@link Slice}.
   */
  public static Slice concatenate(Slice a, Slice b)
  {
    int size = a.length + b.length;
    byte[] bytes = new byte[size];

    System.arraycopy(a.buffer, a.offset, bytes, 0, a.length);
    System.arraycopy(b.buffer, b.offset, bytes, a.length, b.length);

    return new Slice(bytes);
  }

  /**
   * Concatenates a byte array with the contents of a {@link Slice}.
   * @param a The byte array to concatenate. The contents of the byte array appear first in the concatenation.
   * @param b The {@link Slice} to concatenate a byte array with.
   * @return A {@link Slice} whose contents are the concatenation of the input byte array and {@link Slice}.
   */
  public static Slice concatenate(byte[] a, Slice b)
  {
    int size = a.length + b.length;
    byte[] bytes = new byte[size];

    System.arraycopy(a, 0, bytes, 0, a.length);
    System.arraycopy(b.buffer, b.offset, bytes, a.length, b.length);

    return new Slice(bytes);
  }

  /**
   * Concatenates a byte array with the contents of a {@link Slice}.
   * @param a The byte array to concatenate.
   * @param b The {@link Slice} to concatenate a byte array with. The contents of the {@link Slice} appear first in the
   * concatenation.
   * @return A {@link Slice} whose contents are the concatenation of the input byte array and {@link Slice}.
   */
  public static Slice concatenate(Slice a, byte[] b)
  {
    int size = a.length + b.length;
    byte[] bytes = new byte[size];

    System.arraycopy(a.buffer, a.offset, bytes, 0, a.length);
    System.arraycopy(b, 0, bytes, a.length, b.length);

    return new Slice(bytes);
  }

  public static BufferSlice toBufferSlice(Slice slice)
  {
    if (slice instanceof BufferSlice) {
      return (BufferSlice)slice;
    }

    //The hashCode of Slice was not correct, so correct it
    return new BufferSlice(slice);
  }
}
