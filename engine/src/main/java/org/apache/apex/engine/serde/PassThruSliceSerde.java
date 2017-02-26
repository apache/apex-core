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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Throwables;

import com.datatorrent.netlet.util.Slice;

/**
 * This is a {@link Serde} implementation which simply allows an input slice to pass through. No serialization or
 * deserialization transformation is performed on the input {@link Slice}s.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class PassThruSliceSerde implements Serde<Slice>
{
  @Override
  public void serialize(Slice slice, Output output)
  {
    output.write(slice.buffer, slice.offset, slice.length);
  }

  @Override
  public Slice deserialize(Input input)
  {
    if (input.getInputStream() != null) {
      // The input is backed by a stream, cannot directly use its internal buffer
      try {
        return new Slice(input.readBytes(input.available()));
      } catch (IOException ex) {
        throw Throwables.propagate(ex);
      }
    } else {
      return new Slice(input.getBuffer(), input.position(), input.limit() - input.position());
    }
  }
}
