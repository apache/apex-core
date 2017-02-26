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

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * AffixSerde provides serde for adding prefix or suffix
 *
 * @param <T>
 */
public class AffixSerde<T> implements Serde<T>
{
  private Serde<T> serde;
  private byte[] prefix;
  private byte[] suffix;

  private AffixSerde()
  {
    //kyro
  }

  public AffixSerde(byte[] prefix, Serde<T> serde, byte[] suffix)
  {
    this.prefix = prefix;
    this.suffix = suffix;
    this.serde = serde;
  }

  @Override
  public void serialize(T object, Output output)
  {
    if (prefix != null && prefix.length > 0) {
      output.write(prefix);
    }
    serde.serialize(object, output);
    if (suffix != null && suffix.length > 0) {
      output.write(suffix);
    }
  }

  @Override
  public T deserialize(Input input)
  {
    if (prefix != null && prefix.length > 0) {
      input.skip(prefix.length);
    }
    return serde.deserialize(input);
  }

}
