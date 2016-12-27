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

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * This is an implementation of {@link Serde} which deserializes and serializes integers.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class IntSerde implements Serde<Integer>
{
  @Override
  public void serialize(Integer value, Output output)
  {
    output.writeInt(value);
  }

  @Override
  public Integer deserialize(Input input)
  {
    return input.readInt();
  }
}
