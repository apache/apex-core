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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * An implementation of {@link Serde} which serializes and deserializes {@link ImmtablePair}s.
 *
 */
@InterfaceStability.Evolving
public class ImmutablePairSerde<L, R> implements Serde<ImmutablePair<L, R>>
{
  private Serde<L> leftSerde;
  private Serde<R> rightSerde;

  public ImmutablePairSerde()
  {
    this(GenericSerde.DEFAULT, GenericSerde.DEFAULT);
  }

  public ImmutablePairSerde(Serde<L> leftSerde, Serde<R> rightSerde)
  {
    this.leftSerde = leftSerde;
    this.rightSerde = rightSerde;
  }

  @Override
  public void serialize(ImmutablePair<L, R> pair, Output output)
  {
    leftSerde.serialize(pair.left, output);
    rightSerde.serialize(pair.right, output);
  }

  @Override
  public ImmutablePair<L, R> deserialize(Input input)
  {
    throw new RuntimeException("Not Supported.");
  }
}
